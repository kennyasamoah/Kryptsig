#!/usr/bin/env python3
"""
Kryptsig -- dormancy-break signal service for Solana tokens.

Finds tokens that have gone quiet, alerts when one wakes up.
You do not pick the tokens; it builds its own universe from liquidity
and age -- deliberately NOT from momentum.

Kryptsig holds no keys, touches no wallet, and cannot trade.
It is a detection layer. Execution happens elsewhere, by you.

Usage
  python kryptsig.py                    normal run (discover, poll, alert)
  python kryptsig.py --check <address>  audit one token against the rules
  DRY_RUN=1 python kryptsig.py          fixtures, no network

Env
  NTFY_TOPIC   push alerts to https://ntfy.sh/<topic>
"""

import csv
import json
import os
import statistics
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ======================================================================
# UNIVERSE -- who gets watched. No momentum condition. That is the point.
# ======================================================================
MAX_POSITION_USD = 250           # your actual max. Everything derives from this.
MAX_POOL_PCT     = 0.02          # never exceed 2% of pool -- above that your
                                 # own exit moves the price against you

# $250 at 2% of pool => $12,500. Below this you cannot exit at a price you
# would accept, at YOUR size. Not arbitrary -- derived.
MIN_LIQ_ABS      = 12_500
MIN_AGE_HOURS    = 6             # was 14 days. Target is now accumulation,
                                 # not dormancy break -- but we still need
                                 # enough candles for a baseline.
MAX_MCAP         = 20_000_000    # tightened: asymmetry, not safety
UNIVERSE_CAP     = 150

# ======================================================================
# SIGNAL -- loose on purpose during the logging phase.
# ======================================================================
# The inversion that matters: we now require price to be moving, but NOT
# to have already moved. Buyers arriving BEFORE price goes vertical is
# accumulation. Buyers arriving AFTER is the crowd -- and the crowd is
# somebody else's exit.
SPIKE_MULTIPLE   = 3.0           # volume vs baseline
MIN_ABS_VOLUME   = 5_000
MIN_PRICE_CHG_1H = 2.0           # the ramp has started
MAX_PRICE_CHG_1H = 60.0          # ...but has not gone vertical. THE CEILING.
MAX_PRICE_CHG_24H = 300.0        # and did not already run yesterday
MIN_BUYER_RATIO  = 1.3           # unique buyers vs sellers this hour
MIN_BUYER_MULT   = 2.5           # unique buyers vs their own baseline
MIN_TURNOVER     = 0.05

# ======================================================================
# SAFETY -- do not loosen. Protects your ability to exit.
# ======================================================================
MAX_FDV_MC_RATIO = 1.5

# Friction on a $250 ticket: ~0.5% or $0.95 min per side on fomo, plus
# slippage both ways. Logging raw price change overstates every outcome.
FEE_PCT_PER_SIDE   = 0.005
FEE_MIN_USD        = 0.95
SLIPPAGE_ASSUMED   = 0.01      # each side, tier-adjusted below

MAX_ALERTS_PER_DAY = 3         # $750 of correlated exposure is the ceiling
HEARTBEAT_HOURS    = 24        # prove the system is alive even when silent

# The ceiling is our single biggest untested assumption. Log what it rejects
# so the log can refute it.
CEILING_LOG        = "rejected_late.csv"

COOLDOWN_HOURS     = 4
BASELINE_HOURS     = 168         # up to 7 days of hourly candles
MIN_BASELINE_OBS   = 6           # young tokens have short histories
BASELINE_MAX_AGE_H = 168         # refresh weekly (BUG 2)
BACKFILL_BUDGET    = 2
MAX_BACKFILL_TRIES = 3           # then give up (BUG 3)
LOG_MIN_MULTIPLE   = 2.0         # throttle the log (BUG 1)

# With a free CoinGecko Demo key, calls are limited PER KEY. Without one,
# they are limited per IP -- and GitHub Actions runners share IPs with
# thousands of other jobs, so the keyless tier 429s before we start.
CG_KEY = os.environ.get("CG_API_KEY", "").strip()
if CG_KEY:
    GT = "https://api.coingecko.com/api/v3/onchain"
else:
    GT = "https://api.geckoterminal.com/api/v2"

NET  = "solana"
PACE = 1.5 if CG_KEY else 7.0     # keyless is ~10 calls/min
MAX_RETRIES = 4

# CoinGecko Demo: 10,000 credits/month, 100 calls/min. Every retry is a
# billable call. A hard per-run ceiling means a bug cannot burn the month's
# budget in an afternoon -- the run degrades instead of the quota dying.
MONTHLY_CREDITS   = 10_000
MAX_CALLS_PER_RUN = 14
CALLS = {"n": 0, "capped": False}

# Fields the parser depends on. Declared here so --selftest can verify the
# live API actually returns them, instead of num() silently yielding 0.0 and
# a healthy token reading as dead.
POOL_FIELDS = [
    "name", "reserve_in_usd", "base_token_price_usd", "volume_usd",
    "price_change_percentage", "transactions", "pool_created_at", "fdv_usd",
]
NESTED_FIELDS = [
    ("volume_usd", "h1"), ("volume_usd", "h24"),
    ("price_change_percentage", "h1"),
]

RC_MIN_LOCK_NOTE = ("RugCheck publishes LP Locked % on its token page. "
                    "When it disagrees with the computed figure, use theirs.")

# --------------------------------------------------------------- RugCheck
# Used ONLY by --audit, never by the hourly poll. A failure here can never
# affect the running detector.
#
# Birdeye's free tier returns 401 on token_security ("API key lacks
# sufficient permissions"), so it cannot do this job. RugCheck's report
# endpoint carries LP lock status, insider networks, and holder distribution
# -- the three things no price API exposes.
RC      = "https://api.rugcheck.xyz/v1"
RC_KEY  = os.environ.get("RUGCHECK_API_KEY", "").strip()   # optional

# Gates. LP lock is the one that decides whether a loss is recoverable.
MIN_LP_LOCKED   = 0.90
MAX_TOP10       = 0.30
MAX_CREATOR     = 0.05

STATE_FILE = "state.json"
LOG_FILE   = "observations.csv"
ALERT_FILE = "alerts.csv"


# ----------------------------------------------------------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def get(path, params=""):
    """GET with exponential backoff on 429. Returns None after MAX_RETRIES."""
    headers = {"Accept": "application/json", "User-Agent": "kryptsig/3.1"}
    if CG_KEY:
        headers["x-cg-demo-api-key"] = CG_KEY

    if CALLS["n"] >= MAX_CALLS_PER_RUN:
        if not CALLS["capped"]:
            print(f"  [budget] hit {MAX_CALLS_PER_RUN} calls this run -- "
                  f"skipping remaining requests")
            CALLS["capped"] = True
        return None

    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(f"{GT}{path}{params}", headers=headers)
        try:
            CALLS["n"] += 1
            with urllib.request.urlopen(req, timeout=25) as r:
                time.sleep(PACE)
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < MAX_RETRIES - 1:
                wait = PACE * (2 ** attempt) + 3
                print(f"  429 -- backing off {wait:.0f}s "
                      f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            print(f"  ! {path} -> HTTP {e.code}")
            return None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            print(f"  ! {path} -> {e}")
            time.sleep(PACE)
            return None
    return None


def num(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def append_row(path, header, row):
    exists = os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.writer(fh)
        if not exists:
            w.writerow(header)
        w.writerow(row)


def notify(title, body):
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        print(f"[no NTFY_TOPIC]\n{title}\n{body}\n")
        return
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}", data=body.encode(),
        headers={"Title": title, "Priority": "high", "Tags": "eyes"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  ! notify: {e}")


def rc_get(path):
    """RugCheck GET. Key is optional -- the report endpoint is public, but an
    API key raises rate limits. Returns (data, error)."""
    headers = {"accept": "application/json",
               "User-Agent": "kryptsig/3.1"}
    if RC_KEY:
        headers["X-API-KEY"] = RC_KEY
    req = urllib.request.Request(f"{RC}{path}", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read().decode()), None
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode()[:200]
        except Exception:
            pass
        if e.code == 429:
            return None, "HTTP 429 -- rate limited. Set RUGCHECK_API_KEY to raise limits."
        return None, f"HTTP {e.code} {body}"
    except Exception as e:
        return None, str(e)


def dig(d, *names, default=None):
    """Field names in this API are not fully documented. Try several
    spellings rather than guessing one and silently reading nothing."""
    if not isinstance(d, dict):
        return default
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return default


def round_trip_cost(position_usd, tier):
    """Total friction as a fraction of position. A '+5% winner' at $250 is
    roughly flat once this is paid, so outcomes must be logged net."""
    fee = max(position_usd * FEE_PCT_PER_SIDE, FEE_MIN_USD) * 2
    slip = {"A": 0.005, "B": 0.01, "C": 0.02, "D": 0.035}.get(tier, 0.02) * 2
    return (fee / position_usd) + slip


def as_pct(v):
    """Accept either 0-1 or 0-100 and normalise to a fraction."""
    if not isinstance(v, (int, float)):
        return None
    return v / 100.0 if v > 1.5 else v


def audit_summary(mint):
    """Compact audit for the alert path. Returns (verdict, detail, fails).
    verdict: 'block' | 'warn' | 'clear' | 'unknown'"""
    rep, err = rc_get(f"/tokens/{mint}/report")
    if err or not isinstance(rep, dict):
        return "unknown", f"rugcheck unavailable ({err})", []

    fails, notes = [], []
    tok = dig(rep, "token", default={}) or {}
    for label, key in (("mint authority", "mintAuthority"),
                       ("freeze authority", "freezeAuthority")):
        if key in rep or key in tok:
            if rep.get(key, tok.get(key)) not in (None, "", "null"):
                fails.append(f"{label} ACTIVE")

    if dig(rep, "rugged") is True:
        fails.append("flagged rugged")

    # LP: worst defensible view, impossible values treated as failure
    views = []
    tot = lock = 0.0
    for m_ in dig(rep, "markets", default=[]) or []:
        lp = dig(m_, "lp", default={}) or {}
        b = dig(lp, "baseUSD")
        l = dig(lp, "lpLockedUSD")
        p = as_pct(dig(lp, "lpLockedPct"))
        if isinstance(b, (int, float)) and b > 0:
            tot += b * 2
            if isinstance(l, (int, float)):
                lock += l
            if p is not None:
                views.append(p)
    if tot > 0 and lock > 0:
        views.append(lock / tot)
    usable = [v for v in views if 0 <= v <= 1.02]
    if not usable:
        fails.append("LP lock unverifiable -- treat as unlocked")
    else:
        worst = min(usable)
        if worst < MIN_LP_LOCKED:
            fails.append(f"LP only {worst*100:.0f}% locked")
        notes.append(f"LP {worst*100:.0f}%")

    top = dig(rep, "topHolders", "top_holders", default=[]) or []
    if top:
        raw = [dig(h, "pct", "percentage") for h in top[:10]]
        raw = [r for r in raw if isinstance(r, (int, float))]
        t = sum(raw)
        t10 = t / 100.0 if t > 1.5 else t
        notes.append(f"top10 {t10*100:.0f}%")
        if t10 > MAX_TOP10:
            fails.append(f"top-10 {t10*100:.0f}%")

    nets = len(dig(rep, "insiderNetworks", default=[]) or [])
    if nets:
        notes.append(f"{nets} insider network(s)")

    detail = ", ".join(notes) if notes else "no detail"
    if fails:
        return "block", detail, fails
    return ("warn", detail, []) if nets else ("clear", detail, [])


def audit(address):
    """Manual pre-trade check. Never called by the hourly poll."""
    print(f"\nKryptsig audit -- {address}\n" + "=" * 62)

    rep, err = rc_get(f"/tokens/{address}/report")
    if err:
        print(f"\nUNAVAILABLE  {err}")
        print("\nCheck rugcheck.xyz in a browser instead.")
        return
    if not isinstance(rep, dict):
        print(f"\nUNEXPECTED SHAPE: {type(rep).__name__}")
        return

    fails, warns, unknown = [], [], []

    # ---- RugCheck's own verdict ------------------------------------------
    print("\n[1] rugcheck verdict")
    score = dig(rep, "score_normalised", "score")
    if score is not None:
        print(f"  score           {score}")
    if dig(rep, "rugged") is True:
        print("  FAIL  flagged as RUGGED")
        fails.append("flagged rugged")
    risks = dig(rep, "risks", default=[]) or []
    if risks:
        for r in risks[:8]:
            lvl = dig(r, "level", default="")
            nm = dig(r, "name", default="?")
            print(f"  risk  [{lvl}] {nm}")
            if str(lvl).lower() in ("danger", "high", "critical"):
                fails.append(nm)
            else:
                warns.append(nm)
    else:
        print("  no risks listed by rugcheck")

    # ---- authorities ------------------------------------------------------
    print("\n[2] authorities")
    tok = dig(rep, "token", default={}) or {}
    # These live at the TOP level of the report, not inside `token`.
    # Reading the wrong place returned None and printed "revoked" -- a
    # false pass. Check both, and say so when neither reports.
    for label, key in (("mint authority", "mintAuthority"),
                       ("freeze authority", "freezeAuthority")):
        present = key in rep or key in tok
        v = rep.get(key, tok.get(key))
        if not present:
            print(f"  ?     {label:<20} NOT REPORTED -- cannot verify")
            unknown.append(label)
        elif v in (None, "", "null"):
            print(f"  ok    {label:<20} revoked")
        else:
            print(f"  FAIL  {label:<20} ACTIVE ({str(v)[:24]})")
            fails.append(label + " active")

    # ---- LP lock ----------------------------------------------------------
    print("\n[3] liquidity lock")
    markets = dig(rep, "markets", default=[]) or []
    # I computed this four different ways and got 100%, 0%, 84.6% and 142.1%.
    # baseUSD is ONE SIDE of a two-sided pool; lpLockedUSD is the whole LP.
    # Rather than keep guessing at a derived figure, report several views and
    # gate on the WORST. An impossible value is treated as a failure, never
    # as a pass -- a broken calculation must not read as safety.
    per_market, total_side, locked_usd = [], 0.0, 0.0
    for i, m in enumerate(markets):
        lp = dig(m, "lp", default={}) or {}
        locked = as_pct(dig(lp, "lpLockedPct", "lpLockedPercentage"))
        lusd = dig(lp, "lpLockedUSD", "lpLockedUsd")
        busd = dig(lp, "baseUSD", "lpTotalUSD")
        if not isinstance(busd, (int, float)) or busd <= 0:
            continue
        pool_est = busd * 2          # both sides of the AMM pool
        total_side += pool_est
        if isinstance(lusd, (int, float)):
            locked_usd += lusd
        if locked is not None:
            per_market.append((pool_est, locked))
        print(f"        market {i+1}: ~${pool_est:,.0f} pool "
              f"(baseUSD ${busd:,.0f} x2), ${lusd or 0:,.0f} locked"
              f"  [{(locked or 0)*100:.0f}% reported]")
        if i == 0 and lp:
            print(f"        raw lp: " + ", ".join(
                f"{k}={lp[k]}" for k in sorted(lp)
                if isinstance(lp[k], (int, float, str, bool))
            )[:300])

    views = []
    if per_market:
        # deepest pool's own reported figure
        deepest = max(per_market, key=lambda x: x[0])
        views.append(("deepest market (reported)", deepest[1]))
        # share of total pooled dollars sitting in a market that is locked
        lockedish = sum(p for p, l in per_market if l >= 0.9)
        allpools = sum(p for p, _ in per_market)
        if allpools:
            views.append(("share of pools locked", lockedish / allpools))
    if total_side > 0 and locked_usd > 0:
        views.append(("dollar-weighted", locked_usd / total_side))

    for label, v in views:
        flag = "  (impossible -- ignored)" if v > 1.02 else ""
        print(f"        {label:<26} {v*100:.1f}%{flag}")

    usable = [v for _, v in views if 0 <= v <= 1.02]
    if not usable:
        print("  FAIL  lp lock              could not be computed reliably")
        fails.append("LP lock could not be verified -- treat as unlocked")
    else:
        worst = min(usable)
        bad = worst < MIN_LP_LOCKED
        print(f"  {'FAIL' if bad else 'ok  '}  {'lp locked (worst view)':<22} "
              f"{worst*100:.1f}%   (need {MIN_LP_LOCKED*100:.0f}%)")
        if bad:
            fails.append(f"LP only {worst*100:.0f}% locked on the worst view")
        print("        RugCheck publishes its own LP Locked % on the token")
        print("        page. If it is lower than this, use theirs.")

    # ---- distribution -----------------------------------------------------
    print("\n[4] distribution")
    total_holders = dig(rep, "totalHolders", "total_holders")
    if total_holders:
        print(f"  holders         {total_holders:,}")

    creator_pct = as_pct(dig(rep, "creatorBalancePct", "creator_balance_pct"))
    if creator_pct is None:
        cb, sup = dig(rep, "creatorBalance"), dig(tok, "supply")
        if isinstance(cb, (int, float)) and isinstance(sup, (int, float)) and sup:
            creator_pct = cb / sup
    if creator_pct is None:
        print("  ?     creator balance    not reported")
        unknown.append("creator balance")
    else:
        bad = creator_pct > MAX_CREATOR
        print(f"  {'FAIL' if bad else 'ok  '}  {'creator balance':<20} "
              f"{creator_pct*100:.1f}%   (max {MAX_CREATOR*100:.0f}%)")
        if bad:
            fails.append("creator still holding")

    top = dig(rep, "topHolders", "top_holders", default=[]) or []
    if top:
        # Per-item normalisation was the bug: a list like [12.3, 0.9, 0.8]
        # had 12.3 divided by 100 while 0.9 was left alone, producing 300%.
        # Decide the scale ONCE from the whole list.
        raw = [dig(h, "pct", "percentage") for h in top[:10]]
        raw = [r for r in raw if isinstance(r, (int, float))]
        total = sum(raw)
        top10 = total / 100.0 if total > 1.5 else total
        bad = top10 > MAX_TOP10
        print(f"  {'FAIL' if bad else 'ok  '}  {'top 10 holders':<20} "
              f"{top10*100:.1f}%   (max {MAX_TOP10*100:.0f}%)")
        if bad:
            fails.append("top-10 concentration")
        nets = dig(rep, "insiderNetworks", default=[]) or []
        if nets:
            print(f"  WARN  {'insider networks':<20} {len(nets)} detected")
            warns.append(f"{len(nets)} insider network(s) -- linked wallets")
        if dig(rep, "graphInsidersDetected"):
            print(f"        graphInsidersDetected: "
                  f"{dig(rep, 'graphInsidersDetected')}")
        insiders = [h for h in top if dig(h, "insider") is True]
        if insiders:
            print(f"  WARN  {'insider wallets':<20} {len(insiders)} of "
                  f"{len(top)} flagged as linked")
            warns.append(f"{len(insiders)} insider wallets in top holders")
    else:
        unknown.append("top holders")

    # ---- verdict ----------------------------------------------------------
    print("\n" + "=" * 62)
    if fails:
        print("DO NOT BUY -- failed:")
        for f_ in dict.fromkeys(fails):
            print(f"  - {f_}")
    elif unknown:
        print("INCONCLUSIVE -- could not verify:")
        for u in dict.fromkeys(unknown):
            print(f"  - {u}")
    else:
        print("No automated red flags.")
        print("This means nothing obvious was rigged. It is NOT a")
        print("recommendation, and it does not make the trade good.")
    if warns:
        print("\nWarnings (not disqualifying, but weigh them):")
        for w in dict.fromkeys(warns):
            print(f"  - {w}")

    extra = [k for k in rep if k not in
             ("score", "score_normalised", "rugged", "risks", "token",
              "markets", "totalHolders", "total_holders", "creatorBalance",
              "creatorBalancePct", "topHolders", "top_holders",
              "mintAuthority", "freezeAuthority", "insiderNetworks",
              "graphInsidersDetected", "lockers", "lockerOwners")]
    if extra:
        print(f"\nunmapped response fields: {', '.join(sorted(extra)[:16])}")
    print()


# ----------------------------------------------------------------------
def parse_pool(attrs):
    tx1 = (attrs.get("transactions") or {}).get("h1") or {}
    age_days = 0.0
    created = attrs.get("pool_created_at")
    if created:
        try:
            dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - dt).total_seconds() / 86400
        except ValueError:
            pass
    return {
        "name":      attrs.get("name", "?"),
        "liquidity": num(attrs.get("reserve_in_usd")),
        "mcap":      num(attrs.get("market_cap_usd")) or num(attrs.get("fdv_usd")),
        "fdv":       num(attrs.get("fdv_usd")),
        "price":     num(attrs.get("base_token_price_usd")),
        "vol_1h":    num((attrs.get("volume_usd") or {}).get("h1")),
        "vol_24h":   num((attrs.get("volume_usd") or {}).get("h24")),
        "chg_1h":    num((attrs.get("price_change_percentage") or {}).get("h1")),
        "chg_24h":   num((attrs.get("price_change_percentage") or {}).get("h24")),
        "buyers":    tx1.get("buyers", 0) or 0,
        "sellers":   tx1.get("sellers", 0) or 0,
        "mint":      ((attrs.get("base_token") or {}).get("address")
                      or (((attrs.get("relationships") or {}).get("base_token")
                           or {}).get("data") or {}).get("id", "").split("_")[-1]
                      or ""),
        "age_days":  round(age_days, 1),
        "turnover":  0.0,   # filled in below once mcap is known
    }


def risk_tier(liquidity):
    """Liquidity no longer blocks. It sizes. You get the alert either way --
    the tier tells you what you are taking on."""
    pos = min(MAX_POSITION_USD, liquidity * MAX_POOL_PCT)
    pct = pos / liquidity if liquidity else 1.0
    if liquidity >= 100_000:
        tier, note = "A", f"deep -- ${pos:,.0f} is {pct:.2%} of pool"
    elif liquidity >= 50_000:
        tier, note = "B", f"adequate -- ${pos:,.0f} is {pct:.2%} of pool"
    elif liquidity >= 25_000:
        tier, note = "C", f"thin -- ${pos:,.0f} is {pct:.1%} of pool"
    else:
        tier, note = "D", (f"VERY THIN -- ${pos:,.0f} is {pct:.1%} of pool, "
                           f"expect real slippage")
    return tier, pos, note


def enrich(m):
    m["turnover"] = (m["vol_24h"] / m["mcap"]) if m["mcap"] else 0.0
    m["tier"], m["max_pos"], m["tier_note"] = risk_tier(m["liquidity"])
    return m


def liquidity_ok(m):
    """Only the derived floor blocks now. Everything above it gets a tier
    and a position cap instead of a rejection."""
    if m["liquidity"] < MIN_LIQ_ABS:
        return False, (f"liquidity ${m['liquidity']:,.0f} -- a ${MAX_POSITION_USD} "
                       f"position would be >{MAX_POOL_PCT:.0%} of the pool")
    return True, ""


# Admission and alerting are deliberately different.
#
# A token 20 minutes old with $8k pooled cannot be traded today -- but in six
# hours it may have $40k and a baseline. If we reject it at admission we never
# see it again, because new_pools only shows it once. So the pond admits
# early and cheaply; the ALERT gates are what protect you.
WATCH_MIN_LIQ = 2_000    # new_pools median liquidity is ~$2k. This is the
                         # launch firehose; we admit cheaply and evict fast.
# Eviction is trajectory-based. new_pools tokens start near $2k; reaching the
# $12.5k alert floor is roughly the graduation event, which most never manage.
# But some take a day. So: evict fast if there is NO growth, slower if there is.
STALL_HOURS   = 8        # no growth at all by now -> dead
STALL_LIQ     = 5_000
MATURITY_HOURS = 24      # grew but never cleared the alert floor -> dead

def admissible(m):
    """Loose. Just: is this plausibly worth watching as it matures?"""
    if m["liquidity"] < WATCH_MIN_LIQ:
        return False, f"liquidity ${m['liquidity']:,.0f} < ${WATCH_MIN_LIQ:,}"
    if m["mcap"] and m["mcap"] > MAX_MCAP:
        return False, f"mcap ${m['mcap']:,.0f} too large"
    return True, "admitted"


def qualifies(m):
    """Strict. Applied at ALERT time, not admission."""
    ok, why = liquidity_ok(m)
    if not ok:
        return False, why
    if m["age_days"] * 24 < MIN_AGE_HOURS:
        return False, f"only {m['age_days']*24:.1f}h old (need {MIN_AGE_HOURS}h)"
    if m["mcap"] > MAX_MCAP:
        return False, f"mcap ${m['mcap']:,.0f} too large"
    return True, "qualifies"


def discover(state):
    """Two sources per run:
      1. /new_pools  -- freshly created pools. This is where accumulation
         candidates live. Sorting by volume only surfaces SOL/USDC majors.
      2. /pools page N (rotating) -- established pools that may re-accelerate.
    Costs 2 calls."""
    added = 0

    def admit(payload):
        nonlocal added
        for pool in (payload or {}).get("data") or []:
            if len(state["pools"]) >= UNIVERSE_CAP:
                return
            attrs = pool.get("attributes") or {}
            addr = attrs.get("address")
            if not addr or addr in state["pools"]:
                continue
            m = parse_pool(attrs)
            ok, _ = admissible(m)
            if ok:
                state["pools"][addr] = {"name": m["name"], "baseline": None,
                                        "baseline_ts": None, "tries": 0,
                                        "added": now_iso()}
                added += 1

    if len(state["pools"]) < UNIVERSE_CAP:
        admit(get(f"/networks/{NET}/new_pools"))

    page = state.get("next_page", 1)
    state["next_page"] = page + 1 if page < 8 else 1
    if len(state["pools"]) < UNIVERSE_CAP:
        admit(get(f"/networks/{NET}/pools",
                  f"?page={page}&sort=h24_volume_usd_desc"))

    return added


def prune(state, seen):
    """Two evictions:
      1. liquidity collapsed below the watch floor
      2. old enough to have grown, and did not

    Without (2) the pond fills with dead newborns within two days and stops
    admitting anything. Most launches die; the pond has to reflect that."""
    dropped = 0
    for addr, m in seen.items():
        if addr not in state["pools"]:
            continue
        age_h = m["age_days"] * 24
        dead = m["liquidity"] < WATCH_MIN_LIQ
        no_growth = age_h > STALL_HOURS and m["liquidity"] < STALL_LIQ
        stalled = age_h > MATURITY_HOURS and m["liquidity"] < MIN_LIQ_ABS
        # anything at or above the alert floor stays, regardless of age
        stalled = stalled or no_growth
        if dead or stalled:
            del state["pools"][addr]
            state["last_alert"].pop(addr, None)
            dropped += 1
    return dropped


def backfill(addr):
    """Volume baseline from OHLCV candles."""
    data = get(f"/networks/{NET}/pools/{addr}/ohlcv/hour",
               f"?limit={BASELINE_HOURS}&currency=usd")
    try:
        rows = data["data"]["attributes"]["ohlcv_list"]
    except (TypeError, KeyError):
        return None, 0
    vols = [num(r[5]) for r in rows if len(r) >= 6]
    if len(vols) < MIN_BASELINE_OBS:
        return None, len(vols)
    return statistics.median(vols), len(vols)


def update_buyer_baseline(entry, buyers_now):
    """OHLCV gives no buyer counts, so this baseline is accumulated from our
    own polls. Rolling median of the last 336 observations (~7d at 30min).
    Returns the median EXCLUDING the current reading, so a token can never
    trigger against its own spike."""
    hist = entry.setdefault("buyer_hist", [])
    prior = list(hist)
    hist.append(int(buyers_now))
    entry["buyer_hist"] = hist[-336:]
    if len(prior) < 24:
        return None
    return statistics.median(prior)


def too_young_for_baseline(entry):
    """A pool with 2 hours of history cannot produce a baseline yet. Do not
    count that against its retry budget -- it will be old enough later."""
    return entry.get("last_candles", 0) < MIN_BASELINE_OBS and \
           entry.get("tries", 0) > 0 and entry.get("young", False)


def needs_baseline(entry):
    """None, or older than a week (BUG 2), and not exhausted (BUG 3)."""
    if entry.get("tries", 0) >= MAX_BACKFILL_TRIES and not entry.get("baseline"):
        return False
    if entry.get("baseline") is None:
        return True
    ts = entry.get("baseline_ts")
    if not ts:
        return True
    try:
        age_h = (datetime.now(timezone.utc)
                 - datetime.fromisoformat(ts)).total_seconds() / 3600
    except ValueError:
        return True
    return age_h > BASELINE_MAX_AGE_H


def evaluate(m, baseline, last_alert, buyer_base=None):
    """Fire on ACCUMULATION: buyers arriving BEFORE price goes vertical."""
    ok, why = liquidity_ok(m)
    if not ok:
        return False, why
    if m["age_days"] * 24 < MIN_AGE_HOURS:
        return False, f"{m['age_days']*24:.1f}h old -- too young to alert"
    if not baseline or baseline <= 0:
        return False, "no baseline yet"

    mult = m["vol_1h"] / baseline
    m["multiple"] = round(mult, 1)
    ratio = m["buyers"] / max(m["sellers"], 1)
    m["buyer_ratio"] = round(ratio, 2)

    if mult < SPIKE_MULTIPLE:
        return False, f"{mult:.1f}x baseline (need {SPIKE_MULTIPLE}x)"
    if m["vol_1h"] < MIN_ABS_VOLUME:
        return False, f"{mult:.1f}x but only ${m['vol_1h']:,.0f}"

    # ---- THE CEILING: what separates early from somebody else's exit ----
    if m["chg_1h"] < MIN_PRICE_CHG_1H:
        return False, f"{mult:.1f}x volume, price flat ({m['chg_1h']:+.1f}%)"
    if m["chg_1h"] > MAX_PRICE_CHG_1H:
        m["ceiling_reject"] = True
        return False, (f"TOO LATE -- already {m['chg_1h']:+.0f}% this hour "
                       f"(ceiling +{MAX_PRICE_CHG_1H:.0f}%)")
    if m["chg_24h"] > MAX_PRICE_CHG_24H:
        m["ceiling_reject"] = True
        return False, f"TOO LATE -- already {m['chg_24h']:+.0f}% in 24h"

    if ratio < MIN_BUYER_RATIO:
        return False, f"buy/sell {ratio:.2f} -- distribution, not accumulation"

    if buyer_base:
        bmult = m["buyers"] / max(buyer_base, 1)
        m["buyer_mult"] = round(bmult, 1)
        if bmult < MIN_BUYER_MULT:
            return False, (f"{mult:.1f}x volume but buyers only {bmult:.1f}x "
                           f"-- same wallets churning")
    if m["turnover"] < MIN_TURNOVER:
        return False, f"turnover {m['turnover']:.1%} -- asleep for its size"

    if last_alert and (time.time() - last_alert) < COOLDOWN_HOURS * 3600:
        return False, "cooldown"

    bm = f", buyers {m.get('buyer_mult','?')}x" if buyer_base else ""
    return True, (f"ACCUMULATING: {mult:.1f}x vol{bm}, {m['chg_1h']:+.1f}% 1h "
                  f"(under +{MAX_PRICE_CHG_1H:.0f}% ceiling), {ratio:.1f} buy/sell")


def alert_body(addr, m, baseline, reason):
    return (
        f"{m['name']}\n{reason}\n\n"
        f"TIER {m['tier']} -- {m['tier_note']}\n"
        f"MAX POSITION ${m['max_pos']:,.0f}\n"
        f"AUDIT {m.get('audit','not run')}\n"
        f"Round-trip cost ~{round_trip_cost(m['max_pos'], m['tier'])*100:.1f}% "
        f"-- breakeven needs +{round_trip_cost(m['max_pos'], m['tier'])*100:.1f}%\n\n"
        f"Market cap ${m['mcap']:,.0f}\n"
        f"Liquidity  ${m['liquidity']:,.0f}\n"
        f"1h volume  ${m['vol_1h']:,.0f}  vs baseline ${baseline:,.0f}\n"
        f"1h change  {m['chg_1h']:+.1f}%   24h {m['chg_24h']:+.1f}%\n"
        f"Buy/sell   {m['buyers']}/{m['sellers']}"
        + (f"  (buyers {m['buyer_mult']}x)" if m.get("buyer_mult") else "")
        + f"\nAge        {m['age_days']*24:.0f}h\n\n"
        f"geckoterminal.com/solana/pools/{addr}\n\n"
        + ("*** TIER D -- CHECK LP LOCK ON RUGCHECK BEFORE BUYING ***\n"
           if m["tier"] == "D" else "")
        + "NOT A BUY SIGNAL. Verify holders, dev wallet, LP lock."
    )


# ----------------------------------------------------------------------
def check_one(address):
    """--check: audit a single token address against every rule."""
    print(f"\nKryptsig audit: {address}\n" + "=" * 58)
    data = get(f"/networks/{NET}/tokens/{address}/pools")
    pools = (data or {}).get("data") or []
    if not pools:
        print("No pool found. Either the address is wrong, or the token is\n"
              "too small/new to be indexed -- which is itself an answer.")
        return

    attrs = max(pools, key=lambda p: num(
        (p.get("attributes") or {}).get("reserve_in_usd")))["attributes"]
    pool_addr = attrs.get("address")
    m = enrich(parse_pool(attrs))

    print(f"{m['name']}   ({pool_addr})\n")
    print(f"  price        ${m['price']:.10f}".rstrip("0"))
    print(f"  market cap   ${m['mcap']:,.0f}")
    print(f"  liquidity    ${m['liquidity']:,.0f}"
          + (f"   ({m['liquidity']/m['mcap']:.1%} of mcap)" if m["mcap"] else ""))
    print(f"  age          {m['age_days']} days")
    print(f"  1h volume    ${m['vol_1h']:,.0f}")
    print(f"  1h change    {m['chg_1h']:+.1f}%")
    print(f"  buy/sell     {m['buyers']}/{m['sellers']}")
    print(f"  turnover     {m['turnover']:.1%} of mcap (need {MIN_TURNOVER:.0%})")
    print(f"  size tier    {m['tier']}")

    ok, why = qualifies(m)
    print(f"\n  ADMISSION    {'PASS' if ok else 'REJECT'} -- {why}")
    if not ok:
        print("\n  Kryptsig would never watch this token.")
        return

    base, n = backfill(pool_addr)
    print(f"  BASELINE     {n} candles, median ${base or 0:,.0f}/hr")
    if not base:
        print("\n  Not enough history for a baseline yet.")
        return

    fire, reason = evaluate(m, base, None)
    print(f"  TRIGGER      {'FIRE' if fire else 'no'} -- {reason}")
    print(f"\n  TIER {m['tier']} -- {m['tier_note']}")
    print(f"  Max position ${m['max_pos']:,.0f}")
    print("\n  Kryptsig cannot see holder concentration, dev wallet, or LP lock.")


def selftest():
    """Validate every external dependency. Exits non-zero on any failure."""
    fails, warns = [], []

    def ok(label, detail=""):
        print(f"  PASS  {label}" + (f"   {detail}" if detail else ""))

    def bad(label, detail=""):
        print(f"  FAIL  {label}" + (f"   {detail}" if detail else ""))
        fails.append(label)

    def warn(label, detail=""):
        print(f"  WARN  {label}" + (f"   {detail}" if detail else ""))
        warns.append(label)

    print("\nKryptsig selftest\n" + "=" * 58)

    # 1 -- credentials
    print("\n[1] credentials")
    if CG_KEY:
        ok("CG_API_KEY set", f"keyed mode, {GT}")
    else:
        warn("CG_API_KEY missing",
             "keyless -- CoinGecko docs say unsuitable for scheduled polling")
    if os.environ.get("NTFY_TOPIC"):
        ok("NTFY_TOPIC set")
    else:
        bad("NTFY_TOPIC missing", "alerts would be printed, not delivered")

    # 2 -- can we reach the API at all
    print("\n[2] discovery endpoints")
    fresh = get(f"/networks/{NET}/new_pools")
    nf = len((fresh or {}).get("data") or [])
    if nf:
        ok("new_pools", f"{nf} fresh pools")
        adm, reasons = 0, {}
        ages, liqs = [], []
        for p in fresh["data"]:
            m = parse_pool(p.get("attributes") or {})
            ages.append(m["age_days"] * 24)
            liqs.append(m["liquidity"])
            good, why = admissible(m)
            if good:
                adm += 1
            else:
                key = why.split("$")[0].strip() or why
                reasons[key] = reasons.get(key, 0) + 1
        if adm:
            ok("admission (watch list)", f"{adm}/{nf} admitted")
        else:
            bad("admission (watch list)", f"0/{nf} admitted -- nothing to watch")
        for r, c in sorted(reasons.items(), key=lambda x: -x[1])[:3]:
            print(f"        rejected {c}x: {r}")
        if ages:
            ages.sort(); liqs.sort()
            print(f"        median age {ages[len(ages)//2]:.1f}h, "
                  f"median liquidity ${liqs[len(liqs)//2]:,.0f}, "
                  f"max liquidity ${liqs[-1]:,.0f}")
            print(f"        watch floor ${WATCH_MIN_LIQ:,} | "
                  f"alert floor ${MIN_LIQ_ABS:,} | "
                  f"alert age {MIN_AGE_HOURS}h")
    else:
        bad("new_pools", "returned nothing")

    # The pond has a second source: deeper pages of top-pools-by-volume, where
    # already-graduated tokens live. Newborn stats alone understate the intake.
    est = get(f"/networks/{NET}/pools", "?page=4&sort=h24_volume_usd_desc")
    ep = (est or {}).get("data") or []
    if ep:
        adm2 = sum(1 for p in ep if admissible(parse_pool(p.get("attributes") or {}))[0])
        alertable = sum(1 for p in ep
                        if qualifies(parse_pool(p.get("attributes") or {}))[0])
        ok("established pools (page 4)",
           f"{adm2}/{len(ep)} admitted, {alertable} already alertable")

    data = get(f"/networks/{NET}/pools", "?page=1&sort=h24_volume_usd_desc")
    if not data:
        bad("GET /networks/solana/pools", "no response -- see error above")
        print(f"\n{len(fails)} failure(s). Cannot continue.\n")
        sys.exit(1)
    pools = data.get("data") or []
    if not pools:
        bad("discovery returned 0 pools", "response shape may have changed")
        sys.exit(1)
    ok("discovery endpoint", f"{len(pools)} pools returned")

    # 3 -- do the fields the parser needs actually exist
    print("\n[3] pool field contract")
    attrs = pools[0].get("attributes") or {}
    for f in POOL_FIELDS:
        if f in attrs:
            ok(f"attributes.{f}")
        else:
            bad(f"attributes.{f}", "MISSING -- parser will read 0.0")
    for parent, child in NESTED_FIELDS:
        if isinstance(attrs.get(parent), dict) and child in attrs[parent]:
            ok(f"attributes.{parent}.{child}")
        else:
            bad(f"attributes.{parent}.{child}", "MISSING")

    tx1 = (attrs.get("transactions") or {}).get("h1") or {}
    for f in ("buyers", "sellers"):
        if f in tx1:
            ok(f"transactions.h1.{f}")
        else:
            bad(f"transactions.h1.{f}",
                "MISSING -- buyer-count gate cannot work")

    # 4 -- market_cap_usd nullability (GeckoTerminal FAQ: null when
    #      supply unverified). Several gates divide by it.
    print("\n[4] market_cap_usd coverage")
    have = sum(1 for p in pools
               if (p.get("attributes") or {}).get("market_cap_usd"))
    pct = have / max(len(pools), 1)
    if pct >= 0.5:
        ok("market_cap_usd populated", f"{have}/{len(pools)} pools")
    else:
        warn("market_cap_usd often null", f"only {have}/{len(pools)} "
             "-- falling back to fdv_usd for tier/turnover/ratio")

    # 5 -- OHLCV, the baseline source
    print("\n[5] ohlcv endpoint")
    paddr = attrs.get("address")
    base, n = backfill(paddr) if paddr else (None, 0)
    if n == 0:
        bad("ohlcv_list", "no candles parsed -- baselines cannot be built")
    elif n < MIN_BASELINE_OBS:
        warn("ohlcv thin", f"{n} candles (need {MIN_BASELINE_OBS})")
    else:
        ok("ohlcv endpoint", f"{n} candles, median ${base:,.0f}/hr")

    # 6 -- batch endpoint used for every poll
    print("\n[6] multi-pool endpoint")
    addrs = [(p.get("attributes") or {}).get("address")
             for p in pools[:3]]
    addrs = [a for a in addrs if a]
    multi = get(f"/networks/{NET}/pools/multi/{','.join(addrs)}") if addrs else None
    got = len((multi or {}).get("data") or [])
    if got:
        ok("multi endpoint", f"{got}/{len(addrs)} pools returned")
    else:
        bad("multi endpoint", "polling would return nothing every run")

    # 7 -- notification delivery
    print("\n[7] notification")
    if os.environ.get("NTFY_TOPIC"):
        notify("Kryptsig selftest", "Selftest reached your device. Pipe works.")
        ok("ntfy publish sent", "check your phone")
    else:
        bad("ntfy skipped", "no topic")

    print("\n" + "=" * 58)
    print(f"{CALLS['n']} api calls | {len(fails)} failure(s) | {len(warns)} warning(s)")
    if fails:
        print("\nSelftest FAILED:")
        for f in fails:
            print(f"  - {f}")
        sys.exit(1)
    print("\nSelftest passed.\n")


# ----------------------------------------------------------------------
def main():
    if "--audit" in sys.argv:
        i = sys.argv.index("--audit")
        if i + 1 < len(sys.argv):
            audit(sys.argv[i + 1])
        else:
            print("usage: kryptsig.py --audit <token_address>")
        return

    if "--selftest" in sys.argv:
        selftest()
        return

    if "--check" in sys.argv:
        i = sys.argv.index("--check")
        if i + 1 < len(sys.argv):
            check_one(sys.argv[i + 1])
        else:
            print("usage: kryptsig.py --check <token_address>")
        return

    dry = os.environ.get("DRY_RUN") == "1"
    state = load_json(STATE_FILE, {})
    state.setdefault("pools", {})
    state.setdefault("last_alert", {})
    stamp = now_iso()

    fixture = load_json("fixture.json", {}).get("pools", []) if dry else []
    if not dry:
        added = discover(state)
        print(f"discovery: page {state.get('next_page', 1) - 1 or 8}, "
              f"+{added} (tracking {len(state['pools'])})\n")

        pending = [a for a, v in state["pools"].items() if needs_baseline(v)]
        for addr in pending[:BACKFILL_BUDGET]:
            base, n = backfill(addr)
            e = state["pools"][addr]
            e["last_candles"] = n
            # only count it as a failed attempt if there was enough history
            # and we still could not build a baseline
            if base or n >= MIN_BASELINE_OBS:
                e["tries"] = e.get("tries", 0) + 1
            if base:
                e["baseline"], e["baseline_ts"] = base, now_iso()
            print(f"  baseline {e['name'][:22]:<22} {n} candles  ${base or 0:,.0f}/hr")
        if pending:
            print()

    ready = [a for a, v in state["pools"].items() if v.get("baseline")]

    # Poll EVERY pooled token, not just baselined ones. Newborns need their
    # liquidity refreshed so prune() can evict the ones that never grow, and
    # their buyer history has to start accumulating before they mature.
    watched = list(state["pools"].keys())
    batches = ([fixture] if dry
               else [watched[i:i+30] for i in range(0, len(watched), 30)])

    alerts, logged, evaluated, liq_seen = 0, 0, 0, {}
    day = stamp[:10]
    if state.get("alert_day") != day:
        state["alert_day"], state["alerts_today"] = day, 0
    today_alerts = state.get("alerts_today", 0)

    for batch in batches:
        if dry:
            records = [(p["address"], p["attributes"]) for p in batch]
        else:
            data = get(f"/networks/{NET}/pools/multi/{','.join(batch)}")
            records = [((p.get("attributes") or {}).get("address"),
                        p.get("attributes") or {})
                       for p in ((data or {}).get("data") or [])]

        for addr, attrs in records:
            if not addr:
                continue
            m = enrich(parse_pool(attrs))
            liq_seen[addr] = m
            entry = state["pools"].setdefault(addr, {"name": m["name"]})
            baseline = entry.get("baseline")
            buyer_base = update_buyer_baseline(entry, m["buyers"])

            evaluated += 1
            fire, reason = evaluate(m, baseline, state["last_alert"].get(addr),
                                    buyer_base)
            print(f"{m['name'][:26]:<26} {reason}")

            if m.get("multiple", 0) >= LOG_MIN_MULTIPLE or fire:   # BUG 1
                logged += 1
                append_row(LOG_FILE,
                    ["ts","pool","name","tier","price","vol_1h","baseline",
                     "multiple","buyer_base","buyer_mult","turnover","chg_1h",
                     "liquidity","mcap","buyers","sellers","age_days","fired"],
                    [stamp, addr, m["name"], m["tier"], m["price"], m["vol_1h"],
                     baseline, m.get("multiple",""), buyer_base,
                     m.get("buyer_mult",""), round(m["turnover"],4), m["chg_1h"],
                     m["liquidity"], m["mcap"], m["buyers"], m["sellers"],
                     m["age_days"], int(fire)])

            # log what the ceiling rejects, so the log can refute the ceiling
            if m.get("ceiling_reject"):
                append_row(CEILING_LOG,
                    ["ts","pool","mint","name","chg_1h","chg_24h","mcap",
                     "liquidity","multiple","outcome_24h","outcome_72h"],
                    [stamp, addr, m.get("mint",""), m["name"], m["chg_1h"],
                     m["chg_24h"], m["mcap"], m["liquidity"],
                     m.get("multiple",""), "", ""])

            if fire and today_alerts >= MAX_ALERTS_PER_DAY:
                print(f"       ^ suppressed: daily cap "
                      f"({MAX_ALERTS_PER_DAY}) reached")
                fire = False

            if fire:
                alerts += 1
                today_alerts += 1
                state["last_alert"][addr] = time.time()
                verdict, detail, why = ("unknown", "no mint address", [])
                if m.get("mint"):
                    verdict, detail, why = audit_summary(m["mint"])
                m["audit"] = f"{verdict.upper()} -- {detail}"

                if verdict == "block":
                    print(f"       ^ SUPPRESSED by audit: {'; '.join(why)}")
                    alerts -= 1
                    today_alerts -= 1
                else:
                    notify(f"WAKE-UP: {m['name'][:30]}",
                           alert_body(addr, m, baseline, reason))
                append_row(ALERT_FILE,
                    ["ts","pool","mint","name","tier","multiple","buyer_mult",
                     "turnover","chg_1h","mcap","liquidity","buyer_ratio",
                     "max_position","friction_pct","audit","would_i_buy",
                     "outcome_24h","outcome_72h","net_24h","net_72h"],
                    [stamp, addr, m.get("mint",""), m["name"], m["tier"],
                     m.get("multiple"), m.get("buyer_mult",""),
                     round(m["turnover"],4), m["chg_1h"], m["mcap"],
                     m["liquidity"], m.get("buyer_ratio"), round(m["max_pos"]),
                     round(round_trip_cost(m["max_pos"], m["tier"])*100, 2),
                     m.get("audit",""), "", "", "", "", ""])

    state["alerts_today"] = today_alerts
    dropped = prune(state, liq_seen) if not dry else 0

    mode = "keyed" if CG_KEY else "KEYLESS (expect 429s)"
    mature = sum(1 for m in liq_seen.values()
                 if m["liquidity"] >= MIN_LIQ_ABS
                 and m["age_days"] * 24 >= MIN_AGE_HOURS)
    projected = CALLS["n"] * 24 * 30
    pct = projected / MONTHLY_CREDITS
    print(f"\nbudget: {CALLS['n']} calls this run -> ~{projected:,}/month "
          f"({pct:.0%} of {MONTHLY_CREDITS:,})"
          + ("  ** OVER BUDGET **" if pct > 0.85 else ""))

    # Silence is the expected output. Without a heartbeat, a dead workflow
    # and a quiet market look identical.
    if not dry:
        last_hb = state.get("last_heartbeat", 0)
        if alerts == 0 and (time.time() - last_hb) > HEARTBEAT_HOURS * 3600:
            state["last_heartbeat"] = time.time()
            notify("Kryptsig heartbeat",
                   f"Alive. Pond {len(state['pools'])} "
                   f"({mature} alertable, {len(ready)} baselined).\n"
                   f"No alerts in {HEARTBEAT_HOURS}h. "
                   f"Budget {CALLS['n']*24*30:,}/month.")
        elif alerts:
            state["last_heartbeat"] = time.time()

    with open(STATE_FILE, "w") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)

    print(f"{stamp} | {mode} | {CALLS['n']} api calls | "
          f"pond {len(state['pools'])} ({mature} alertable, {len(ready)} baselined) "
          f"| {logged} logged | {dropped} pruned | {alerts} alert(s)")

    # A green check on a run that did nothing is worse than a crash: it
    # produces weeks of false confidence instead of one visible failure.
    problems = []
    if dry:
        return
    if CALLS["n"] == 0:
        problems.append("no API calls were made")
    if not state["pools"] and state.get("empty_runs", 0) >= 3:
        problems.append("universe still empty after 3 runs -- "
                        "admission filters may be rejecting everything")
    state["empty_runs"] = 0 if state["pools"] else state.get("empty_runs", 0) + 1
    if state["pools"] and not ready:
        stalled = all(v.get("tries", 0) >= MAX_BACKFILL_TRIES
                      for v in state["pools"].values())
        if stalled:
            problems.append("every backfill attempt exhausted -- "
                            "ohlcv parsing is broken")
    if evaluated == 0 and ready:
        problems.append(f"{len(ready)} pools have baselines but none were "
                        "evaluated -- multi endpoint returned nothing")

    if problems:
        print("\nRUN UNHEALTHY -- failing so this is visible in Actions:")
        for p in problems:
            print(f"  - {p}")
        print("\nRun `python kryptsig.py --selftest` to isolate the cause.")
        sys.exit(1)


if __name__ == "__main__":
    main()
