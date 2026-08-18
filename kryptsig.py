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

# Derived from FEES, not slippage -- sizing already handles slippage, since
# max_pos = min($250, liquidity x 2%). What a floor actually protects against
# is fixed fees eating a small position:
#
#   $0.95 min per side on fomo = $1.90 round trip
#   $95 position  -> 2.0% fees        <- the practical minimum
#   $50 position  -> 3.8% fees
#
# $95 at 2% of pool => a $4,750 pool. Rounded to $5,000. The old $12,500
# floor was ~2.5x stricter than its own justification and excluded ~90% of
# the pond -- including the small pools where nobody else is looking yet.
MIN_LIQ_ABS      = 5_000
MIN_POSITION_USD = 95      # below this, fees exceed 2% -- do not bother
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
# Two signal types with different gates, tagged separately in the log so
# you can find out which one actually works.
#
#   EARLY   -- young token accelerating for the first time. Baseline is only
#              a few hours of its own infancy, so the bar is lower and the
#              ceiling tighter: we are trying to be there before the feed.
#   DORMANT -- established token with a real 7-day baseline waking up. The
#              comparison means much more, so the bar is higher and the
#              ceiling wider: a genuine wake-up can move hard and still be
#              early.
DORMANT_MIN_AGE_DAYS  = 14
DORMANT_MIN_CANDLES   = 48      # a full day+ of history, not six hours

DORMANT_SPIKE         = 8.0
DORMANT_MIN_VOLUME    = 15_000
DORMANT_MIN_CHG_1H    = 5.0
DORMANT_MAX_CHG_1H    = 120.0
DORMANT_MAX_CHG_24H   = 400.0
DORMANT_MIN_BUYER_MULT = 3.0
DORMANT_MIN_TURNOVER  = 0.08

SPIKE_MULTIPLE   = 3.0           # volume vs baseline
MIN_ABS_VOLUME   = 5_000
MIN_PRICE_CHG_1H = 2.0           # the ramp has started
MAX_PRICE_CHG_1H = 60.0          # ...but has not gone vertical. THE CEILING.
MAX_PRICE_CHG_24H = 300.0        # and did not already run yesterday
MIN_BUYER_RATIO  = 1.3           # unique buyers vs sellers this hour
MIN_BUYER_MULT   = 3.0           # raised with the liquidity floor drop:
                                 # thinner pools are easier to fake, so the
                                 # hardest-to-fake metric carries more weight
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
JOURNAL_FILE       = "journal.csv"   # YOURS. Kryptsig never overwrites it.

# Two-speed polling. Watching 150 sleepy pools every 15 minutes wastes the
# budget; watching the few already stirring is cheap and cuts detection
# latency from 60 minutes to 15 -- the difference between catching a fast
# mover and having the ceiling reject it as TOO LATE.
FULL_SCAN_HOURS  = 1.0     # discovery + backfill + poll everything
HOT_MULTIPLE     = 1.5     # volume vs baseline that earns a hot-list slot
HOT_CHG_1H       = 2.0     # ...or this much price movement
HOT_MAX          = 30      # one multi call

COOLDOWN_HOURS     = 4
BASELINE_HOURS     = 168         # up to 7 days of hourly candles
MIN_BASELINE_OBS   = 6           # young tokens have short histories
BASELINE_MAX_AGE_H = 168         # refresh weekly (BUG 2)
BACKFILL_BUDGET    = 5   # ceiling; the real figure is computed per run from
                         # remaining call headroom so the pond size cannot
                         # silently push monthly usage over budget
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
THROTTLE_AT       = 1.10   # GitHub's scheduler is unreliable, so guessing a
                           # safe cadence is guessing. Measure spend instead:
                           # if we are >10% ahead of the month's pace, force
                           # cheap HOT scans until back on track.
MAX_CALLS_PER_RUN = 18
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
        return "unknown", f"rugcheck unavailable ({err})", [], {}

    fails, notes = [], []
    tok = dig(rep, "token", default={}) or {}
    for label, key in (("mint authority", "mintAuthority"),
                       ("freeze authority", "freezeAuthority")):
        if key in rep or key in tok:
            if rep.get(key, tok.get(key)) not in (None, "", "null"):
                fails.append(f"{label} ACTIVE")

    if dig(rep, "rugged") is True:
        fails.append("flagged rugged")

    # LP. Do NOT take the minimum across every market: a token with one
    # deep locked pool and a $2 unlocked side pool would report 0% and get
    # blocked. Only two views are meaningful --
    #   (a) dollars locked / dollars pooled across all markets
    #   (b) the DEEPEST market's own reported figure
    # Take the lower of those two, bounded to a possible range.
    tot = lock = 0.0
    deepest_pool = deepest_pct = None
    for m_ in dig(rep, "markets", default=[]) or []:
        lp = dig(m_, "lp", default={}) or {}
        b = dig(lp, "baseUSD")
        l = dig(lp, "lpLockedUSD")
        p = as_pct(dig(lp, "lpLockedPct"))
        if not isinstance(b, (int, float)) or b <= 0:
            continue
        pool = b * 2                      # both sides of the AMM
        tot += pool
        if isinstance(l, (int, float)):
            lock += l
        if deepest_pool is None or pool > deepest_pool:
            deepest_pool, deepest_pct = pool, p

    views = []
    if tot > 0:
        views.append(lock / tot)
    if deepest_pct is not None:
        views.append(deepest_pct)
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

    # Provenance. These were arriving in every report and being discarded.
    # A launchpad implies a distribution mechanism and a pre-built audience;
    # a creator with 40 prior tokens is a serial deployer. Neither is proven
    # to predict anything -- log them so the question becomes answerable.
    pad = dig(rep, "launchpad") or dig(rep, "deployPlatform") or ""
    ctok = dig(rep, "creatorTokens")
    ncreated = len(ctok) if isinstance(ctok, list) else ctok
    if pad:
        notes.append(f"via {pad}")
    if isinstance(ncreated, int) and ncreated > 1:
        notes.append(f"creator has {ncreated} tokens")

    detail = ", ".join(notes) if notes else "no detail"
    meta = {"launchpad": pad, "creator_tokens": ncreated}
    if fails:
        return "block", detail, fails, meta
    return (("warn", detail, [], meta) if nets else ("clear", detail, [], meta))


def audit(address):
    """Manual pre-trade check. Never called by the hourly poll."""
    print(f"\nKryptsig audit -- {address}\n" + "=" * 62)

    rep, err = rc_get(f"/tokens/{address}/report")

    # A pool address is not a token mint. If the caller pasted a pool (easy
    # to do -- it is what --pond used to print), resolve it via GeckoTerminal
    # rather than dead-ending on "invalid token mint".
    if err and "invalid token mint" in str(err).lower():
        print(f"  not a token mint -- trying to resolve as a pool address")
        pdata = get(f"/networks/{NET}/pools/{address}")
        attrs = ((pdata or {}).get("data") or {}).get("attributes") or {}
        rels  = ((pdata or {}).get("data") or {}).get("relationships") or {}
        mint = ((attrs.get("base_token") or {}).get("address")
                or (((rels.get("base_token") or {}).get("data") or {})
                    .get("id", "")).split("_")[-1])
        if mint:
            print(f"  resolved pool -> mint {mint}\n")
            address = mint
            rep, err = rc_get(f"/tokens/{address}/report")
        else:
            print("  could not resolve -- paste the token mint instead")

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
    # Some markets have NO LP token at all (lpMint is the System Program
    # null address, lpTotalSupply 0). Their "0% locked" is missing data, not
    # a finding -- but treating it as safe would loosen a safety gate on an
    # assumption. So: report both views, gate on the conservative one.
    NULL_MINT = "11111111111111111111111111111111"
    per_market, total_side, locked_usd = [], 0.0, 0.0
    unaccounted_usd, accounted_side, accounted_locked = 0.0, 0.0, 0.0
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
        no_lp = (str(dig(lp, "lpMint", default="")) == NULL_MINT
                 or (dig(lp, "lpTotalSupply") == 0
                     and dig(lp, "lpCurrentSupply") == 0))
        tag = "  <- no LP token; 0% is missing data, not unlocked" if no_lp else ""
        if no_lp:
            unaccounted_usd += pool_est
        else:
            accounted_side += pool_est
            if isinstance(lusd, (int, float)):
                accounted_locked += lusd
        print(f"        market {i+1}: ~${pool_est:,.0f} pool "
              f"(baseUSD ${busd:,.0f} x2), ${lusd or 0:,.0f} locked"
              f"  [{(locked or 0)*100:.0f}% reported]{tag}")
        if i == 0 and lp:
            print(f"        raw lp: " + ", ".join(
                f"{k}={lp[k]}" for k in sorted(lp)
                if isinstance(lp[k], (int, float, str, bool))
            )[:300])

    views = []
    if per_market:
        deepest = max(per_market, key=lambda x: x[0])
        views.append(("deepest market (reported)", deepest[1]))
        lockedish = sum(p for p, l in per_market if l >= 0.9)
        allpools = sum(p for p, _ in per_market)
        if allpools:
            views.append(("share of pools locked", lockedish / allpools))
    if total_side > 0 and locked_usd > 0:
        views.append(("dollar-weighted", locked_usd / total_side))

    if accounted_side > 0:
        views.append(("LP-accounted markets only", accounted_locked / accounted_side))

    for label, v in views:
        flag = "  (impossible -- ignored)" if v > 1.02 else ""
        print(f"        {label:<26} {v*100:.1f}%{flag}")

    if unaccounted_usd > 0:
        share = unaccounted_usd / (unaccounted_usd + accounted_side) \
            if (unaccounted_usd + accounted_side) else 0
        print(f"\n        ${unaccounted_usd:,.0f} ({share:.0%} of pooled value) sits in")
        print("        markets with no LP token accounting. Whether that")
        print("        liquidity is protocol-locked or withdrawable cannot be")
        print("        determined from this API. Gate uses the worse reading.")

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

    # ---- provenance -------------------------------------------------------
    pad = dig(rep, "launchpad") or dig(rep, "deployPlatform")
    ctok = dig(rep, "creatorTokens")
    n_created = len(ctok) if isinstance(ctok, list) else ctok
    if pad or n_created:
        print("\n[5] provenance")
        if pad:
            print(f"        launchpad        {pad}")
        if n_created:
            print(f"        creator tokens   {n_created}"
                  + ("   <- serial deployer" if isinstance(n_created, int)
                     and n_created >= 5 else ""))

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
def extract_mint(pool):
    """GeckoTerminal is JSON:API -- `relationships` sits BESIDE `attributes`,
    not inside it, and the base token id is formatted 'solana_<mint>'.
    Reading it off attributes (as I first did) silently returned nothing."""
    if not isinstance(pool, dict):
        return ""
    rel = (pool.get("relationships") or {}).get("base_token") or {}
    tid = (rel.get("data") or {}).get("id", "")
    if "_" in tid:
        return tid.split("_", 1)[1]
    return tid or ""


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
    fees = (max(pos * FEE_PCT_PER_SIDE, FEE_MIN_USD) * 2) / pos if pos else 1
    if liquidity >= 100_000:
        tier, note = "A", f"deep -- ${pos:,.0f} is {pct:.2%} of pool"
    elif liquidity >= 50_000:
        tier, note = "B", f"adequate -- ${pos:,.0f} is {pct:.2%} of pool"
    elif liquidity >= 20_000:
        tier, note = "C", f"thin -- ${pos:,.0f}, fees {fees:.1%}"
    else:
        tier, note = "D", (f"VERY THIN -- ${pos:,.0f} only, fees {fees:.1%}, "
                           f"your own exit moves price ~{pct*2:.0%}")
    return tier, pos, note


def enrich(m):
    m["turnover"] = (m["vol_24h"] / m["mcap"]) if m["mcap"] else 0.0
    m["tier"], m["max_pos"], m["tier_note"] = risk_tier(m["liquidity"])
    return m


def liquidity_ok(m):
    """Only the derived floor blocks now. Everything above it gets a tier
    and a position cap instead of a rejection."""
    if m["liquidity"] < MIN_LIQ_ABS:
        return False, (f"liquidity ${m['liquidity']:,.0f} < ${MIN_LIQ_ABS:,} "
                       f"floor")
    pos = m["liquidity"] * MAX_POOL_PCT
    if pos < MIN_POSITION_USD:
        return False, (f"max position ${pos:,.0f} -- fees would exceed 2%")
    return True, ""


# Admission and alerting are deliberately different.
#
# A token 20 minutes old with $8k pooled cannot be traded today -- but in six
# hours it may have $40k and a baseline. If we reject it at admission we never
# see it again, because new_pools only shows it once. So the pond admits
# early and cheaply; the ALERT gates are what protect you.
# A name-based equity filter was removed here. It produced a false positive
# on its first day -- Z500 is an Ansem onchain index token, not a stock --
# and tokenised equities are already excluded in practice by their enormous
# volume baselines. Guessing what a ticker means added risk, not safety.
def is_equity(name):
    return False


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
    if is_equity(m.get("name")):
        return False, "tokenised equity"
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
    """Wider intake. MOMO -- a week-old token with 1,428 holders that ran
    from $170k to $836k -- was never sampled, because one page of new_pools
    plus one rotating page is ~40 pools out of thousands. The gates were
    never the problem on that one; the pond was too small to contain it.

    Now: 2 pages of new_pools (births) + 3 rotating pages of top-by-volume
    (established names that could still wake up). 5 calls instead of 2.
    """
    added = 0

    def admit(payload, src):
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
                                        "added": now_iso(), "src": src}
                added += 1
                print(f"  + {m['name'][:28]:<28} ${m['liquidity']:>9,.0f} liq"
                      f"  {m['age_days']*24:>5.1f}h  [{src}]")

    for page in (1, 2):
        if len(state["pools"]) >= UNIVERSE_CAP:
            break
        admit(get(f"/networks/{NET}/new_pools", f"?page={page}"), "new")

    # Rotate three pages per run through the top-by-volume list, so the whole
    # list is covered every few scans rather than every eight.
    page = state.get("next_page", 1)
    for _ in range(3):
        if len(state["pools"]) >= UNIVERSE_CAP:
            break
        admit(get(f"/networks/{NET}/pools",
                  f"?page={page}&sort=h24_volume_usd_desc"), f"top{page}")
        page = page + 1 if page < 10 else 1
    state["next_page"] = page

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
    # The median includes the token's own bursts. A name that already ran
    # once gets an unreachable bar -- GTA6 came back at $1,273,980/hr on a
    # $61k pool, meaning it would need $3.8M in an hour to trigger. But a
    # baseline is supposed to represent QUIET, not average. Use the 30th
    # percentile: it anchors to the calm hours and is barely moved by spikes.
    vols.sort()
    idx = max(0, int(len(vols) * 0.30) - 1)
    quiet = vols[idx]
    # Guard against a floor of zero on tokens with dead hours.
    if quiet <= 0:
        nonzero = [v for v in vols if v > 0]
        quiet = nonzero[max(0, int(len(nonzero) * 0.30) - 1)] if nonzero else 0
    return quiet, len(vols)


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


def backfill_priority(entry):
    """Lower sorts first. Pools that returned candles are close to a
    baseline; pools that returned NOTHING have no trading at all and should
    not keep jumping the queue ahead of them."""
    n = entry.get("last_candles", 0)
    attempts = entry.get("attempts", 0)
    if attempts == 0:
        return (0, 0)            # never tried -- highest priority
    if n > 0:
        return (1, -n)           # partial history -- closest to ready
    return (2, attempts)         # zero candles, tried before -- back off


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


def evaluate(m, baseline, last_alert, buyer_base=None, candles=0):
    """Two signal types. DORMANT is checked first: an established token with
    a real 7-day baseline waking up is a stronger claim than a young token
    accelerating, and it earns a wider price ceiling."""
    ok, why = liquidity_ok(m)
    if not ok:
        return False, why
    age_h = m["age_days"] * 24
    if age_h < MIN_AGE_HOURS:
        return False, f"{age_h:.1f}h old -- too young to alert"
    if not baseline or baseline <= 0:
        return False, "no baseline yet"

    dormant = (m["age_days"] >= DORMANT_MIN_AGE_DAYS
               and candles >= DORMANT_MIN_CANDLES)
    m["signal"] = "dormant" if dormant else "early"

    if dormant:
        spike, minvol   = DORMANT_SPIKE, DORMANT_MIN_VOLUME
        lo, hi, hi24    = (DORMANT_MIN_CHG_1H, DORMANT_MAX_CHG_1H,
                           DORMANT_MAX_CHG_24H)
        bmult, turn     = DORMANT_MIN_BUYER_MULT, DORMANT_MIN_TURNOVER
    else:
        spike, minvol   = SPIKE_MULTIPLE, MIN_ABS_VOLUME
        lo, hi, hi24    = (MIN_PRICE_CHG_1H, MAX_PRICE_CHG_1H,
                           MAX_PRICE_CHG_24H)
        bmult, turn     = MIN_BUYER_MULT, MIN_TURNOVER

    tag = m["signal"].upper()
    mult = m["vol_1h"] / baseline
    m["multiple"] = round(mult, 1)
    ratio = m["buyers"] / max(m["sellers"], 1)
    m["buyer_ratio"] = round(ratio, 2)

    if mult < spike:
        return False, f"[{tag}] {mult:.1f}x baseline (need {spike}x)"
    if m["vol_1h"] < minvol:
        return False, f"[{tag}] {mult:.1f}x but only ${m['vol_1h']:,.0f}"

    if m["chg_1h"] < lo:
        return False, f"[{tag}] {mult:.1f}x volume, price flat ({m['chg_1h']:+.1f}%)"
    if m["chg_1h"] > hi:
        m["ceiling_reject"] = True
        return False, (f"[{tag}] TOO LATE -- already {m['chg_1h']:+.0f}% this "
                       f"hour (ceiling +{hi:.0f}%)")
    if m["chg_24h"] > hi24:
        m["ceiling_reject"] = True
        return False, f"[{tag}] TOO LATE -- already {m['chg_24h']:+.0f}% in 24h"

    if ratio < MIN_BUYER_RATIO:
        return False, f"[{tag}] buy/sell {ratio:.2f} -- distribution"

    if buyer_base:
        bm = m["buyers"] / max(buyer_base, 1)
        m["buyer_mult"] = round(bm, 1)
        if bm < bmult:
            return False, (f"[{tag}] {mult:.1f}x volume but buyers only "
                           f"{bm:.1f}x -- same wallets churning")
    if m["turnover"] < turn:
        return False, f"[{tag}] turnover {m['turnover']:.1%} -- asleep for its size"

    if last_alert and (time.time() - last_alert) < COOLDOWN_HOURS * 3600:
        return False, "cooldown"

    bstr = f", buyers {m.get('buyer_mult','?')}x" if buyer_base else ""
    lead = ("DORMANT BREAK" if dormant else "ACCUMULATING")
    return True, (f"{lead}: {mult:.1f}x vol{bstr}, {m['chg_1h']:+.1f}% 1h "
                  f"(ceiling +{hi:.0f}%), {ratio:.1f} buy/sell, "
                  f"{m['age_days']:.0f}d old")


def alert_body(addr, m, baseline, reason):
    return (
        f"{m['name']}  [{m.get('signal','?').upper()}]\n{reason}\n\n"
        f"TIER {m['tier']} -- {m['tier_note']}\n"
        f"MAX POSITION ${m['max_pos']:,.0f}\n"
        f"AUDIT {m.get('audit','not run')}\n"
        + (f"LAUNCHPAD {m['launchpad']}\n" if m.get("launchpad") else "")
        + 
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

    # 7 -- golden test. I computed LP lock four different ways and got
    # 100%, 0%, 84.6%, 142.1% and 71%. One of those flipped a rejection into
    # a pass. This pins a known-bad token so that can never happen silently.
    print("\n[7] golden test (known-bad token)")
    GOLDEN = "zj1jpp7QMveWHLs61vL9KMZf254KvW7j4AAmBF8ry2k"
    gv, gdetail, gwhy, _gmeta = audit_summary(GOLDEN)
    if gv == "unknown":
        warn("golden test", f"rugcheck unreachable -- {gdetail}")
    elif gv == "block":
        reason = "; ".join(gwhy)
        ok("golden test", f"correctly blocked: {reason[:60]}")
        # A pass here is not enough. If the LP figure reads 0% the gate is
        # probably taking the minimum across tiny side pools, which would
        # block nearly every multi-pool token and produce silence you would
        # mistake for a quiet market.
        if "LP only 0%" in reason:
            bad("golden test detail",
                "LP computed as 0% -- gate is over-blocking, not working")
    else:
        bad("golden test",
            f"known-bad token returned '{gv}' -- LP gate is broken. {gdetail}")

    # 8 -- notification delivery
    print("\n[8] notification")
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
    if "--log" in sys.argv:
        # Append or update one journal row. Keyed by address so repeat calls
        # for the same token update in place rather than duplicating.
        args = sys.argv[sys.argv.index("--log") + 1:]
        if len(args) < 3:
            print("usage: kryptsig.py --log <address> <field> <value> [note]")
            print("fields: would_i_buy | entry_mcap | outcome_24h | "
                  "outcome_72h | note")
            return
        addr, field, value = args[0], args[1], args[2]
        note = " ".join(args[3:]) if len(args) > 3 else ""

        cols = ["address", "name", "signal", "alerted_ts", "would_i_buy",
                "entry_mcap", "outcome_24h", "outcome_72h", "note",
                "updated_ts"]
        rows, found = [], False
        if os.path.exists(JOURNAL_FILE):
            with open(JOURNAL_FILE) as fh:
                rows = list(csv.DictReader(fh))
        for r in rows:
            if r.get("address") == addr:
                found = True
                if field in cols:
                    r[field] = value
                if note:
                    r["note"] = (r.get("note", "") + " | " + note).strip(" |")
                r["updated_ts"] = now_iso()
        if not found:
            new_row = {c: "" for c in cols}
            new_row.update({"address": addr, "updated_ts": now_iso()})
            if field in cols:
                new_row[field] = value
            new_row["note"] = note
            # pull name/signal/timestamp from the alert log if we have it
            if os.path.exists(ALERT_FILE):
                with open(ALERT_FILE) as fh:
                    for a in csv.DictReader(fh):
                        if addr in (a.get("pool"), a.get("mint")):
                            new_row["name"] = a.get("name", "")
                            new_row["signal"] = a.get("signal", "")
                            new_row["alerted_ts"] = a.get("ts", "")
            rows.append(new_row)

        with open(JOURNAL_FILE, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"{'updated' if found else 'created'} journal row for {addr}")
        print(f"  {field} = {value}" + (f"   note: {note}" if note else ""))
        print(f"\n{len(rows)} row(s) in {JOURNAL_FILE}")
        return

    if "--pond" in sys.argv:
        st = load_json(STATE_FILE, {})
        pools = st.get("pools", {})
        if not pools:
            print("Pond is empty. Run a poll first.")
            return
        rows = []
        for addr, v in pools.items():
            rows.append((
                v.get("name", "?")[:28],
                "yes" if v.get("baseline") else "no",
                f"${v['baseline']:,.0f}/hr" if v.get("baseline") else "-",
                v.get("last_candles", 0),
                v.get("attempts", 0),
                (v.get("added") or "")[5:16].replace("T", " "),
                addr,
                v.get("mint", ""),
            ))
        rows.sort(key=lambda r: (r[1] != "yes", r[0]))
        print(f"\nPond: {len(rows)} pools tracked\n" + "=" * 78)
        print(f"{'name':<28} {'base':<5} {'baseline':<12} {'cdl':>4} "
              f"{'att':>3}  {'added':<12}")
        print("-" * 78)
        for r in rows:
            print(f"{r[0]:<28} {r[1]:<5} {r[2]:<12} {r[3]:>4} {r[4]:>3}  {r[5]:<12}")
        ready = sum(1 for r in rows if r[1] == "yes")
        print("-" * 78)
        print(f"{ready} baselined (can alert), {len(rows)-ready} still warming up")
        print(f"\ncdl = hourly candles returned, att = backfill attempts.")
        print(f"A baseline needs >= {MIN_BASELINE_OBS} candles. An hourly candle only")
        print("exists if trades happened that hour -- so cdl 0 with att > 0")
        print("means the pool has no trading at all, not that it is young.")
        print("\nAddresses  (POOL is the pair, MINT is the token --")
        print("            audit and rugcheck.xyz both need the MINT)")
        for r in rows:
            print(f"  {r[0]}")
            print(f"    pool  {r[6]}")
            print(f"    mint  {r[7] or '(not seen yet -- poll again)'}")
        return

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

    # One 15-minute cron drives both cadences: do the expensive full scan
    # only every FULL_SCAN_HOURS, otherwise poll just the hot list.
    # Purge FIRST. Doing it after the scan decision meant an emptied hot
    # list produced a HOT scan with nothing to poll -- zero API calls, and
    # the health check correctly (but unhelpfully) failed the run.
    stale = [a for a, v in state["pools"].items() if is_equity(v.get("name"))]
    for a in stale:
        del state["pools"][a]
        state["last_alert"].pop(a, None)
    if stale:
        state["hot_list"] = [a for a in state.get("hot_list", [])
                             if a not in stale]
        print(f"purged {len(stale)} grandfathered equity pool(s)")

    month = stamp[:7]
    if state.get("credit_month") != month:
        state["credit_month"], state["credits_used"] = month, 0
    used = state.get("credits_used", 0)
    elapsed = max(int(stamp[8:10]) / 30.0, 0.01)
    pace = (used / MONTHLY_CREDITS) / elapsed
    throttled = pace > THROTTLE_AT

    since_full = time.time() - state.get("last_full_scan", 0)
    hot = [a for a in state.get("hot_list", []) if a in state["pools"]]
    # An empty hot list promotes to a full scan rather than polling nothing.
    full_scan = dry or since_full >= FULL_SCAN_HOURS * 3600 or not hot
    if throttled and full_scan and hot and not dry:
        full_scan = False
        print(f"THROTTLED: {used:,}/{MONTHLY_CREDITS:,} used, {pace:.0%} of "
              f"pace -- forcing cheap HOT scan")
    print(f"scan: {'FULL' if full_scan else 'HOT'} ({len(hot)} hot, "
          f"{since_full/3600:.1f}h since full, {used:,} credits used, "
          f"{pace:.0%} of pace)\n")
    if full_scan:
        state["last_full_scan"] = time.time()

    fixture = load_json("fixture.json", {}).get("pools", []) if dry else []
    if not dry and full_scan:
        added = discover(state)
        print(f"discovery: page {state.get('next_page', 1) - 1 or 8}, "
              f"+{added} (tracking {len(state['pools'])})\n")

        # Young pools cannot produce a baseline (too few candles) and by
        # design do not burn their retry budget -- so in insertion order they
        # occupy every backfill slot forever and starve the mature pools that
        # WOULD succeed. Symptom: "6 alertable, 2 baselined" for hours.
        # Try the oldest first; they are the ones that can actually succeed.
        # Spend only the headroom left after discovery (2) and polling
        # (one call per 30 pools), keeping 2 calls in reserve for retries.
        poll_calls = max(1, -(-len(state["pools"]) // 30))
        # Target ~9 calls/run (65% of monthly credits) rather than the hard
        # 14-call ceiling. A large pond should shrink backfill, not the budget.
        target = 12
        budget = max(1, min(BACKFILL_BUDGET,
                            target - CALLS["n"] - poll_calls))

        # A baseline is worthless on a pool that fails the liquidity gate --
        # evaluate() rejects it before the baseline is ever consulted. Five
        # slots went to $414-$4,083 pools while a $61k candidate waited.
        # Only spend backfill on pools that could actually alert.
        def worth_backfilling(a):
            v = state["pools"][a]
            if not needs_baseline(v):
                return False
            if is_equity(v.get("name")):
                return False
            liq = v.get("last_liq")
            if liq is not None and liq < MIN_LIQ_ABS:
                return False        # seen, and too thin to ever alert
            return True

        pending = [a for a in state["pools"] if worth_backfilling(a)]
        skipped = sum(1 for a in state["pools"]
                      if needs_baseline(state["pools"][a])) - len(pending)
        if skipped:
            print(f"  ({skipped} pending skipped: below alert liquidity "
                  f"or equity)")
        pending.sort(key=lambda a: backfill_priority(state["pools"][a]))
        if pending:
            print(f"  backfill budget this run: {budget} "
                  f"({len(pending)} pending)")
        for addr in pending[:budget]:
            base, n = backfill(addr)
            e = state["pools"][addr]
            e["attempts"] = e.get("attempts", 0) + 1   # always increments
            e["last_candles"] = n
            e["last_try"] = now_iso()
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
    watched = list(state["pools"].keys()) if full_scan else hot[:HOT_MAX]
    batches = ([fixture] if dry
               else [watched[i:i+30] for i in range(0, len(watched), 30)])

    alerts, logged, evaluated, liq_seen = 0, 0, 0, {}
    day = stamp[:10]
    if state.get("alert_day") != day:
        state["alert_day"], state["alerts_today"] = day, 0
    today_alerts = state.get("alerts_today", 0)

    for batch in batches:
        if dry:
            records = [(p["address"], p["attributes"], p) for p in batch]
        else:
            data = get(f"/networks/{NET}/pools/multi/{','.join(batch)}")
            records = [((p.get("attributes") or {}).get("address"),
                        p.get("attributes") or {}, p)
                       for p in ((data or {}).get("data") or [])]

        for addr, attrs, pool_obj in records:
            if not addr:
                continue
            m = enrich(parse_pool(attrs))
            m["mint"] = extract_mint(pool_obj) or m.get("mint", "")
            liq_seen[addr] = m
            entry = state["pools"].setdefault(addr, {"name": m["name"]})
            entry["last_liq"] = m["liquidity"]   # after entry exists
            if m.get("mint"):
                entry["mint"] = m["mint"]
            baseline = entry.get("baseline")
            buyer_base = update_buyer_baseline(entry, m["buyers"])

            evaluated += 1
            fire, reason = evaluate(m, baseline, state["last_alert"].get(addr),
                                    buyer_base, entry.get("last_candles", 0))
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
                    ["ts","pool","mint","name","signal","chg_1h","chg_24h","mcap",
                     "liquidity","multiple","outcome_24h","outcome_72h"],
                    [stamp, addr, m.get("mint",""), m["name"],
                     m.get("signal",""), m["chg_1h"],
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
                verdict, detail, why, meta = ("unknown", "no mint address",
                                              [], {})
                if m.get("mint"):
                    verdict, detail, why, meta = audit_summary(m["mint"])
                m["audit"] = f"{verdict.upper()} -- {detail}"
                m["launchpad"] = meta.get("launchpad", "")
                m["creator_tokens"] = meta.get("creator_tokens", "")

                if verdict == "block":
                    print(f"       ^ SUPPRESSED by audit: {'; '.join(why)}")
                    alerts -= 1
                    today_alerts -= 1
                else:
                    label = ("DORMANT BREAK" if m.get("signal") == "dormant"
                             else "ACCUMULATING")
                    notify(f"{label}: {m['name'][:28]}",
                           alert_body(addr, m, baseline, reason))
                append_row(ALERT_FILE,
                    ["ts","pool","mint","name","signal","tier","launchpad",
                     "creator_tokens","multiple","buyer_mult",
                     "turnover","chg_1h","mcap","liquidity","buyer_ratio",
                     "max_position","friction_pct","audit","would_i_buy",
                     "outcome_24h","outcome_72h","net_24h","net_72h"],
                    [stamp, addr, m.get("mint",""), m["name"],
                     m.get("signal",""), m["tier"],
                     m.get("launchpad",""), m.get("creator_tokens",""),
                     m.get("multiple"), m.get("buyer_mult",""),
                     round(m["turnover"],4), m["chg_1h"], m["mcap"],
                     m["liquidity"], m.get("buyer_ratio"), round(m["max_pos"]),
                     round(round_trip_cost(m["max_pos"], m["tier"])*100, 2),
                     m.get("audit",""), "", "", "", "", ""])

    # Anything stirring goes on the hot list for 15-minute polling.
    if not dry:
        fresh = []
        for a, mm in liq_seen.items():
            b = state["pools"].get(a, {}).get("baseline") or 0
            if b and (mm["vol_1h"] / b >= HOT_MULTIPLE
                      or mm["chg_1h"] >= HOT_CHG_1H):
                fresh.append((mm["vol_1h"] / b if b else 0, a))
        fresh.sort(reverse=True)
        picked = [a for _, a in fresh[:HOT_MAX]]
        if full_scan:
            state["hot_list"] = picked
        else:
            # a hot scan only sees the hot list; keep anything still stirring
            keep = [a for a in state.get("hot_list", []) if a not in liq_seen]
            state["hot_list"] = (picked + keep)[:HOT_MAX]
        print(f"hot list: {len(state['hot_list'])} pool(s) for 15-min polling")

    state["credits_used"] = state.get("credits_used", 0) + CALLS["n"]
    state["alerts_today"] = today_alerts
    dropped = prune(state, liq_seen) if not dry else 0

    mode = "keyed" if CG_KEY else "KEYLESS (expect 429s)"
    # A HOT scan only polls the hot list, so counting "alertable" from what
    # we just saw would report 1 when the pond has 7. Carry the last full
    # scan's figure and label it, rather than printing a number that means
    # something different depending on scan type.
    if full_scan:
        mature = sum(1 for m in liq_seen.values()
                     if m["liquidity"] >= MIN_LIQ_ABS
                     and m["age_days"] * 24 >= MIN_AGE_HOURS)
        state["last_mature"] = mature
        mature_note = ""
    else:
        mature = state.get("last_mature", 0)
        mature_note = " as of last full"
    projected = CALLS["n"] * 24 * 30
    pct = projected / MONTHLY_CREDITS
    print(f"\nbudget: {CALLS['n']} calls this run -> ~{projected:,}/month "
          f"({pct:.0%} of {MONTHLY_CREDITS:,})"
          + ("  ** OVER BUDGET **" if pct > 0.85 else ""))

    # Nudge for outcomes that are due. Without this the log quietly rots --
    # and the log is the entire point.
    due = []
    if not dry and os.path.exists(ALERT_FILE):
        try:
            journal = {}
            if os.path.exists(JOURNAL_FILE):
                with open(JOURNAL_FILE) as fh:
                    journal = {r["address"]: r for r in csv.DictReader(fh)}
            now_ts = time.time()
            with open(ALERT_FILE) as fh:
                for a in csv.DictReader(fh):
                    try:
                        t = datetime.fromisoformat(a["ts"]).timestamp()
                    except Exception:
                        continue
                    key = a.get("pool", "")
                    j = journal.get(key, {})
                    hrs = (now_ts - t) / 3600
                    if hrs >= 24 and not j.get("outcome_24h"):
                        due.append((a.get("name", "?"), "24h"))
                    elif hrs >= 72 and not j.get("outcome_72h"):
                        due.append((a.get("name", "?"), "72h"))
        except Exception as e:
            print(f"  ! outcome check: {e}")

    if due:
        lines = "\n".join(f"{n[:24]} ({w})" for n, w in due[:6])
        print(f"\n{len(due)} outcome(s) due to be logged")
        notify(f"{len(due)} outcome(s) due",
               f"Alerts waiting on results:\n{lines}\n\n"
               f"Actions -> mode: log")

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
          f"pond {len(state['pools'])} ({mature} alertable{mature_note}, "
          f"{len(ready)} baselined) "
          f"| {logged} logged | {dropped} pruned | {alerts} alert(s)")

    # A green check on a run that did nothing is worse than a crash: it
    # produces weeks of false confidence instead of one visible failure.
    problems = []
    if dry:
        return
    if CALLS["n"] == 0 and watched:
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
    if evaluated == 0 and ready and watched:
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
