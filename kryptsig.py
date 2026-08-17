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
MIN_LIQ_ABS      = 30_000        # hard floor in USD
MIN_LIQ_RATIO    = 0.15          # ...and >= 15% of market cap
MIN_AGE_DAYS     = 14
MAX_MCAP         = 200_000_000   # raised; size_tier logs which band performs
UNIVERSE_CAP     = 400

# ======================================================================
# SIGNAL -- loose on purpose during the logging phase.
# ======================================================================
SPIKE_MULTIPLE   = 4.0
MIN_ABS_VOLUME   = 10_000
MIN_PRICE_CHG_1H = 8.0
MIN_BUYER_RATIO  = 1.2
MIN_BUYER_MULT   = 3.0           # unique buyers vs their own 7d baseline
MIN_TURNOVER     = 0.10          # 24h volume / mcap -- real activity for its size

# ======================================================================
# SAFETY -- do not loosen. Protects your ability to exit.
# ======================================================================
MAX_FDV_MC_RATIO = 1.5
POSITION_PCT     = 0.01          # never exceed 1% of pool liquidity

COOLDOWN_HOURS     = 6
BASELINE_HOURS     = 168         # 7 days of hourly candles
MIN_BASELINE_OBS   = 48
BASELINE_MAX_AGE_H = 168         # refresh weekly (BUG 2)
BACKFILL_BUDGET    = 25
MAX_BACKFILL_TRIES = 3           # then give up (BUG 3)
LOG_MIN_MULTIPLE   = 2.0         # throttle the log (BUG 1)

GT   = "https://api.geckoterminal.com/api/v2"
NET  = "solana"
PACE = 2.2

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
    req = urllib.request.Request(
        f"{GT}{path}{params}",
        headers={"Accept": "application/json", "User-Agent": "kryptsig/3.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f"  ! {path} -> {e}")
        data = None
    time.sleep(PACE)
    return data


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
        "buyers":    tx1.get("buyers", 0) or 0,
        "sellers":   tx1.get("sellers", 0) or 0,
        "age_days":  round(age_days, 1),
        "turnover":  0.0,   # filled in below once mcap is known
    }


def size_tier(mcap):
    if mcap < 1_000_000:
        return "micro"
    if mcap < 20_000_000:
        return "small"
    return "mid"


def enrich(m):
    m["turnover"] = (m["vol_24h"] / m["mcap"]) if m["mcap"] else 0.0
    m["tier"] = size_tier(m["mcap"])
    return m


def liquidity_ok(m):
    """Absolute floor AND a ratio that scales with market cap (BUG 6).
    A $300k token with $50k pooled passes; with $12k pooled it does not."""
    if m["liquidity"] < MIN_LIQ_ABS:
        return False, f"liquidity ${m['liquidity']:,.0f} < ${MIN_LIQ_ABS:,} floor"
    if m["mcap"] > 0:
        ratio = m["liquidity"] / m["mcap"]
        if ratio < MIN_LIQ_RATIO:
            return False, (f"liquidity {ratio:.1%} of mcap "
                           f"(need {MIN_LIQ_RATIO:.0%}) -- too thin to exit")
    return True, ""


def qualifies(m):
    ok, why = liquidity_ok(m)
    if not ok:
        return False, why
    if m["age_days"] < MIN_AGE_DAYS:
        return False, f"only {m['age_days']}d old (need {MIN_AGE_DAYS}d)"
    if m["mcap"] > MAX_MCAP:
        return False, f"mcap ${m['mcap']:,.0f} too large"
    return True, "qualifies"


def discover(state):
    added = 0
    for page in range(1, 9):
        if len(state["pools"]) >= UNIVERSE_CAP:      # BUG 4
            break
        data = get(f"/networks/{NET}/pools",
                   f"?page={page}&sort=h24_volume_usd_desc")
        if not data or not data.get("data"):
            break
        for pool in data["data"]:
            if len(state["pools"]) >= UNIVERSE_CAP:
                break
            attrs = pool.get("attributes") or {}
            addr = attrs.get("address")
            if not addr or addr in state["pools"]:
                continue
            ok, _ = qualifies(parse_pool(attrs))
            if ok:
                state["pools"][addr] = {
                    "name": parse_pool(attrs)["name"], "baseline": None,
                    "baseline_ts": None, "tries": 0, "added": now_iso()}
                added += 1
    return added


def prune(state, seen_liquidity):
    """Free slots held by pools that have died (BUG 5)."""
    dropped = 0
    for addr, liq in seen_liquidity.items():
        if liq < MIN_LIQ_ABS and addr in state["pools"]:
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
    ok, why = liquidity_ok(m)
    if not ok:
        return False, why
    if m["mcap"] and m["fdv"] and m["fdv"] / m["mcap"] > MAX_FDV_MC_RATIO:
        return False, f"FDV/MC {m['fdv']/m['mcap']:.1f}x -- unlock overhang"
    if not baseline or baseline <= 0:
        return False, "no baseline yet"

    mult = m["vol_1h"] / baseline
    m["multiple"] = round(mult, 1)
    ratio = m["buyers"] / max(m["sellers"], 1)
    m["buyer_ratio"] = round(ratio, 2)

    if mult < SPIKE_MULTIPLE:
        return False, f"{mult:.1f}x baseline (need {SPIKE_MULTIPLE}x)"
    if m["vol_1h"] < MIN_ABS_VOLUME:
        return False, f"{mult:.1f}x but only ${m['vol_1h']:,.0f} -- too small"
    if m["chg_1h"] < MIN_PRICE_CHG_1H:
        return False, f"{mult:.1f}x volume, price only {m['chg_1h']:+.1f}%"
    if ratio < MIN_BUYER_RATIO:
        return False, f"{mult:.1f}x but buy/sell {ratio:.2f} -- distribution"

    # New money, not the same wallets churning. Hardest metric to fake:
    # inflating volume needs 2 wallets, inflating unique buyers needs 300.
    if buyer_base:
        bmult = m["buyers"] / max(buyer_base, 1)
        m["buyer_mult"] = round(bmult, 1)
        if bmult < MIN_BUYER_MULT:
            return False, (f"{mult:.1f}x volume but buyers only {bmult:.1f}x "
                           f"-- same wallets churning")
    if m["turnover"] < MIN_TURNOVER:
        return False, f"turnover {m['turnover']:.1%} of mcap -- asleep for its size"

    if last_alert and (time.time() - last_alert) < COOLDOWN_HOURS * 3600:
        return False, "cooldown"

    bm = f", buyers {m.get('buyer_mult','?')}x" if buyer_base else ""
    return True, (f"{mult:.1f}x vol{bm}, {m['chg_1h']:+.1f}% 1h, "
                  f"{ratio:.1f} buy/sell, {m['turnover']:.0%} turnover")


def alert_body(addr, m, baseline, reason):
    return (
        f"{m['name']}\n{reason}\n\n"
        f"Price      ${m['price']:.10f}".rstrip("0") + "\n"
        f"Market cap ${m['mcap']:,.0f}\n"
        f"Liquidity  ${m['liquidity']:,.0f}  "
        f"({m['liquidity']/m['mcap']:.0%} of mcap)\n" if m["mcap"] else ""
    ) + (
        f"MAX POSITION ${m['liquidity']*POSITION_PCT:,.0f}\n"
        f"1h volume  ${m['vol_1h']:,.0f}  vs baseline ${baseline:,.0f}\n"
        f"Buy/sell   {m['buyers']}/{m['sellers']}"
        + (f"  (buyers {m['buyer_mult']}x baseline)" if m.get("buyer_mult") else "")
        + f"\nTurnover   {m['turnover']:.0%} of mcap   [{m['tier']}]\n"
        f"Age        {m['age_days']}d\n\n"
        f"geckoterminal.com/solana/pools/{addr}\n\n"
        f"NOT A BUY SIGNAL.\n"
        f"Verify top-10 holders, dev wallet, LP lock before anything."
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
    print(f"\n  Max position ${m['liquidity']*POSITION_PCT:,.0f} "
          f"({POSITION_PCT:.0%} of liquidity)")
    print("\n  Kryptsig cannot see holder concentration, dev wallet, or LP lock.")


# ----------------------------------------------------------------------
def main():
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
        print(f"discovery: +{discover(state)} "
              f"(tracking {len(state['pools'])})\n")

        pending = [a for a, v in state["pools"].items() if needs_baseline(v)]
        for addr in pending[:BACKFILL_BUDGET]:
            base, n = backfill(addr)
            e = state["pools"][addr]
            e["tries"] = e.get("tries", 0) + 1
            if base:
                e["baseline"], e["baseline_ts"] = base, now_iso()
            print(f"  baseline {e['name'][:22]:<22} {n} candles  ${base or 0:,.0f}/hr")
        if pending:
            print()

    ready = [a for a, v in state["pools"].items() if v.get("baseline")]
    batches = [fixture] if dry else [ready[i:i+30] for i in range(0, len(ready), 30)]

    alerts, logged, liq_seen = 0, 0, {}

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
            liq_seen[addr] = m["liquidity"]
            entry = state["pools"].setdefault(addr, {"name": m["name"]})
            baseline = entry.get("baseline")
            buyer_base = update_buyer_baseline(entry, m["buyers"])

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

            if fire:
                alerts += 1
                state["last_alert"][addr] = time.time()
                notify(f"WAKE-UP: {m['name'][:30]}",
                       alert_body(addr, m, baseline, reason))
                append_row(ALERT_FILE,
                    ["ts","pool","name","tier","multiple","buyer_mult","turnover",
                     "chg_1h","mcap","liquidity","buyer_ratio","max_position",
                     "would_i_buy","outcome_24h","outcome_72h"],
                    [stamp, addr, m["name"], m["tier"], m.get("multiple"),
                     m.get("buyer_mult",""), round(m["turnover"],4), m["chg_1h"],
                     m["mcap"], m["liquidity"], m.get("buyer_ratio"),
                     round(m["liquidity"]*POSITION_PCT), "", "", ""])

    dropped = prune(state, liq_seen) if not dry else 0

    with open(STATE_FILE, "w") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)

    print(f"\n{stamp} | tracking {len(state['pools'])} | {len(ready)} live "
          f"| {logged} logged | {dropped} pruned | {alerts} alert(s)")


if __name__ == "__main__":
    main()
