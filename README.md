# Kryptsig

An **accumulation detector** for Solana tokens.

Kryptsig fires when unique buyers are arriving and price has started moving
but has **not yet gone vertical**. The goal is the window before a token
shows up in a social feed — because by the time someone posts their thesis,
their followers are the exit.

**It holds no keys, touches no wallet, and cannot trade.** Detection only.

---

## The ceiling is the whole idea

Most screeners require a big move. That guarantees you arrive after it.
Kryptsig requires the opposite: price must be up, but **under a ceiling**.

```
volume >= 3x baseline           the move is real
unique buyers >= 2.5x baseline  new wallets, not the same ones churning
buyers/sellers >= 1.3           accumulation, not distribution
price +2% to +60% in 1h         the ramp -- NOT the spike
price under +300% in 24h        did not already run yesterday
```

SalaryCat at +385%/hour is rejected as TOO LATE. The same token six hours
earlier, at +14% with buyers climbing, is exactly what fires.

---

## Liquidity sizes, it does not block

Everything derives from one number: **your max position, $250.**

A $250 position should never exceed 2% of the pool, which sets a hard floor
at $12,500 — below that you cannot exit at a price you would accept. Above
it, you get the alert plus a tier and a position cap:

| Tier | Liquidity | Meaning |
|---|---|---|
| A | $100k+ | deep, clean exit |
| B | $50-100k | adequate |
| C | $25-50k | thin, expect slippage |
| D | $12.5-25k | very thin, real slippage, **check LP lock** |

You are not blocked from thin tokens. You are told what you are taking on.

---

## What this costs you

More alerts — several a day, not several a week. More false positives:
accumulation patterns often just fade. And rugs will now reach your phone.
The tier label is a warning, not a wall.

**Tier D alerts require a manual RugCheck for LP lock before buying.**
Unlocked liquidity means the pool can be withdrawn and your position becomes
unsellable at any price. No price API exposes this.

---

## Deploy (from a phone)

0. **Get a free CoinGecko Demo API key** — coingecko.com/en/api/pricing,
   Demo plan, no card. This is not optional in practice: the keyless tier
   limits by IP, GitHub Actions runners share IPs with thousands of jobs,
   and CoinGecko's own docs say the keyless API is unsuitable for scheduled
   polling. Without a key you will get HTTP 429 on the first call.
1. `ntfy` app → subscribe to an unguessable topic (invent your own string)
2. New GitHub repo, **public** (Actions minutes are unmetered there)
3. Add `kryptsig.py`, `README.md`, `.github/workflows/kryptsig.yml`
4. Settings → Secrets → Actions → add two secrets:
   `NTFY_TOPIC` = your ntfy topic
   `CG_API_KEY` = your CoinGecko Demo key
5. Actions → kryptsig → Run workflow

Nothing to configure. First run discovers pools and backfills 25 baselines;
each later run backfills 25 more.

---

## Selftest — run this after any change

Actions → kryptsig → Run workflow → mode: **selftest**

Validates every external dependency in turn and exits non-zero on failure:

```
[1] credentials      CG_API_KEY, NTFY_TOPIC present
[2] discovery        /networks/solana/pools reachable, returns pools
[3] field contract   every field the parser reads actually exists
[4] mcap coverage    how often market_cap_usd is null (gates divide by it)
[5] ohlcv            candles parse, enough for a baseline
[6] multi endpoint   batch polling returns data
[7] notification     sends a real push to your phone
```

Step 3 is the important one. If GeckoTerminal renames a field, `num()`
returns 0.0 and a healthy token reads as dead — a silent failure that looks
like "nothing is happening." The selftest turns that into a visible FAIL.

## Runs fail loudly

A green check on a run that did nothing is worse than a crash: it produces
weeks of false confidence instead of one visible failure. Kryptsig exits
non-zero when a run is unhealthy — no API calls made, empty universe, all
backfills exhausted, or baselines present but nothing evaluated. You get a
red X in Actions, which is a notification you will actually see.

## Auditing a single token

```
python kryptsig.py --check <token_address>
```

Prints whether the token would be admitted, its baseline, and whether current
conditions trip the trigger. Use it on anything your feed surfaces — it turns
"would this have caught X?" into something you can actually run.

---

## Position sizing

Price impact ≈ trade size ÷ (liquidity ÷ 2). Kryptsig prints a **max position
of 1% of pool liquidity** in every alert. On $88k liquidity that is $880.
Exceed it and your own exit moves the price against you.

---

## What it cannot see

Top-10 holder concentration. Dev wallet size. **LP lock status.**

A token with unlocked liquidity can have its pool withdrawn entirely, leaving
you holding something unsellable at any price. No price API exposes this.

**An alert is a prompt to go look. Never a reason to buy.**

---

## The log is the product

`alerts.csv` has three blank columns: `would_i_buy`, `outcome_24h`,
`outcome_72h`. Fill them in by hand, every time.

`observations.csv` records anything at 2x baseline or above — near-misses
included — so every threshold is **recomputable after the fact**. In three
weeks you can ask "what would 8x have caught that 4x missed, and were those
good?" and answer it from data you already have.

Every row carries a `tier` — micro (<$1M), small ($1-20M), mid ($20M+) —
plus `buyer_mult` and `turnover`. After ~30 days, group outcomes by tier to
find out which band actually performs, rather than assuming micro-caps win.

Then compare alerts you would have bought against ones you would have
skipped. That is the only way to learn whether your judgment adds anything
on top of the trigger.

---

## Operational notes

- **Call budget.** CoinGecko Demo gives 10,000 credits/month at 100 calls/min.
  Kryptsig runs hourly: 1 discovery page (rotating 1-8), 2 backfills, and
  4 batch polls of 30 = **7 calls/run, ~5,000/month, about 50%**. The rest is
  headroom for retries, selftests, and `--check` runs — all of which bill.
- **Hard ceiling of 14 calls per run.** Every 429 retry is a billable call, so
  a bug that triggers retries everywhere could otherwise burn a month of
  credits in an afternoon. At the ceiling the run degrades and says so; the
  quota survives.
- Each run prints projected monthly usage and flags anything over 85%.
- With a key, requests go to api.coingecko.com/api/v3/onchain and are limited
  per key. Without one they go to api.geckoterminal.com and are limited per
  IP. The summary line prints which mode you are in.
- 429s trigger exponential backoff, four attempts, then the call is skipped.
- Baselines refresh weekly, so a token that permanently re-rates stops
  alerting against a stale number.
- Dead pools are pruned automatically to free universe slots.
- Public repo: unmetered Actions. Private: 2,000 min/month — widen the cron.
- Scheduled workflows disable after 60 days of repo inactivity; this commits
  every run, so it keeps itself alive.
