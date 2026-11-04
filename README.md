# Lynx Relay

<!-- mcp-name: io.github.naitly0909/lynx-relay -->

**One local address for your free blockchain RPC allowances.** Open one free account at each provider, paste the keys once, and point your bot at `http://127.0.0.1:8899/solana`. When one provider's free allowance runs out, rate-limits you or goes down, the relay moves to the next one by itself. Built for beginners who want to build without paying for an RPC plan.

- **One account per provider.** The relay switches *between different providers*. It is not a tool for opening several accounts at the same provider: that usually breaks their terms and can get you banned.
- **Keys stay on your machine.** They are stored in `~/.lynx-relay/config.json` (mode 600) or in environment variables, and are sent only to the provider they belong to. Nothing goes through LynxCheck or anyone else.
- **Standard library only.** Python 3.9+, no dependencies, no account with us.
- Requests (HTTP JSON-RPC) and streams (Solana `...Subscribe` methods, EVM `eth_subscribe`) are both switched. A transaction is never sent twice after an unclear failure (see below).

## Quick start

```bash
git clone https://github.com/naitly0909/lynx-relay
cd lynx-relay
python3 -m lynxrelay providers          # which providers have a free tier, with the source of each number
python3 -m lynxrelay init               # asks for one key per provider; press Enter to skip any
python3 -m lynxrelay test               # one cheap call to every configured source
python3 -m lynxrelay serve              # http://127.0.0.1:8899
```

Avoid `add --key ...` on a shared machine (the key lands in your shell history); `init` and `add` without `--key` ask for it without echoing.

Then use `http://127.0.0.1:8899/solana` for requests and `ws://127.0.0.1:8899/ws/solana` for streams (or `/evm/eth`, `/evm/base`, `/evm/bsc`, `/evm/arb`, `/evm/polygon`, `/evm/avax`, `/evm/op`) wherever your bot wants an RPC URL. `GET /status` shows which source is active and how much of each allowance is used. Without any key the relay already works with free public nodes (slower, no guarantees), so you can start in one minute and add providers later.

With `pip install .` the command is `lynx-relay` instead of `python3 -m lynxrelay`.

## Where to get free keys

`lynx-relay providers` prints the list with the sign-up link of each provider. Numbers change without notice; every row says where it comes from (`measured`, `vendor_doc`, `third_party_unverified`) and the catalog carries the date it was last checked. Always compare with the provider's own page. Add or fix entries in `~/.lynx-relay/catalog.local.json` (same format as `lynxrelay/catalog.json`; entries with the same id replace the built-in ones).

| provider | needs | free allowance (see the command for the current text) |
|---|---|---|
| Helius | key | 1,000,000 credits per month, 10 requests/s (Solana) |
| Alchemy | key | 30M compute units per month (Solana + 7 EVM networks; enable each network in the dashboard) |
| Chainstack, Ankr, Syndica | full endpoint URL | free plans, limits in the catalog (Solana) |
| Solana public RPC, PublicNode, Tenderly, thirdweb and others | nothing | free public nodes, no guarantees |

## How the switching works

- Sources are tried in order: providers you configured first (catalog order, or your own `order` in the config), free public nodes last.
- **Rate limit (429, or a node's own rate-limit error):** that source rests for the `Retry-After` time (or 5 s, 15 s, 30 s ... up to 5 min) and the call goes to the next one.
- **Allowance:** every call adds its cost to a local counter. The relay stops using a source at 98% of its monthly allowance (restarts on your billing day, set `reset_day`) and also reacts to the provider's own quota error. Your provider's dashboard is the truth; the counter is a safety margin.
- **Plan limits:** if a node says a method is not available on its plan, that method skips that source for an hour.
- **Wrong key (401/403):** the source is switched off and shown as `disabled` in the status until you fix the key.
- **A node's definitive answer** (invalid params, reverted call) is returned as it is and is not retried elsewhere.
- **Transactions:** `sendTransaction` / `eth_sendRawTransaction` move to the next source only after a clear refusal (429, quota, auth). After a timeout or a 5xx the relay stops and tells you to check the signature first, because the first node may already have processed it.
- If everything is busy you get a JSON-RPC error with the reasons and `retry_in_s`; error texts never contain URLs or keys.

## Streams

Point a stream client (for example a recorder that uses `logsSubscribe`) at `ws://127.0.0.1:8899/ws/solana`. For EVM chains use `ws://127.0.0.1:8899/ws/evm/base` (or eth, bsc, arb, polygon, op, avax) with `eth_subscribe`, for example `newHeads`; measured 2026-11-04 with the free PublicNode streams. The relay keeps one connection to the active provider and remembers your subscriptions. When that connection drops, the provider refuses (rate limit, quota, wrong key) or its allowance is used up, the relay connects to the next source, subscribes again and keeps the subscription ids you were given, so your client only sees a short gap. Stream data is metered by the bytes it delivers (for example Helius: 20 credits per MB) against the same allowance as your requests. Every few minutes the relay checks whether a preferred source has recovered (for instance after its monthly reset) and moves back to it.
Not replayed: requests that were in flight during a switch (retry them) and messages missed in the gap. Free public nodes carry no guarantees; measured 2026-11-03: the Solana public node accepted 40 per-token subscriptions and delivered trades, one other public node did not.

## Safety

The proxy listens on `127.0.0.1` only. Requests with an `Origin` header (a web page in your browser) or a foreign `Host` header are refused, so a website cannot spend your allowance. Request bodies are never logged. `status` and the MCP tools show provider names and counters, never URLs or keys.

## Use from an AI assistant (MCP)

```json
{ "mcpServers": { "lynx-relay": { "command": "python3", "args": ["-m", "lynxrelay", "mcp"], "cwd": "/path/to/relay" } } }
```

Quick connect for Claude Code: `claude mcp add lynx-relay -e PYTHONPATH=/path/to/relay -- python3 -m lynxrelay mcp`.

Tools: `relay_free_tiers`, `relay_status`, `relay_rpc` (read-only calls), `relay_setup`. Keys are never entered through the assistant, because they would stay in the chat history: use `init` or `add` in a terminal.

## Limits of version 0.2

- Streams: Solana (`/ws/solana`; Helius, Alchemy, the Solana public node) and EVM (`/ws/evm/<chain>` with `eth_subscribe`; PublicNode for eth, base, bsc, arb, polygon, op and avax without a key, Alchemy with one). Subscription ids stay the same after a switch (hex strings on EVM).
- Call costs are an average per provider (1 credit for Helius, a guess of 20 compute units for Alchemy). Treat the percentage as an estimate.
- The catalog is only as good as its last check. Please send corrections.

Information only. This tool is not affiliated with any provider and gives no financial advice.
