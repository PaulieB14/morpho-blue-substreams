# Morpho Blue + MetaMorpho Substreams

Event-sourced **market state, user positions and MetaMorpho vault balances** for
[Morpho Blue](https://github.com/morpho-org/morpho-blue) on Ethereum mainnet,
emitted as upsert SQL you can sink into your own database.

<img src="assets/icon.png" width="72" align="right" alt="Substreams" />

**Published:**
[`morpho-blue-paulie`](https://substreams.dev/packages/morpho-blue-paulie/v0.1.0) (Ethereum) ·
[`morpho-blue-paulie-base`](https://substreams.dev/packages/morpho-blue-paulie-base/v0.1.0) (Base)

```bash
# Direct artifact URLs — these always work
substreams gui https://spkg.io/v1/packages/morpho-blue-paulie/v0.1.0        # Ethereum
substreams gui https://spkg.io/v1/packages/morpho-blue-paulie-base/v0.1.0   # Base

# Short names, once the registry finishes indexing them
substreams gui morpho-blue-paulie@v0.1.0
substreams gui morpho-blue-paulie-base@v0.1.0
```

## What this is

StreamingFast already ships a Morpho Blue **event decoder**. This package does
not fork it — it *imports* it and spends its modules on the layer that decoder
does not have:

| | StreamingFast `morpho-blue-substreams` | this package |
| --- | --- | --- |
| Blue event tape | ✅ `map_events` | imported, not reimplemented |
| Admin events (`SetFee`, `SetFeeRecipient`, `EnableIrm`, …) | ❌ | ✅ `map_blue_admin` + `store_blue_config` |
| Market params / totals | ❌ | ✅ stores |
| Per-user positions | ❌ | ✅ `store_positions` |
| MetaMorpho vaults | ❌ | ✅ factory + vault share tracking |
| SQL output | append-only event tables | **upsert** current-state tables |

## Module graph

```
                        ┌─ store_market_params ─┐
morpho_sf:map_events ───┼─ store_market_totals ─┤
                        └─ store_positions ─────┤
                                 ▲              │
map_blue_admin ─→ store_fee_recipient           ├─→ db_out ─→ DatabaseChanges
              ├─→ store_market_fee ─────────────┤
              └─→ store_blue_config ────────────┤
                                                │
map_metamorpho_factory ─→ store_vaults ─┐       │
                                        ▼       │
                        map_metamorpho_events ──┤
                                 ├─→ store_vault_positions
                                 └─→ store_vault_totals
```

## Correctness notes

Three things make event-sourced Morpho accounting easy to get wrong. All are
handled here, and all are documented in
[`docs/MORPHO_BLUE_ACCOUNTING.md`](docs/MORPHO_BLUE_ACCOUNTING.md).

1. **Events must be replayed in log order.** The upstream `Events` message groups
   events *by type*. Iterating field-by-field can apply an `AccrueInterest` after
   a `Supply` that actually came later in the same transaction — on-chain,
   interest always accrues first. Every store here merge-sorts on `log_index`
   before applying anything.

2. **Fee shares are minted silently.** `AccrueInterest` credits `feeShares`
   straight into the fee recipient's supply position and emits **no** `Supply`
   event (`EventsLib` says so explicitly). The upstream decoder does not decode
   `SetFeeRecipient`, so this package adds `map_blue_admin` and reads the
   recipient *at the accrual's own ordinal* — a `SetFeeRecipient` later in the
   same block must not be applied retroactively.

3. **Bad debt is socialized.** On `Liquidate` with bad debt, the loss comes off
   `totalBorrowAssets` **and** `totalSupplyAssets`, and the borrower's remaining
   borrow shares are zeroed. Suppliers eat it.

Values are raw on-chain integers. Shares are **not** assets — convert with
Morpho's virtual-share math (`VIRTUAL_SHARES = 1e6`, `VIRTUAL_ASSETS = 1`)
against the matching `market_states` row.

## Build and run

The upstream decoder is **not published to the Substreams registry** (it only
exists as source in `streamingfast/substreams-chain-modules`), so a build of it
is vendored at `vendor/` to keep this package self-contained.
`make vendor` regenerates it from source.

```bash
make build          # cargo build --target wasm32-unknown-unknown --release
make pack           # -> morpho-blue-paulie-v0.1.0.spkg
make stale          # guard: fails if the .wasm is older than src/

substreams run morpho-blue-paulie-v0.1.0.spkg db_out \
  -e mainnet.eth.streamingfast.io:443 --start-block 18883124 --stop-block +1000
```

> `substreams pack` does **not** compile. It packages whatever `.wasm` sits at
> the manifest path, so always `make build` first — `make stale` catches it.

Sink the output with
[substreams-sink-sql](https://github.com/streamingfast/substreams-sink-sql)
against [`schema.sql`](schema.sql). Numeric columns default to `0` because
`db_out` emits only the columns that changed in a block.

## Tables

| table | key | holds |
| --- | --- | --- |
| `markets` | `market_id` | the 5-tuple: loan/collateral token, oracle, IRM, LLTV |
| `market_states` | `market_id` | supply/borrow assets and shares, collateral, fee |
| `positions` | `{market_id}:{user}` | `supply_shares`, `borrow_shares`, `collateral` |
| `vaults` | vault address | MetaMorpho name, symbol, asset, factory |
| `vault_positions` | `{vault}:{user}` | vault share balance |
| `vault_states` | vault address | `total_shares` (exact), `net_deposited_assets` |
| `blue_config` | `owner` / `irm:…` / `lltv:…` | protocol owner and enabled IRM/LLTV sets |
| `liquidatable_positions` | `{market_id}:{user}` | known-underwater positions + health factor |
| `market_bad_debt` | `market_id` | cumulative realized bad debt |
| `borrower_bad_debt` | `{market_id}:{borrower}` | bad debt attributed to the borrower, with a count |

## Versus the Morpho API

Be clear-eyed about this: the [Morpho API](https://docs.morpho.org/developers/api/morpho/)
is excellent and covers more than this package. It has USD values, APYs at eight
timescales, rewards, oracle prices, PnL and ROE per position, `healthFactor`,
`priceVariationToLiquidationPrice`, and bulk paging (59k positions at 1,000 a
page, no rate limiting observed). For most consumers it is the right answer.

What this adds:

| | Morpho API | this package |
| --- | --- | --- |
| Market state, positions, vault shares | ✅ richer (USD, APY, PnL) | ✅ raw integers |
| `healthFactor` | ✅ | ✅ streamed per block, not polled |
| **Seizable collateral + liquidation incentive** | ❌ | ✅ `map_position_risk` |
| **Bad debt per borrower** | ❌ market-level only | ✅ `borrower_bad_debt` |
| Reorg undo signals | ❌ | ✅ inherent to Substreams |
| Composable with other Substreams | ❌ | ✅ module imports |
| You own the pipeline | ❌ | ✅ |

The liquidation economics fall out of the contract math rather than the API:

```
LIF     = min(1.15e18, WAD / (WAD - 0.3e18 * (WAD - lltv)))
seizable = borrowed.wMulDown(LIF).mulDivDown(1e36, oraclePrice)   // capped at collateral
```

`map_position_risk` reads `IOracle.price()` over RPC at 1e36 scale.
**Caveat:** positions are only re-priced when an event touches them. A position
that crosses into liquidation purely because the oracle moved is not seen until
the next touch — re-pricing every open position every block would mean an RPC
call per market per block. Treat `liquidatable_positions` as "known
liquidatable as of last touch", not a complete real-time feed.

## Scope

**In:** Ethereum mainnet, Morpho Blue from block 18883124, MetaMorpho factories
V1.1 (`0x1897A899…`) and V1 (`0xA9c3D3a3…`).

**Not yet:** USD prices, APYs and rewards (use the
[Morpho API](https://docs.morpho.org/developers/api/morpho/) — it already does
these well); live health factors, which need an oracle `price()` read at 1e36
scale — `positions` + `market_states` + `lltv` + the oracle address are emitted
so a consumer can join prices themselves; Vault V2; other chains (see below).

Note `vault_states.net_deposited_assets` is deposit principal, **not** AUM — a
vault also earns interest in the underlying Blue markets, which emits no
vault-level event. For true AUM, join the vault's own rows in `positions`.

### Networks

A Substreams package is pinned to a single `network:`, so this repo ships two
manifests from one Rust crate:

| | Ethereum | Base |
| --- | --- | --- |
| manifest | `substreams.yaml` | `substreams.base.yaml` (generated) |
| package | `morpho_blue_paulie` | `morpho_blue_paulie_base` |
| Morpho Blue | `0xBBBB…FFCb` | same address |
| initial block | 18883124 | 13977148 |
| MetaMorpho factories | `0x1897A899…`, `0xA9c3D3a3…` | `0xFf62A7c2…`, `0xA9c3D3a3…` |

The Base initial block was found by binary-searching `eth_getCode` against the
Blue address, not taken from a doc. All known factories are checked on both
chains — an address that is not a factory on a given chain simply never emits
`CreateMetaMorpho`.

Regenerate the Base manifest with `make base-manifest`; it is derived from
`substreams.yaml` so the two cannot drift. Build both with
`make pack && make pack-base`.

Other Morpho deployments (Arbitrum, Polygon, Unichain, …) follow the same
recipe: repack the vendored decoder for that network, add the factory address,
generate a manifest.

## Verification

**Event-sourced totals match on-chain storage exactly.** Verified by picking
markets created *after* the manifest's initial block — so the stores observe
their entire life — running to a fixed block, and reading `Morpho.market(id)`
at that same block via archive `eth_call`:

| market | field | indexed | on-chain |
| --- | --- | --- | --- |
| `0x8ab1b309…` | `total_supply_assets` | 101000 | 101000 |
| | `total_supply_shares` | 101000000000 | 101000000000 |
| | `total_borrow_assets` | 90000 | 90000 |
| | `total_borrow_shares` | 90000000000 | 90000000000 |
| `0xdd89d343…` | all four | 500000 / 500000000000 / 500000 / 500000000000 | identical |

(Ethereum, block 25,905,044.) Both packages were also streamed from their
published `spkg.io` artifacts rather than local builds. MetaMorpho decoding was
checked against a real vault creation — "MEV Capital M^0 Vault" / `MC.wM` at
block 20,873,628 — and matches the Morpho API exactly.

**What is NOT exercised:** the `AccrueInterest` fee-share path. It is
implemented and documented above, but **no market currently has a non-zero
fee** (0 of 300 Ethereum markets sampled via the Morpho API), so `feeShares` is
always 0 and the branch never fires against live data. It exists because
governance can enable a fee up to `MAX_FEE` (25%) at any time, at which point an
indexer without it silently gets the fee recipient's balance wrong forever.
Treat that path as defensive, not battle-tested.

Full-history backfill totals are also unverified — backfill from block 18883124
exceeds the free tier's 10,000-block limit, which is why the checks above use
recently created markets instead.

## License

MIT
