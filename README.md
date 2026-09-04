# Morpho Blue + MetaMorpho Substreams

Event-sourced **market state, user positions and MetaMorpho vault balances** for
[Morpho Blue](https://github.com/morpho-org/morpho-blue) on Ethereum mainnet,
emitted as upsert SQL you can sink into your own database.

<img src="assets/icon.png" width="72" align="right" alt="Substreams" />

**Published:**
[`morpho-blue-paulie`](https://substreams.dev/packages/morpho-blue-paulie/v0.1.0) (Ethereum) ·
[`morpho-blue-paulie-base`](https://substreams.dev/packages/morpho-blue-paulie-base/v0.1.0) (Base)

```bash
substreams gui morpho-blue-paulie@v0.1.0            # Ethereum
substreams gui morpho-blue-paulie-base@v0.1.0       # Base
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

Validated against mainnet at block 21,000,000 and at real vault-creation blocks:
markets, positions and vault events decode and accumulate, and vault metadata
matches the Morpho API exactly (e.g. `MC.wM` / "MEV Capital M^0 Vault" at block
20,873,628). Absolute totals across a full backfill are **not** yet verified —
that needs a paid endpoint, since store backfill from block 18883124 exceeds the
free tier's 10,000-block limit.

## License

MIT
