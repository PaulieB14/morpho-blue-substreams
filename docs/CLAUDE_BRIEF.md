# Claude brief: PaulieB14/morpho-blue-substreams (lego compose)

Build a **composable** Morpho Blue + MetaMorpho Substreams package in this repo.
**Do not fork** StreamingFast’s event decoder. **Import** it and stack modules on top.

## Why lego

StreamingFast already ships `morpho-blue-substreams@v0.1.0` with a working Blue event map:

- Package: `morpho-blue-substreams@v0.1.0`
- Module: `map_events` → `proto:morpho_blue.types.v1.Events`
- Network: `mainnet`
- Morpho Blue: `0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb`
- Initial block: `18883124`
- Events covered: CreateMarket, Supply, SupplyCollateral, Borrow, Repay, Withdraw, WithdrawCollateral, Liquidate, AccrueInterest, FlashLoan
- Their `db_out` is append-only SQL event tables only — **no stores, no MetaMorpho, no positions**

We own the product layer: stores + MetaMorpho + upsert sinks shaped like [Morpho’s API](https://docs.morpho.org/developers/api/morpho/).

Verify anytime:

```bash
substreams info morpho-blue-substreams@v0.1.0
```

## MUST READ: Blue accounting

Study and follow **[`MORPHO_BLUE_ACCOUNTING.md`](./MORPHO_BLUE_ACCOUNTING.md)** (derived from [morpho-org/morpho-blue](https://github.com/morpho-org/morpho-blue)).

Non-negotiable correctness rules:

1. **Fee shares are silent** — `AccrueInterest.feeShares` mint to current `feeRecipient` with **no** `Supply` event (`EventsLib` warning). Track `feeRecipient` via our own `map_blue_admin` (`SetFeeRecipient`); apply fee shares into that address’s supply position.
2. **Bad debt socializes to suppliers** — on `Liquidate`, subtract `bad_debt_assets` from **both** `totalBorrowAssets` and `totalSupplyAssets`.
3. **Virtual shares** — `VIRTUAL_SHARES=1e6`, `VIRTUAL_ASSETS=1` when converting shares↔assets for API/HF views; borrows use `toAssetsUp`.
4. **Health** — `maxBorrow = collateral * oraclePrice / 1e36 * lltv`; healthy iff `maxBorrow >= borrowShares.toAssetsUp(...)`. Oracle scale is **1e36**.
5. Still **import** SF `map_events` for the high-volume tape; add a thin **`map_blue_admin`** for `SetFee`, `SetFeeRecipient`, `EnableIrm`, `EnableLltv`, `SetOwner` (SF stub omits these). Prefer enrich → stores over forking SF decode.

Recommended graph tweak:

```
morpho_sf:map_events ──┐
map_blue_admin ────────┼─► map_position_deltas (join feeRecipient) ─► store_positions / store_market_totals
                       └─► store_market_params (CreateMarket)
```

## Manifest pattern (required)

```yaml
specVersion: v0.1.0
package:
  name: morpho_blue_paulie
  version: v0.1.0
  url: https://github.com/PaulieB14/morpho-blue-substreams

imports:
  morpho_sf: morpho-blue-substreams@v0.1.0
  database: https://github.com/streamingfast/substreams-sink-database-changes/releases/download/v1.0.0/substreams-sink-database-changes-v1.0.0.spkg
  # bump database-changes version to whatever current SF packs use (SF stub used v4.0.0)

network: mainnet

modules:
  - name: store_market_params
    kind: store
    updatePolicy: set
    valueType: proto:morpho_paulie.v1.MarketParams
    inputs:
      - map: morpho_sf:map_events

  - name: store_market_totals
    kind: store
    updatePolicy: add
    valueType: bigint  # or proto; pick one and stick to it
    inputs:
      - map: morpho_sf:map_events

  - name: store_positions
    kind: store
    updatePolicy: add
    valueType: bigint
    inputs:
      - map: morpho_sf:map_events

  - name: map_metamorpho_events
    kind: map
    inputs:
      - source: sf.ethereum.type.v2.Block
      # optional: store of known vault addresses once factory create is indexed
    output:
      type: proto:morpho_paulie.v1.MetaMorphoEvents

  - name: store_vaults
    kind: store
    updatePolicy: set
    valueType: proto:morpho_paulie.v1.VaultMeta
    inputs:
      - map: map_metamorpho_events

  - name: store_vault_positions
    kind: store
    updatePolicy: add
    valueType: bigint
    inputs:
      - map: map_metamorpho_events

  - name: db_out
    kind: map
    inputs:
      - store: store_market_params
        mode: deltas
      - store: store_market_totals
        mode: deltas
      - store: store_positions
        mode: deltas
      - store: store_vaults
        mode: deltas
      - store: store_vault_positions
        mode: deltas
      # optionally also morpho_sf:map_events for raw event tables
    output:
      type: proto:sf.substreams.sink.database.v1.DatabaseChanges
```

Adjust `database` import URL/version to match a known-good StreamingFast pack (see SF morpho stub’s `substreams.yaml` which used `substreams-sink-database-changes` **v4.0.0**).

Module names under imports are referenced as `morpho_sf:map_events`.

## Position / store semantics (match Morpho API)

Morpho REST position fields: `collateral_assets`, `supply_shares`, `borrow_shares` per `(chain, market_id, user)`.

| Event | Position updates (user = `on_behalf` unless noted) |
| --- | --- |
| Supply | +supply_shares, (assets informational) |
| Withdraw | −supply_shares |
| Borrow | +borrow_shares |
| Repay | −borrow_shares |
| SupplyCollateral | +collateral |
| WithdrawCollateral | −collateral |
| Liquidate | borrower: −collateral (seized), −borrow_shares (repaid); track bad debt fields on market totals |
| AccrueInterest | market totals: +interest to borrow side / fee_shares — follow Morpho Blue accounting (totalBorrowAssets += interest, etc.) |
| CreateMarket | set market params only |

**Keys**

- Market params / totals: `market_id` (bytes32 hex lowercased with `0x`)
- Position: `{market_id}:{user}` (user lowercased)
- Prefer `add` stores with signed bigint deltas for shares/collateral; or a single proto value with `set` if you prefer read-modify-write (document choice)

**Market totals** should support API-like state:

- `total_supply_assets`, `total_supply_shares`
- `total_borrow_assets`, `total_borrow_shares`
- `total_collateral_assets` (sum of collateral)
- `fee` if SetFee is ever added (SF stub does **not** decode SetFee yet — skip or note gap)

## MetaMorpho (new map — not in SF pack)

Ethereum factories:

| Factory | Address |
| --- | --- |
| MetaMorpho Factory V1.1 | `0x1897A8997241C1cD4bD0698647e4EB7213535c24` |
| MetaMorpho Factory [OLD] | `0xA9c3D3a366466Fa809d1Ae982Fb2c46E5fC41101` |

Sources: https://docs.morpho.org/get-started/resources/addresses/  
Repos: https://github.com/morpho-org/metamorpho-v1.1 and https://github.com/morpho-org/metamorpho

**v0.1 scope**

1. Decode factory `CreateMetaMorpho` (or whatever the V1.1 create event is named) → store vault address + asset + name/symbol if in event.
2. For Deposit / Withdraw / Transfer on MetaMorpho vaults:
   - Option A (simpler for v0.1): maintain `store_vaults` from factory creates; in `map_metamorpho_events`, only decode logs whose `address` is in an **in-block** set built from create events in the same block **plus** you cannot see prior store state in a pure map without `store` input. Correct pattern: use a **store of vault addresses**, then a map that takes `store: store_vaults` + block and filters logs to known vaults; **or** index Deposit/Withdraw by matching MetaMorpho/ERC-4626 topic0 and verifying the contract was created by the factory (harder). Recommended:  
     - `map_factory_creates` → `store_vaults`  
     - `map_vault_events` inputs: `source: Block` + `store: store_vaults` (keys = vault addresses) — filter logs to keys present in store (use `store.get_at` / has).
3. `store_vault_positions`: key `{vault}:{user}`, value shares (Transfer/Deposit/Withdraw deltas).

**Out of scope v0.1:** Vault V2 factory/adapters, USD prices, reward APRs, full HF with oracle RPC (document as follow-up; emit lltv + position shares so a consumer can join oracle prices).

## Proto (ours)

Package e.g. `morpho_paulie.v1`:

- `MarketParams` — market_id, loan_token, collateral_token, oracle, irm, lltv, created_block, tx
- `MetaMorphoEvents` — repeated CreateVault, Deposit, Withdraw, Transfer (typed messages, one type per event — Substreams Ethereum skill rule)
- Entity/view messages optional if you map stores → db without intermediate protos

Do **not** re-vend SF `morpho_blue.types.v1` unless build requires it; consume via import.

## SQL schema (primary product)

Upsert tables (not only append-only events):

```sql
CREATE TABLE markets (
  market_id TEXT PRIMARY KEY,
  loan_token TEXT NOT NULL,
  collateral_token TEXT NOT NULL,
  oracle TEXT NOT NULL,
  irm TEXT NOT NULL,
  lltv NUMERIC NOT NULL,
  created_block BIGINT,
  updated_block BIGINT
);

CREATE TABLE market_states (
  market_id TEXT PRIMARY KEY,
  total_supply_assets NUMERIC NOT NULL,
  total_supply_shares NUMERIC NOT NULL,
  total_borrow_assets NUMERIC NOT NULL,
  total_borrow_shares NUMERIC NOT NULL,
  total_collateral_assets NUMERIC NOT NULL,
  updated_block BIGINT
);

CREATE TABLE positions (
  id TEXT PRIMARY KEY, -- market_id:user
  market_id TEXT NOT NULL,
  user_address TEXT NOT NULL,
  supply_shares NUMERIC NOT NULL,
  borrow_shares NUMERIC NOT NULL,
  collateral NUMERIC NOT NULL,
  updated_block BIGINT
);

CREATE TABLE vaults (
  address TEXT PRIMARY KEY,
  asset TEXT,
  name TEXT,
  symbol TEXT,
  factory TEXT,
  created_block BIGINT
);

CREATE TABLE vault_positions (
  id TEXT PRIMARY KEY, -- vault:user
  vault TEXT NOT NULL,
  user_address TEXT NOT NULL,
  shares NUMERIC NOT NULL,
  updated_block BIGINT
);
```

Optional: keep thin raw event tables if you also pass `morpho_sf:map_events` into `db_out`.

## Repo layout

```
substreams.yaml
Cargo.toml
build.rs
buf.gen.yaml
proto/morpho_paulie.proto
abi/MetaMorphoFactory.json
abi/MetaMorpho.json   # or ERC4626 + MetaMorpho specifics
src/lib.rs
src/metamorpho.rs
src/stores.rs
src/db.rs
schema.sql
README.md
.gitignore
docs/CLAUDE_BRIEF.md  # this file
```

## README must say

1. We **import** SF `map_events` (lego) — we do not maintain a Blue decoder fork.
2. What we add: market/position stores, MetaMorpho, upsert `db_out`.
3. Run examples:

```bash
substreams build
substreams run . store_positions -e mainnet.eth.streamingfast.io:443 --start-block 18883124 --stop-block +100
substreams run . db_out -e mainnet.eth.streamingfast.io:443 --start-block -1
```

4. Base is follow-up (same Blue address on many chains, but SF pack is `network: mainnet`).
5. Optional tiny upstream PR to SF: decode missing admin events (`EnableIrm`, `EnableLltv`, `SetFee`, …) — **not** required for this pack.

## Success criteria

- [ ] `substreams build` succeeds with `imports.morpho_sf: morpho-blue-substreams@v0.1.0`
- [ ] At least one store module takes `map: morpho_sf:map_events`
- [ ] MetaMorpho factory + vault share events decoded into typed protos
- [ ] `db_out` upserts markets / market_states / positions / vaults / vault_positions
- [ ] README documents lego architecture
- [ ] Tag/publish path ready for `substreams registry publish` as PaulieB14 (user runs auth)

## Non-goals

- Large PR into `streamingfast/substreams-chain-modules` for stores/MetaMorpho
- Copy-paste of SF `src/lib.rs` map_events as plan A (only if import is broken — then open an issue and temporary vendor with clear TODO to revert to import)
- LlamaGuard / Pendle / other packs in this repo

## Reference code

SF stub (read-only reference for Events shape + db_out style):

https://github.com/streamingfast/substreams-chain-modules/tree/main/lending/morpho-blue-substreams

Paulie’s prior packs for style (stores, SQL): aerodrome-substreams, uniswap-v4-base, uniswap-v4-robinhood under PaulieB14.
