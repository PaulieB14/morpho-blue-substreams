# Morpho Blue accounting (from morpho-org/morpho-blue)

Source of truth: [`morpho-org/morpho-blue`](https://github.com/morpho-org/morpho-blue) — `src/Morpho.sol`, `src/interfaces/IMorpho.sol`, `src/libraries/{EventsLib,SharesMathLib,ConstantsLib,MarketParamsLib,UtilsLib}.sol`.

This doc is what makes the PaulieB14 pack **awesome**: event-sourced stores that match on-chain `Market` / `Position` after every tx.

## Mental model

Singleton Morpho Blue. Each **market** is a `MarketParams` 5-tuple:

`loanToken | collateralToken | oracle | irm | lltv`

**Market id** = `keccak256(abi.encode(marketParams))` over exactly `5 * 32` bytes (`MarketParamsLib.id`).

Two share pools per market (independent):

| Pool | Position field | Market totals |
| --- | --- | --- |
| Supply (loan token) | `supplyShares` (uint256) | `totalSupplyAssets`, `totalSupplyShares` |
| Borrow (loan token) | `borrowShares` (uint128) | `totalBorrowAssets`, `totalBorrowShares` |
| Collateral | `collateral` (uint128) | *not* in Market struct — sum of positions only |

Also on Market: `lastUpdate` (timestamp), `fee` (WAD, max 25% = `MAX_FEE`).

**IMorpho warnings (critical for indexers):**

- `totalSupplyAssets` / `totalBorrowAssets` do **not** include interest since `lastUpdate` until `_accrueInterest` runs.
- `totalSupplyShares` / feeRecipient `supplyShares` do **not** include fee shares since last accrual until accrue runs.
- Every mutating user op calls `_accrueInterest` first, then emits `AccrueInterest` (if elapsed > 0 and irm != 0), then the user event.

So **after processing all events in a block in order**, event-sourced totals match storage. Between touches, on-chain view can be “stale” until the next op accrues — same as the contract.

## Virtual shares (SharesMathLib)

```
VIRTUAL_SHARES = 1e6
VIRTUAL_ASSETS = 1
```

Conversions (OpenZeppelin-style inflation defense):

- `toSharesDown(assets, totalAssets, totalShares) = assets * (totalShares + 1e6) / (totalAssets + 1)`
- `toAssetsUp(shares, ...)` used for **borrow** side when checking health / repay (rounds against user)
- `toAssetsDown` for seize math paths

When emitting API-like `supplyAssets` / `borrowAssets` from shares, use the same rounding as Morpho:

- Display borrow debt ≈ `borrowShares.toAssetsUp(totalBorrowAssets, totalBorrowShares)`
- Display supply assets ≈ `supplyShares.toAssetsDown(totalSupplyAssets, totalSupplyShares)`

## Health / liquidatable (awesome module)

From `_isHealthy` (`Morpho.sol`):

```
borrowed = borrowShares.toAssetsUp(totalBorrowAssets, totalBorrowShares)
maxBorrow = collateral.mulDivDown(oraclePrice, ORACLE_PRICE_SCALE=1e36).wMulDown(lltv)
healthy  <=> maxBorrow >= borrowed
```

Oracle: `IOracle(oracle).price()` — scale **1e36** (quote loan token per 1 collateral token, Morpho convention).

Liquidation incentive (`liquidate`):

```
LIF = min(MAX_LIQUIDATION_INCENTIVE_FACTOR=1.15e18,
          WAD / (WAD - LIQUIDATION_CURSOR=0.3e18 * (WAD - lltv)))
```

**Substreams implication:** positions + market totals + lltv + oracle address are enough for consumers; a live liquidatable feed needs **oracle price** (RPC `eth_call` enrichment or separate price Substream). v0.1 can emit `map_borrower_snapshots` (shares, collateral, totals, lltv, oracle) without price; v0.2 adds price + `health_factor = maxBorrow/borrowed`.

## AccrueInterest — the fee-share landmine

`_accrueInterest` (`Morpho.sol` ~483):

1. `elapsed = now - lastUpdate`; if 0, return (no event).
2. `interest = totalBorrowAssets.wMulDown(borrowRate.wTaylorCompounded(elapsed))`
3. `totalBorrowAssets += interest`
4. `totalSupplyAssets += interest`  (full interest, including fee portion)
5. If `fee != 0`:
   - `feeAmount = interest.wMulDown(fee)`
   - `feeShares = feeAmount.toSharesDown(totalSupplyAssets - feeAmount, totalSupplyShares)`
   - **`position[id][feeRecipient].supplyShares += feeShares`**
   - `totalSupplyShares += feeShares`
6. Emit `AccrueInterest(id, prevBorrowRate, interest, feeShares)`
7. `lastUpdate = now`

**EventsLib warning on Supply:** *feeRecipient receives shares during interest accrual without any Supply event.*

Therefore a correct position store **MUST**:

1. Track global `feeRecipient` from `SetFeeRecipient` (and initial deploy owner config — Morpho deploy sets recipient; index from first `SetFeeRecipient` or known genesis).
2. On every `AccrueInterest` with `feeShares > 0`, add `feeShares` to **`feeRecipient`’s** supplyShares for that `market_id`.

StreamingFast `morpho-blue-substreams@v0.1.0` does **not** decode `SetFeeRecipient` / `SetFee`. For awesome accuracy we either:

- **A (lego+)** keep SF `map_events` and add a thin `map_blue_admin` in our pack for `SetFee`, `SetFeeRecipient`, `EnableIrm`, `EnableLltv`, `SetOwner`, `SetAuthorization`, **or**
- **B** tiny upstream PR to SF stub for those events, then compose.

Recommend **A** for speed (still lego for the heavy event volume).

Also store per-market `fee` from `SetFee` (after accrue in contract) so we can explain feeShares; not required to *apply* AccrueInterest deltas (event already carries `feeShares` + `interest`).

## Event → store delta table (paste into handlers)

Position key: `{market_id}:{user}` lowercase. User = `onBehalf` except Liquidate → `borrower`.

Market totals keys: `{market_id}:total_supply_assets` etc. (or one proto blob with `set`).

| Event | Position deltas | Market totals deltas | Notes |
| --- | --- | --- | --- |
| CreateMarket | — | init zeros; set params; lastUpdate implicit | Persist MarketParams; id from event |
| Supply | onBehalf: +supplyShares | +totalSupplyAssets, +totalSupplyShares | assets+shares from event |
| Withdraw | onBehalf: −supplyShares | −both supply totals | |
| Borrow | onBehalf: +borrowShares | +both borrow totals | |
| Repay | onBehalf: −borrowShares | −borrow assets/shares | assets may be 1 over totalBorrowAssets |
| SupplyCollateral | onBehalf: +collateral | — (optional sum collateral store) | |
| WithdrawCollateral | onBehalf: −collateral | — | |
| Liquidate | borrower: −borrowShares (repaidShares), −collateral (seized); if badDebt: borrowShares→0 | −repaid from borrow totals; if badDebt: −badDebtAssets from **both** totalBorrowAssets and **totalSupplyAssets**, −badDebtShares from totalBorrowShares | Socialized bad debt hits suppliers |
| AccrueInterest | **feeRecipient: +feeShares** (supply) | +interest supply assets, +interest borrow assets, +feeShares supply shares | No Supply event |
| FlashLoan | — | — | No position; optional analytics only |
| SetFee | — | set market.fee | Does not change totals (accrue already ran) |
| SetFeeRecipient | — | update global feeRecipient pointer | Pending unaccrued fees go to new recipient (per IMorpho docs) |
| SetAuthorization | optional auth graph | — | Not needed for position balances |
| EnableIrm / EnableLltv / SetOwner | registry / admin | — | Nice for completeness |

## Bad debt (Liquidate)

If after seize `collateral == 0` and borrowShares remain:

- `badDebtShares = remaining borrowShares`
- `badDebtAssets = min(totalBorrowAssets, badDebtShares.toAssetsUp(...))`
- Subtract badDebtAssets from **totalBorrowAssets and totalSupplyAssets**
- Zero borrower borrowShares; subtract badDebtShares from totalBorrowShares

Event fields `bad_debt_assets` / `bad_debt_shares` are authoritative for the indexer — apply them; don’t re-derive unless verifying.

## Can we rebuild exact totals from events alone?

**Yes**, for post-tx state, if we:

1. Apply all SF-decoded user/market events in log order.
2. Apply AccrueInterest interest + feeShares (including silent feeRecipient supply).
3. Track feeRecipient via SetFeeRecipient (our admin map).
4. Apply Liquidate bad debt to supply side.

**Caveats:**

- Genesis `feeRecipient` before first SetFeeRecipient: read from chain once or set known Morpho deployment config in README.
- Mid-block ordering: process logs in `(tx_index, log_index)` order (Substreams block txs already ordered).
- View-at-timestamp without a later accrue: same staleness as contract until next touch — document it.

## Authorization

`SetAuthorization` / `IncrementNonce` do **not** change balances. Only who may call withdraw/borrow/repay/withdrawCollateral on behalf of `onBehalf`. Skip for v0.1 balances; optional `store_auth` for wallets UX.

## Flash loans

Free flash loans; emit only; no market/position mutation (liquidity is transient).

## MetaMorpho relationship

MetaMorpho vaults are ERC-4626 wrappers that hold a **Blue supply (+ sometimes collateral strategies)** as `onBehalf = vault`. In Blue position stores, vault addresses appear as large suppliers. Indexing MetaMorpho Deposit/Withdraw gives **end-user** vault shares; Blue `store_positions` still shows vault→market. Both layers needed for Morpho API parity (`marketPositions` vs `vaultPositions`).

## What SF stub misses (priority)

| Missing event | Need for awesome pack? |
| --- | --- |
| SetFeeRecipient | **YES — critical** for AccrueInterest fee shares |
| SetFee | YES — market fee state |
| EnableIrm / EnableLltv | Nice (market creation allowlist) |
| SetOwner | Nice |
| SetAuthorization / IncrementNonce | Optional |

## Constants cheat sheet

| Name | Value |
| --- | --- |
| ORACLE_PRICE_SCALE | 1e36 |
| MAX_FEE | 0.25e18 |
| LIQUIDATION_CURSOR | 0.3e18 |
| MAX_LIQUIDATION_INCENTIVE_FACTOR | 1.15e18 |
| VIRTUAL_SHARES | 1e6 |
| VIRTUAL_ASSETS | 1 |
| WAD | 1e18 |

## Suggested module add-on (still lego)

```
morpho_sf:map_events  ──► store_market_params / store_market_totals / store_positions
map_blue_admin (ours) ──► store_fee_recipient, store_market_fee, (optional admin)
AccrueInterest handler must read store_fee_recipient at ordinal
```

Wire `store_positions` inputs: `morpho_sf:map_events` + `store: store_fee_recipient` (or pass fee recipient via a map that joins admin + events first: `map_blue_enriched`).

Cleanest pattern:

1. `map_blue_admin` → `store_fee_recipient` (set)
2. `map_position_deltas` inputs: `morpho_sf:map_events` + `store: store_fee_recipient` → emits typed deltas including synthetic fee supply
3. `store_positions` / `store_market_totals` consume those deltas

That keeps SF as the Blue **tape** lego and our code as the **accounting brain**.
