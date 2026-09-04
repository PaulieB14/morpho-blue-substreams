# Morpho Blue indexer brief (for Substreams pack on SF `morpho-blue-substreams@v0.1.0`)

Sources: `src/Morpho.sol`, `src/interfaces/IMorpho.sol`, `src/libraries/{EventsLib,SharesMathLib,ConstantsLib,MarketParamsLib,MathLib,UtilsLib}.sol`, `src/libraries/periphery/MorphoBalancesLib.sol`. MetaMorpho: `morpho-org/metamorpho` `src/MetaMorpho.sol` (`MORPHO.supply(..., address(this), ...)`).

---

## 1. Core accounting model

**Shares design** (`SharesMathLib.sol`): OZ-style virtual shares.
- `VIRTUAL_SHARES = 1e6`, `VIRTUAL_ASSETS = 1`
- `toSharesDown(a,TA,TS) = a * (TS+VS) / (TA+VA)` (down)
- `toAssetsDown(s,TA,TS) = s * (TA+VA) / (TS+VS)` (down)
- Up variants use `mulDivUp`
- Empty market rate ≈ `VS/VA = 1e6` shares per asset

**Rounding used on-chain** (`Morpho.sol`):
| Op | Given assets | Given shares |
|----|--------------|--------------|
| supply | shares=toSharesDown | assets=toAssetsUp |
| withdraw | shares=toSharesUp | assets=toAssetsDown |
| borrow | shares=toSharesUp | assets=toAssetsDown |
| repay | shares=toSharesDown | assets=toAssetsUp |

**Market** (`IMorpho.sol`): `totalSupplyAssets`, `totalSupplyShares`, `totalBorrowAssets`, `totalBorrowShares` (all uint128), `lastUpdate`, `fee` (WAD, ≤ `MAX_FEE=0.25e18`). Warnings: storage totals omit unaccrued interest; `totalSupplyShares` omits unaccrued fee shares.

**Position**: `supplyShares` (uint256), `borrowShares` (uint128), `collateral` (uint128). Collateral is raw token amount (no shares). Loan side is share-based.

**Liquidity invariant**: `totalBorrowAssets ≤ totalSupplyAssets` (enforced on borrow/withdraw).

---

## 2. Event → store deltas

Position key user = **`onBehalf`** (Supply/Withdraw/Borrow/Repay/SupplyCollateral/WithdrawCollateral) or **`borrower`** (Liquidate). Caller/receiver are not position keys.

| Event | Position Δ | Market totals Δ | Notes |
|-------|------------|-----------------|-------|
| **CreateMarket** | — | all totals 0; `lastUpdate=block.timestamp`; store `MarketParams` | `id = keccak256(marketParams)` |
| **AccrueInterest** | `feeRecipient.supplyShares += feeShares` | `TBA+=interest`, `TSA+=interest`, `TSS+=feeShares`; `lastUpdate=ts` | No `feeRecipient` in event — track via `SetFeeRecipient`. If `fee=0` or `interest=0`, `feeShares=0`. |
| **Supply** | `onBehalf.supplyShares += shares` | `TSS+=shares`, `TSA+=assets` | Preceded by AccrueInterest if elapsed>0 |
| **Withdraw** | `onBehalf.supplyShares -= shares` | `TSS-=shares`, `TSA-=assets` | Auth required on-chain |
| **Borrow** | `onBehalf.borrowShares += shares` | `TBS+=shares`, `TBA+=assets` | |
| **Repay** | `onBehalf.borrowShares -= shares` | `TBS-=shares`, `TBA = zeroFloorSub(TBA, assets)` | `assets` may be **TBA+1**; clamp |
| **SupplyCollateral** | `onBehalf.collateral += assets` | — | **No interest accrual** |
| **WithdrawCollateral** | `onBehalf.collateral -= assets` | — | Accrues first |
| **Liquidate** | see below | see below | Accrues first |
| **FlashLoan** | — | — | No share/market Δ; liquidity only |
| **SetFee** | — | `fee=newFee` (after accrue) | |
| **SetFeeRecipient** | — | — | Updates global feeRecipient for next AccrueInterest |
| **SetAuthorization** / **IncrementNonce** | — | — | Ops only; optional |
| **EnableIrm/EnableLltv/SetOwner** | — | — | Admin metadata |

**Liquidate** (`Morpho.sol` ~347–417):
1. `borrower.borrowShares -= repaidShares`; `TBS -= repaidShares`; `TBA = zeroFloorSub(TBA, repaidAssets)`
2. `borrower.collateral -= seizedAssets` (no market collateral total — collateral is per-position only)
3. **Bad debt** iff `collateral == 0` after seize:
   - `badDebtShares = remaining borrowShares`
   - `badDebtAssets = min(TBA, toAssetsUp(badDebtShares, TBA, TBS))`
   - `TBA -= badDebtAssets`; **`TSA -= badDebtAssets`** (socialized to suppliers); `TBS -= badDebtShares`; `borrower.borrowShares = 0`
4. Event fields are exact deltas to apply (including bad debt).

**AccrueInterest exact effects** (`_accrueInterest`):
```
interest = TBA.wMulDown(borrowRate.wTaylorCompounded(elapsed))
TBA += interest; TSA += interest
if fee != 0:
  feeAmount = interest.wMulDown(fee)
  feeShares = toSharesDown(feeAmount, TSA - feeAmount, TSS)
  position[feeRecipient].supplyShares += feeShares
  TSS += feeShares
emit AccrueInterest(id, borrowRate, interest, feeShares)
lastUpdate = now
```
Emitted only when `elapsed > 0` **and** `irm != address(0)`. If `elapsed>0` but `irm==0`, `lastUpdate` still advances with **no event**.

---

## 3. Interest accrual & event-only rebuild

**When**: Any of supply/withdraw/borrow/repay/withdrawCollateral/liquidate/setFee/accrueInterest with `elapsed = now - lastUpdate > 0`. Not on supplyCollateral or flashLoan.

**Rebuild stored totals from events alone?** **Yes**, with caveats:
- Apply AccrueInterest `interest`/`feeShares` then action deltas in **global log order** (`log_index`), not by event-type buckets.
- Track `feeRecipient` timeline (`SetFeeRecipient` + constructor owner path; fees mint to whatever recipient is at accrual).
- `CreateMarket` seeds zeros + params; `SetFee` updates `fee`.
- **Caveat**: between events, on-chain *expected* balances include pending interest (`MorphoBalancesLib.expectedMarketBalances`) — **not** reconstructible without IRM `borrowRateView` + elapsed time (needs IRM bytecode/RPC or indexing IRM). Event-synced stores match **storage** after last accrual, not “live expected.”
- Repay/Liquidate: always `zeroFloorSub` for TBA when applying `assets`/`repaidAssets`.

---

## 4. Liquidation / HF (liquidatable feed)

Healthy iff (`Morpho.sol` `_isHealthy`):
```
borrowed = borrowShares.toAssetsUp(TBA, TBS)
maxBorrow = collateral.mulDivDown(oraclePrice, ORACLE_PRICE_SCALE).wMulDown(lltv)
healthy ⇔ maxBorrow >= borrowed   (borrowShares==0 ⇒ healthy)
```
- `ORACLE_PRICE_SCALE = 1e36` (`ConstantsLib.sol`)
- **On-chain state needed**: `collateral`, `borrowShares`, market `TBA`/`TBS` (accrued), `lltv` from MarketParams
- **Off-chain / oracle**: `IOracle.price()` — **not** in Morpho events; must read oracle each block (or index oracle) for HF feed
- LIF (seize math): `min(1.15e18, WAD / (WAD - 0.3e18 * (WAD - lltv)))` — for simulating max seize, not for HF itself

---

## 5. Authorization

`isAuthorized[authorizer][authorized]`; `_isSenderAuthorized`: `msg.sender == onBehalf || isAuthorized[onBehalf][msg.sender]`. Gates withdraw/borrow/withdrawCollateral. **Does not change position accounting.** Optional store for UX; **not required** for position/market stores. Supply/repay/supplyCollateral/liquidate need no auth on `onBehalf`/`borrower`.

---

## 6. Market id

`MarketParamsLib.id`: `keccak256` over 160 bytes (`5 * 32`) of `(loanToken, collateralToken, oracle, irm, lltv)` in memory layout order. `CreateMarket` emits both `id` and full `marketParams`; also written to `idToMarketParams`.

---

## 7. Indexer gotchas

1. **SF `map_events` splits types into separate repeated fields** → consumers that iterate per-array lose AccrueInterest→Supply order. **Always sort by `(block, log_index)`** before applying stores.
2. **Flash loans**: no accounting Δ; can temporarily drain ERC20 balance; ignore for shares.
3. **Bad debt**: reduces **both** TBA and TSA; suppliers take the loss via share price.
4. **Repay/Liquidate +1 dust**: `assets` may exceed TBA by 1; use `zeroFloorSub`.
5. **Zero shares**: `exactlyOneZero(assets,shares)` — events always have both filled after conversion; empty market protected by virtual shares (donation/inflation mitigated; comment in SharesMathLib).
6. **feeRecipient supplyShares** grow **only** via AccrueInterest (no Supply event) — EventsLib warns this.
7. **SupplyCollateral skips accrue** — HF uses possibly stale TBA until next accrue; liquidate accrues first.
8. **Callbacks** (supply/repay/liquidate/flash) can reenter Morpho in same tx — multiple events; order by log_index.
9. Tiny borrow markets (`TBA < ~1e4`): share price manipulable (IMorpho docs) — edge case.
10. Tokens with fee-on-transfer / rebasing unsupported by design.

---

## 8. Recommended store keys + delta table (paste-ready)

```
# Keys
market:{id}                    → MarketParams + fee + lastUpdate
market_totals:{id}             → TSA,TSS,TBA,TBS   (uint deltas)
position:{id}:{user}           → supplyShares, borrowShares, collateral
fee_recipient                  → address (singleton)
auth:{authorizer}:{authorized} → bool (optional)
# Optional indexes
borrowers:{id}                 → set of users with borrowShares>0 (liquidatable scan)
vault_as_user:{vault}          → flag MetaMorpho (user = vault address)
```

```
# Delta table (apply after sorting logs)
CreateMarket(id,params):       market[id]=params; totals=0; lastUpdate=ts
SetFee(id,fee):                market[id].fee=fee
SetFeeRecipient(addr):         fee_recipient=addr
AccrueInterest(id,i,fs):       TBA+=i; TSA+=i; TSS+=fs;
                               position[id][fee_recipient].supplyShares+=fs
                               lastUpdate=ts
Supply(id,onBehalf,a,s):       pos.supplyShares+=s; TSS+=s; TSA+=a
Withdraw(id,onBehalf,a,s):     pos.supplyShares-=s; TSS-=s; TSA-=a
Borrow(id,onBehalf,a,s):       pos.borrowShares+=s; TBS+=s; TBA+=a
Repay(id,onBehalf,a,s):        pos.borrowShares-=s; TBS-=s; TBA=max(0,TBA-a)
SupplyCollateral(id,ob,a):     pos.collateral+=a
WithdrawCollateral(id,ob,a):   pos.collateral-=a
Liquidate(id,bor,ra,rs,sa,bda,bds):
  pos.borrowShares-=rs; TBS-=rs; TBA=max(0,TBA-ra)
  pos.collateral-=sa
  TBA-=bda; TSA-=bda; TBS-=bds; pos.borrowShares-=bds  # bds clears remainder (=0)
FlashLoan:                     no-op for stores
```

Asset↔share conversion for displays: use virtual shares formulas against stored totals (same as chain).

---

## 9. What SF stub (`morpho-blue-substreams@v0.1.0`) misses

Package maps CreateMarket, Supply, SupplyCollateral, Borrow, Repay, Withdraw, WithdrawCollateral, Liquidate, AccrueInterest, FlashLoan → raw event tables only (`schema.sql`). ABI includes but **map does not emit**: `SetFee`, `SetFeeRecipient`, `SetAuthorization`, `IncrementNonce`, `EnableIrm`, `EnableLltv`, `SetOwner`.

**Missing for an awesome pack:**
- **Stores** for market totals + positions (stub is event log dump)
- **`SetFee` / `SetFeeRecipient`** (required to attribute `feeShares` and know `fee`)
- **Cross-type ordered store module** (fix AccrueInterest ordering bug)
- **feeRecipient position** updates on AccrueInterest
- **Borrower index / HF module** (needs oracle prices — out of Blue ABI)
- **MetaMorpho**: vault factories/events; Blue `onBehalf = vault`; allocations
- **lastUpdate / fee** on market entity
- Base/other deployments (stub hardcodes mainnet `0xBBBB…FFCb`, block `18883124`)

---

## MetaMorpho (brief)

MetaMorpho ERC4626 vault holds **one Blue supply (+ optionally collateral) position per allocated market** with `onBehalf = address(vault)`. Index Blue `position[id][vault]` as the vault’s market exposure; vault share accounting is separate (MetaMorpho events). Do not treat depositors as Blue users.
