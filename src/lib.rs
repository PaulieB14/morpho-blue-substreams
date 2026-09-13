//! Morpho Blue + MetaMorpho Substreams (PaulieB14).
//!
//! Composition, not a fork: StreamingFast's `morpho-blue-substreams` already
//! decodes the Blue event tape, so we import its `map_events` and spend our
//! modules on what it does not have — market params, market totals, per-user
//! positions, MetaMorpho vaults, and upsert SQL.
//!
//! Accounting follows `morpho-org/morpho-blue`; see docs/MORPHO_BLUE_ACCOUNTING.md.
//! Two rules drive most of the code here:
//!
//!   1. `AccrueInterest` mints `feeShares` directly into the fee recipient's
//!      supply position and emits no `Supply` event. Miss it and every fee
//!      recipient's balance is wrong forever.
//!   2. Bad debt on `Liquidate` is socialized: it comes off `totalBorrowAssets`
//!      *and* `totalSupplyAssets`, and zeroes the borrower's remaining shares.

mod abi;
mod pb;

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use substreams::errors::Error;
use substreams::pb::substreams::store_delta::Operation as DeltaOperation;
use substreams::pb::substreams::Clock;
use substreams::scalar::BigInt;
use substreams::store::{
    DeltaBigInt, DeltaProto, DeltaString, Deltas, StoreAdd, StoreAddBigInt, StoreDelete, StoreGet,
    StoreGetBigInt, StoreGetProto, StoreGetString, StoreNew, StoreSet, StoreSetProto,
    StoreSetString,
};
use substreams_database_change::pb::sf::substreams::sink::database::v1::DatabaseChanges;
use substreams_database_change::tables::Tables;
use substreams_ethereum::pb::eth::v2::Block;
use substreams_ethereum::rpc::RpcBatch;
use substreams_ethereum::Event;

use crate::pb::morpho_blue::types::v1 as sf;
use crate::pb::morpho_paulie::v1 as mp;

/// MorphoBlue singleton, Ethereum mainnet (deployed block 18883124).
const MORPHO_BLUE: [u8; 20] = hex_literal::hex!("BBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb");
/// MetaMorpho factories across supported networks.
/// See https://docs.morpho.org/developers/contracts/
///
/// This crate is compiled once and wired by two manifests (Ethereum and Base),
/// so every known factory is checked on both chains. That is safe: an address
/// that is not a factory on a given chain simply never emits CreateMetaMorpho.
/// `0xA9c3D3a3…` is deployed at the same address on both.
const MM_FACTORIES: [[u8; 20]; 3] = [
    hex_literal::hex!("1897A8997241C1cD4bD0698647e4EB7213535c24"), // Ethereum V1.1
    hex_literal::hex!("A9c3D3a366466Fa809d1Ae982Fb2c46E5fC41101"), // Ethereum + Base V1
    hex_literal::hex!("Ff62A7c278C62eD665133147129245053Bbf5918"), // Base V1.1
];

/// Single global key: Blue has one fee recipient across all markets.
const FEE_RECIPIENT_KEY: &str = "fee_recipient";

// SharesMathLib / ConstantsLib, from morpho-org/morpho-blue.
const VIRTUAL_SHARES: u64 = 1_000_000;
const VIRTUAL_ASSETS: u64 = 1;
/// IOracle.price() is quoted at 1e36, not 1e18.
const ORACLE_PRICE_SCALE_EXP: u32 = 36;
const WAD_EXP: u32 = 18;
/// LIQUIDATION_CURSOR = 0.3e18
const LIQUIDATION_CURSOR: u64 = 300_000_000_000_000_000;
/// MAX_LIQUIDATION_INCENTIVE_FACTOR = 1.15e18
const MAX_LIF: &str = "1150000000000000000";

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

fn block_timestamp(block: &Block) -> u64 {
    block
        .header
        .as_ref()
        .and_then(|h| h.timestamp.as_ref().map(|t| t.seconds as u64))
        .unwrap_or(0)
}

fn fmt_addr(addr: &[u8]) -> String {
    format!("0x{}", hex::encode(addr))
}

fn fmt_bytes32(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

/// Parse a decimal string from the event tape. These come from on-chain uint256
/// values, so a parse failure means upstream corruption — zero is the safe
/// delta (no-op) rather than a panic that would stall the whole stream.
fn bi(s: &str) -> BigInt {
    BigInt::from_str(s).unwrap_or_else(|_| BigInt::zero())
}

fn pos_key(market_id: &str, user: &str, field: &str) -> String {
    format!("pos:{}:{}:{}", market_id, user.to_lowercase(), field)
}

fn mkt_key(market_id: &str, field: &str) -> String {
    format!("mkt:{}:{}", market_id, field)
}

fn vault_pos_key(vault: &str, user: &str) -> String {
    format!("vpos:{}:{}", vault.to_lowercase(), user.to_lowercase())
}

/// One Blue event, tagged with its log index so the whole block can be replayed
/// in true on-chain order.
///
/// This matters more than it looks: StreamingFast's `Events` message buckets by
/// event *type*, so iterating field-by-field can apply an `AccrueInterest` after
/// a `Supply` that actually happened later in the same transaction. Interest
/// accrual always runs first on-chain, so the naive order silently corrupts
/// share math.
enum Ev<'a> {
    Supply(&'a sf::Supply),
    Withdraw(&'a sf::Withdraw),
    Borrow(&'a sf::Borrow),
    Repay(&'a sf::Repay),
    SupplyCollateral(&'a sf::SupplyCollateral),
    WithdrawCollateral(&'a sf::WithdrawCollateral),
    Liquidate(&'a sf::Liquidate),
    Accrue(&'a sf::AccrueInterest),
}

fn ordered(events: &sf::Events) -> Vec<(u64, Ev<'_>)> {
    let mut out: Vec<(u64, Ev)> = Vec::new();
    out.extend(events.supplies.iter().map(|e| (e.log_index, Ev::Supply(e))));
    out.extend(events.withdraws.iter().map(|e| (e.log_index, Ev::Withdraw(e))));
    out.extend(events.borrows.iter().map(|e| (e.log_index, Ev::Borrow(e))));
    out.extend(events.repays.iter().map(|e| (e.log_index, Ev::Repay(e))));
    out.extend(
        events
            .supply_collaterals
            .iter()
            .map(|e| (e.log_index, Ev::SupplyCollateral(e))),
    );
    out.extend(
        events
            .withdraw_collaterals
            .iter()
            .map(|e| (e.log_index, Ev::WithdrawCollateral(e))),
    );
    out.extend(events.liquidates.iter().map(|e| (e.log_index, Ev::Liquidate(e))));
    out.extend(
        events
            .accrued_interests
            .iter()
            .map(|e| (e.log_index, Ev::Accrue(e))),
    );
    out.sort_by_key(|(idx, _)| *idx);
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Blue admin events (SF's map_events does not decode these)
// ─────────────────────────────────────────────────────────────────────────────

#[substreams::handlers::map]
pub fn map_blue_admin(block: Block) -> Result<mp::BlueAdminEvents, Error> {
    let mut out = mp::BlueAdminEvents::default();
    let timestamp = block_timestamp(&block);

    for trx in block.transactions() {
        let tx_hash = format!("0x{}", hex::encode(&trx.hash));

        for (log, _call) in trx.logs_with_calls() {
            if log.address != MORPHO_BLUE {
                continue;
            }
            let log_index = log.index as u64;

            if let Some(ev) = abi::morpho_blue_admin::events::SetFeeRecipient::match_and_decode(log)
            {
                out.fee_recipients.push(mp::SetFeeRecipient {
                    new_fee_recipient: fmt_addr(&ev.new_fee_recipient),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
                continue;
            }
            if let Some(ev) = abi::morpho_blue_admin::events::SetFee::match_and_decode(log) {
                out.fees.push(mp::SetFee {
                    market_id: fmt_bytes32(&ev.id),
                    new_fee: ev.new_fee.to_string(),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
                continue;
            }
            if let Some(ev) = abi::morpho_blue_admin::events::EnableIrm::match_and_decode(log) {
                out.enabled_irms.push(mp::EnableIrm {
                    irm: fmt_addr(&ev.irm),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
                continue;
            }
            if let Some(ev) = abi::morpho_blue_admin::events::EnableLltv::match_and_decode(log) {
                out.enabled_lltvs.push(mp::EnableLltv {
                    lltv: ev.lltv.to_string(),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
                continue;
            }
            if let Some(ev) = abi::morpho_blue_admin::events::SetOwner::match_and_decode(log) {
                out.owners.push(mp::SetOwner {
                    new_owner: fmt_addr(&ev.new_owner),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::store]
pub fn store_fee_recipient(events: mp::BlueAdminEvents, store: StoreSetString) {
    for ev in events.fee_recipients {
        // Ordinal = log index so `get_at` below resolves the recipient that was
        // actually in effect at the ordinal of a given AccrueInterest.
        store.set(ev.log_index, FEE_RECIPIENT_KEY, &ev.new_fee_recipient);
    }
}

/// Protocol-level Blue config: the owner, plus the enabled IRM and LLTV sets.
///
/// Without this, `map_blue_admin` would decode `SetOwner` / `EnableIrm` /
/// `EnableLltv` and drop them on the floor. Enabled IRMs and LLTVs are
/// append-only in Morpho (there is no disable), so `set` is sufficient.
#[substreams::handlers::store]
pub fn store_blue_config(events: mp::BlueAdminEvents, store: StoreSetString) {
    for ev in events.owners {
        store.set(ev.log_index, "owner", &ev.new_owner);
    }
    for ev in events.enabled_irms {
        store.set(ev.log_index, format!("irm:{}", ev.irm), &"enabled".to_string());
    }
    for ev in events.enabled_lltvs {
        store.set(ev.log_index, format!("lltv:{}", ev.lltv), &"enabled".to_string());
    }
}

#[substreams::handlers::store]
pub fn store_market_fee(events: mp::BlueAdminEvents, store: StoreSetString) {
    for ev in events.fees {
        store.set(ev.log_index, format!("fee:{}", ev.market_id), &ev.new_fee);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Markets, totals, positions
// ─────────────────────────────────────────────────────────────────────────────

#[substreams::handlers::store]
pub fn store_market_params(events: sf::Events, store: StoreSetProto<mp::MarketParams>) {
    for m in events.markets_created {
        store.set(
            m.log_index,
            mkt_key(&m.market_id, "params"),
            &mp::MarketParams {
                market_id: m.market_id.clone(),
                loan_token: m.loan_token,
                collateral_token: m.collateral_token,
                oracle: m.oracle,
                irm: m.irm,
                lltv: m.lltv,
                created_block: m.block_num,
                created_tx: m.tx_hash,
                created_timestamp: m.timestamp,
            },
        );
    }
}

#[substreams::handlers::store]
pub fn store_market_totals(events: sf::Events, store: StoreAddBigInt) {
    for (ord, ev) in ordered(&events) {
        match ev {
            Ev::Supply(e) => {
                store.add(ord, mkt_key(&e.market_id, "total_supply_assets"), bi(&e.assets));
                store.add(ord, mkt_key(&e.market_id, "total_supply_shares"), bi(&e.shares));
            }
            Ev::Withdraw(e) => {
                store.add(ord, mkt_key(&e.market_id, "total_supply_assets"), -bi(&e.assets));
                store.add(ord, mkt_key(&e.market_id, "total_supply_shares"), -bi(&e.shares));
            }
            Ev::Borrow(e) => {
                store.add(ord, mkt_key(&e.market_id, "total_borrow_assets"), bi(&e.assets));
                store.add(ord, mkt_key(&e.market_id, "total_borrow_shares"), bi(&e.shares));
            }
            Ev::Repay(e) => {
                store.add(ord, mkt_key(&e.market_id, "total_borrow_assets"), -bi(&e.assets));
                store.add(ord, mkt_key(&e.market_id, "total_borrow_shares"), -bi(&e.shares));
            }
            Ev::SupplyCollateral(e) => {
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_collateral_assets"),
                    bi(&e.assets),
                );
            }
            Ev::WithdrawCollateral(e) => {
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_collateral_assets"),
                    -bi(&e.assets),
                );
            }
            Ev::Liquidate(e) => {
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_borrow_assets"),
                    -bi(&e.repaid_assets),
                );
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_borrow_shares"),
                    -bi(&e.repaid_shares),
                );
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_collateral_assets"),
                    -bi(&e.seized_assets),
                );

                // Socialized bad debt: suppliers eat it, so it leaves BOTH sides.
                let bad_assets = bi(&e.bad_debt_assets);
                if bad_assets != BigInt::zero() {
                    store.add(
                        ord,
                        mkt_key(&e.market_id, "total_borrow_assets"),
                        -bad_assets.clone(),
                    );
                    store.add(ord, mkt_key(&e.market_id, "total_supply_assets"), -bad_assets);
                }
                let bad_shares = bi(&e.bad_debt_shares);
                if bad_shares != BigInt::zero() {
                    store.add(ord, mkt_key(&e.market_id, "total_borrow_shares"), -bad_shares);
                }
            }
            Ev::Accrue(e) => {
                // Interest inflates both sides; fee shares dilute suppliers.
                let interest = bi(&e.interest);
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_borrow_assets"),
                    interest.clone(),
                );
                store.add(ord, mkt_key(&e.market_id, "total_supply_assets"), interest);
                store.add(
                    ord,
                    mkt_key(&e.market_id, "total_supply_shares"),
                    bi(&e.fee_shares),
                );
            }
        }
    }
}

#[substreams::handlers::store]
pub fn store_positions(
    events: sf::Events,
    fee_recipient: StoreGetString,
    store: StoreAddBigInt,
) {
    for (ord, ev) in ordered(&events) {
        match ev {
            Ev::Supply(e) => store.add(
                ord,
                pos_key(&e.market_id, &e.on_behalf, "supply_shares"),
                bi(&e.shares),
            ),
            Ev::Withdraw(e) => store.add(
                ord,
                pos_key(&e.market_id, &e.on_behalf, "supply_shares"),
                -bi(&e.shares),
            ),
            Ev::Borrow(e) => store.add(
                ord,
                pos_key(&e.market_id, &e.on_behalf, "borrow_shares"),
                bi(&e.shares),
            ),
            Ev::Repay(e) => store.add(
                ord,
                pos_key(&e.market_id, &e.on_behalf, "borrow_shares"),
                -bi(&e.shares),
            ),
            Ev::SupplyCollateral(e) => store.add(
                ord,
                pos_key(&e.market_id, &e.on_behalf, "collateral"),
                bi(&e.assets),
            ),
            Ev::WithdrawCollateral(e) => store.add(
                ord,
                pos_key(&e.market_id, &e.on_behalf, "collateral"),
                -bi(&e.assets),
            ),
            Ev::Liquidate(e) => {
                // The borrower loses the seized collateral and the repaid debt;
                // any bad debt zeroes what is left of their borrow shares.
                store.add(
                    ord,
                    pos_key(&e.market_id, &e.borrower, "collateral"),
                    -bi(&e.seized_assets),
                );
                let shares_gone = bi(&e.repaid_shares) + bi(&e.bad_debt_shares);
                store.add(
                    ord,
                    pos_key(&e.market_id, &e.borrower, "borrow_shares"),
                    -shares_gone,
                );
            }
            Ev::Accrue(e) => {
                // The silent mint. No Supply event accompanies this.
                let fee_shares = bi(&e.fee_shares);
                if fee_shares == BigInt::zero() {
                    continue;
                }
                // Read the recipient as of this event's ordinal, not end-of-block:
                // a SetFeeRecipient later in the same block must not be applied
                // retroactively to interest that accrued before it.
                if let Some(recipient) = fee_recipient.get_at(ord, FEE_RECIPIENT_KEY) {
                    store.add(
                        ord,
                        pos_key(&e.market_id, &recipient, "supply_shares"),
                        fee_shares,
                    );
                }
                // No recipient set yet => fee is 0 by construction, nothing to credit.
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MetaMorpho
// ─────────────────────────────────────────────────────────────────────────────

#[substreams::handlers::map]
pub fn map_metamorpho_factory(block: Block) -> Result<mp::MetaMorphoEvents, Error> {
    let mut out = mp::MetaMorphoEvents::default();
    let timestamp = block_timestamp(&block);

    for trx in block.transactions() {
        let tx_hash = format!("0x{}", hex::encode(&trx.hash));

        for (log, _call) in trx.logs_with_calls() {
            if !MM_FACTORIES.iter().any(|f| log.address == *f) {
                continue;
            }
            if let Some(ev) =
                abi::metamorpho_factory::events::CreateMetaMorpho::match_and_decode(log)
            {
                out.vaults_created.push(mp::VaultCreated {
                    id: format!("{}-{}", tx_hash, log.index),
                    vault: fmt_addr(&ev.meta_morpho),
                    asset: fmt_addr(&ev.asset),
                    name: ev.name,
                    symbol: ev.symbol,
                    factory: fmt_addr(&log.address),
                    tx_hash: tx_hash.clone(),
                    log_index: log.index as u64,
                    block_num: block.number,
                    timestamp,
                    initial_owner: fmt_addr(&ev.initial_owner),
                    initial_timelock: ev.initial_timelock.to_string(),
                });
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::store]
pub fn store_vaults(events: mp::MetaMorphoEvents, store: StoreSetProto<mp::VaultMeta>) {
    for v in events.vaults_created {
        store.set(
            v.log_index,
            format!("vault:{}", v.vault.to_lowercase()),
            &mp::VaultMeta {
                vault: v.vault.clone(),
                asset: v.asset,
                name: v.name,
                symbol: v.symbol,
                factory: v.factory,
                initial_owner: v.initial_owner,
                initial_timelock: v.initial_timelock,
                created_block: v.block_num,
                created_tx: v.tx_hash,
                created_timestamp: v.timestamp,
            },
        );
    }
}

/// Vault share movements, restricted to addresses the factory actually created.
///
/// `Deposit`/`Withdraw`/`Transfer` are generic ERC-4626/ERC-20 signatures — matching
/// them by topic alone across all of mainnet would pull in every vault and token
/// on the chain. The `store_vaults` lookup is what makes them Morpho-specific.
#[substreams::handlers::map]
pub fn map_metamorpho_events(
    block: Block,
    vaults: StoreGetProto<mp::VaultMeta>,
    created_this_block: mp::MetaMorphoEvents,
) -> Result<mp::MetaMorphoEvents, Error> {
    let mut out = mp::MetaMorphoEvents::default();
    let timestamp = block_timestamp(&block);

    // A vault can be created and deposited into in the same block, before the
    // store commits, so union the store with this block's creations.
    let fresh: HashMap<String, ()> = created_this_block
        .vaults_created
        .iter()
        .map(|v| (v.vault.to_lowercase(), ()))
        .collect();
    out.vaults_created = created_this_block.vaults_created;

    for trx in block.transactions() {
        let tx_hash = format!("0x{}", hex::encode(&trx.hash));

        for (log, _call) in trx.logs_with_calls() {
            let addr = fmt_addr(&log.address);
            let key = addr.to_lowercase();
            let known = fresh.contains_key(&key)
                || vaults.get_last(format!("vault:{}", key)).is_some();
            if !known {
                continue;
            }

            let id = format!("{}-{}", tx_hash, log.index);
            let log_index = log.index as u64;

            if let Some(ev) = abi::metamorpho::events::Deposit::match_and_decode(log) {
                out.deposits.push(mp::VaultDeposit {
                    id,
                    vault: addr.clone(),
                    sender: fmt_addr(&ev.sender),
                    owner: fmt_addr(&ev.owner),
                    assets: ev.assets.to_string(),
                    shares: ev.shares.to_string(),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
                continue;
            }
            if let Some(ev) = abi::metamorpho::events::Withdraw::match_and_decode(log) {
                out.withdraws.push(mp::VaultWithdraw {
                    id,
                    vault: addr.clone(),
                    sender: fmt_addr(&ev.sender),
                    receiver: fmt_addr(&ev.receiver),
                    owner: fmt_addr(&ev.owner),
                    assets: ev.assets.to_string(),
                    shares: ev.shares.to_string(),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
                continue;
            }
            if let Some(ev) = abi::metamorpho::events::Transfer::match_and_decode(log) {
                out.transfers.push(mp::VaultTransfer {
                    id,
                    vault: addr.clone(),
                    from_address: fmt_addr(&ev.from),
                    to_address: fmt_addr(&ev.to),
                    shares: ev.value.to_string(),
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_num: block.number,
                    timestamp,
                });
            }
        }
    }

    Ok(out)
}

#[substreams::handlers::store]
pub fn store_vault_positions(events: mp::MetaMorphoEvents, store: StoreAddBigInt) {
    // ERC-4626 mints and burns are reported as Transfers from/to the zero
    // address as well as Deposit/Withdraw events. Only Transfer is applied here
    // so shares are not double counted; the zero address is skipped so it does
    // not accumulate a phantom negative balance.
    let zero = format!("0x{}", "0".repeat(40));

    for t in events.transfers {
        let shares = bi(&t.shares);
        if t.from_address.to_lowercase() != zero {
            store.add(
                t.log_index,
                vault_pos_key(&t.vault, &t.from_address),
                -shares.clone(),
            );
        }
        if t.to_address.to_lowercase() != zero {
            store.add(t.log_index, vault_pos_key(&t.vault, &t.to_address), shares);
        }
    }
}

/// Vault-level totals.
///
/// `total_shares` is exact: ERC-4626 mints and burns show up as Transfers from
/// and to the zero address, so summing those is the share supply.
///
/// `net_deposited_assets` is deliberately NOT called total assets. A MetaMorpho
/// vault's asset balance also grows from interest earned in the underlying Blue
/// markets, which produces no vault-level event. This column is the deposit
/// principal only; for true assets-under-management, join the vault's Blue
/// positions in `positions` and convert shares to assets.
#[substreams::handlers::store]
pub fn store_vault_totals(events: mp::MetaMorphoEvents, store: StoreAddBigInt) {
    let zero = format!("0x{}", "0".repeat(40));

    for t in &events.transfers {
        let shares = bi(&t.shares);
        if t.from_address.to_lowercase() == zero {
            store.add(t.log_index, format!("vtot:{}:total_shares", t.vault.to_lowercase()), shares);
        } else if t.to_address.to_lowercase() == zero {
            store.add(t.log_index, format!("vtot:{}:total_shares", t.vault.to_lowercase()), -shares);
        }
    }
    for d in &events.deposits {
        store.add(
            d.log_index,
            format!("vtot:{}:net_deposited_assets", d.vault.to_lowercase()),
            bi(&d.assets),
        );
    }
    for w in &events.withdraws {
        store.add(
            w.log_index,
            format!("vtot:{}:net_deposited_assets", w.vault.to_lowercase()),
            -bi(&w.assets),
        );
    }
}

fn pow10(e: u32) -> BigInt {
    BigInt::from(10).pow(e)
}

/// Morpho's SharesMathLib.toAssetsUp — rounds against the borrower, which is
/// what `_isHealthy` uses, so health must be computed with this and not a
/// rounded-down conversion.
fn to_assets_up(shares: &BigInt, total_assets: &BigInt, total_shares: &BigInt) -> BigInt {
    let num = shares.clone() * (total_assets.clone() + BigInt::from(VIRTUAL_ASSETS));
    let den = total_shares.clone() + BigInt::from(VIRTUAL_SHARES);
    if den == BigInt::zero() {
        return BigInt::zero();
    }
    // ceil division
    (num + den.clone() - BigInt::one()) / den
}

/// Liquidation incentive factor, WAD.
/// LIF = min(MAX_LIF, WAD / (WAD - LIQUIDATION_CURSOR * (WAD - lltv)))
fn liquidation_incentive(lltv: &BigInt) -> BigInt {
    let wad = pow10(WAD_EXP);
    let max_lif = bi(MAX_LIF);
    if *lltv >= wad {
        return max_lif;
    }
    let cursor_term = BigInt::from(LIQUIDATION_CURSOR) * (wad.clone() - lltv.clone()) / wad.clone();
    let den = wad.clone() - cursor_term;
    if den <= BigInt::zero() {
        return max_lif;
    }
    let lif = wad.clone() * wad / den;
    if lif > max_lif {
        max_lif
    } else {
        lif
    }
}

/// Per-position risk, priced with the market's own oracle read over RPC.
///
/// `healthFactor` is also available from the Morpho API. What is not: the
/// liquidation economics — how much collateral is actually seizable and at what
/// incentive — which a liquidator needs and which only falls out of the
/// contract math.
#[substreams::handlers::map]
pub fn map_position_risk(
    clock: Clock,
    events: sf::Events,
    params: StoreGetProto<mp::MarketParams>,
    totals: StoreGetBigInt,
    positions: StoreGetBigInt,
) -> Result<mp::PositionRisks, Error> {
    let mut out = mp::PositionRisks::default();

    // Only positions touched this block are re-priced. Re-pricing every open
    // position every block would mean an RPC call per market per block for no
    // new information.
    let mut touched: Vec<(String, String)> = Vec::new();
    let mut push = |m: &str, u: &str, v: &mut Vec<(String, String)>| {
        let k = (m.to_string(), u.to_lowercase());
        if !v.contains(&k) {
            v.push(k);
        }
    };
    for (_, ev) in ordered(&events) {
        match ev {
            Ev::Supply(e) => push(&e.market_id, &e.on_behalf, &mut touched),
            Ev::Withdraw(e) => push(&e.market_id, &e.on_behalf, &mut touched),
            Ev::Borrow(e) => push(&e.market_id, &e.on_behalf, &mut touched),
            Ev::Repay(e) => push(&e.market_id, &e.on_behalf, &mut touched),
            Ev::SupplyCollateral(e) => push(&e.market_id, &e.on_behalf, &mut touched),
            Ev::WithdrawCollateral(e) => push(&e.market_id, &e.on_behalf, &mut touched),
            Ev::Liquidate(e) => push(&e.market_id, &e.borrower, &mut touched),
            Ev::Accrue(_) => {}
        }
    }
    if touched.is_empty() {
        return Ok(out);
    }

    // One RPC batch for the distinct markets in this block.
    let mut markets: Vec<String> = Vec::new();
    for (m, _) in &touched {
        if !markets.contains(m) {
            markets.push(m.clone());
        }
    }
    let mut batch = RpcBatch::new();
    let mut priced: Vec<(String, mp::MarketParams)> = Vec::new();
    for m in &markets {
        if let Some(p) = params.get_last(mkt_key(m, "params")) {
            if let Ok(addr) = hex::decode(p.oracle.trim_start_matches("0x")) {
                // A market may be created with the zero oracle; skip those.
                if addr.iter().any(|b| *b != 0) {
                    batch = batch.add(abi::morpho_oracle::functions::Price {}, addr);
                    priced.push((m.clone(), p));
                }
            }
        }
    }
    if priced.is_empty() {
        return Ok(out);
    }
    let responses = match batch.execute() {
        Ok(r) => r.responses,
        Err(_) => return Ok(out),
    };

    let scale = pow10(ORACLE_PRICE_SCALE_EXP);
    let wad = pow10(WAD_EXP);

    for (i, (market_id, p)) in priced.iter().enumerate() {
        let price = match responses.get(i).and_then(|r| {
            RpcBatch::decode::<_, abi::morpho_oracle::functions::Price>(r)
        }) {
            Some(v) => v,
            None => continue,
        };
        if price == BigInt::zero() {
            continue;
        }
        let lltv = bi(&p.lltv);
        let tba = totals
            .get_last(mkt_key(market_id, "total_borrow_assets"))
            .unwrap_or_else(BigInt::zero);
        let tbs = totals
            .get_last(mkt_key(market_id, "total_borrow_shares"))
            .unwrap_or_else(BigInt::zero);
        let lif = liquidation_incentive(&lltv);

        for (m, user) in touched.iter().filter(|(m, _)| m == market_id) {
            let collateral = positions
                .get_last(pos_key(m, user, "collateral"))
                .unwrap_or_else(BigInt::zero);
            let borrow_shares = positions
                .get_last(pos_key(m, user, "borrow_shares"))
                .unwrap_or_else(BigInt::zero);
            if borrow_shares <= BigInt::zero() && collateral <= BigInt::zero() {
                continue;
            }

            let borrowed = to_assets_up(&borrow_shares, &tba, &tbs);
            // maxBorrow = collateral.mulDivDown(price, 1e36).wMulDown(lltv)
            let max_borrow = collateral.clone() * price.clone() / scale.clone() * lltv.clone() / wad.clone();
            let liquidatable = borrowed > BigInt::zero() && max_borrow < borrowed;

            let hf = if borrowed > BigInt::zero() {
                (max_borrow.clone() * wad.clone() / borrowed.clone()).to_string()
            } else {
                String::new()
            };

            // Seizing the whole debt: borrowed.wMulDown(lif).mulDivDown(1e36, price)
            let mut seizable = borrowed.clone() * lif.clone() / wad.clone() * scale.clone() / price.clone();
            if seizable > collateral {
                seizable = collateral.clone();
            }

            out.risks.push(mp::PositionRisk {
                market_id: m.clone(),
                user: user.clone(),
                collateral: collateral.to_string(),
                borrow_shares: borrow_shares.to_string(),
                borrowed_assets: borrowed.to_string(),
                oracle_price: price.to_string(),
                lltv: p.lltv.clone(),
                max_borrow: max_borrow.to_string(),
                health_factor_wad: hf,
                liquidatable,
                seizable_collateral: if liquidatable { seizable.to_string() } else { String::new() },
                liquidation_incentive_wad: lif.to_string(),
                block_num: clock.number,
                timestamp: clock.timestamp.as_ref().map(|t| t.seconds as u64).unwrap_or(0),
            });
        }
    }

    Ok(out)
}

/// Bad debt, attributed to the borrower who produced it.
///
/// The Morpho API reports bad debt per market, so it cannot answer "which
/// position caused this" or "which liquidation socialized this loss".
#[substreams::handlers::map]
pub fn map_bad_debt(events: sf::Events) -> Result<mp::BadDebtEvents, Error> {
    let mut out = mp::BadDebtEvents::default();
    for e in &events.liquidates {
        let assets = bi(&e.bad_debt_assets);
        let shares = bi(&e.bad_debt_shares);
        if assets == BigInt::zero() && shares == BigInt::zero() {
            continue;
        }
        out.events.push(mp::BadDebtEvent {
            id: e.id.clone(),
            market_id: e.market_id.clone(),
            borrower: e.borrower.to_lowercase(),
            liquidator: e.caller.to_lowercase(),
            bad_debt_assets: e.bad_debt_assets.clone(),
            bad_debt_shares: e.bad_debt_shares.clone(),
            seized_assets: e.seized_assets.clone(),
            repaid_assets: e.repaid_assets.clone(),
            tx_hash: e.tx_hash.clone(),
            log_index: e.log_index,
            block_num: e.block_num,
            timestamp: e.timestamp,
        });
    }
    Ok(out)
}

/// Cumulative realized bad debt, per market and per borrower.
///
/// The Morpho API exposes `realizedBadDebt` per market. Per-borrower totals are
/// not available there, so "which addresses repeatedly leave bad debt behind"
/// can only be answered from an index like this one.
#[substreams::handlers::store]
pub fn store_bad_debt(events: mp::BadDebtEvents, store: StoreAddBigInt) {
    for e in events.events {
        let assets = bi(&e.bad_debt_assets);
        let shares = bi(&e.bad_debt_shares);
        store.add(e.log_index, format!("bd:market:{}:assets", e.market_id), assets.clone());
        store.add(e.log_index, format!("bd:market:{}:shares", e.market_id), shares);
        store.add(
            e.log_index,
            format!("bd:borrower:{}:{}:assets", e.market_id, e.borrower),
            assets,
        );
        store.add(e.log_index, format!("bd:borrower:{}:{}:count", e.market_id, e.borrower), BigInt::one());
    }
}

/// The currently-liquidatable set, keyed `liq:{market_id}:{user}`.
///
/// CAVEAT, and it is a real one: positions are only re-priced when an event
/// touches them. A position can cross into liquidatable purely because the
/// oracle price moved, with no Morpho event at all, and this store will not
/// notice until something touches that market. Re-pricing every open position
/// every block would cost an RPC call per market per block, which is not
/// practical here. Treat this as "known liquidatable as of the last touch",
/// not as a complete real-time liquidation feed.
#[substreams::handlers::store]
pub fn store_liquidatable(risks: mp::PositionRisks, store: StoreSetString) {
    for r in risks.risks {
        let key = format!("liq:{}:{}", r.market_id, r.user);
        if r.liquidatable {
            store.set(r.block_num, key, &r.health_factor_wad);
        } else {
            // No longer underwater — drop it rather than leave a stale row.
            store.delete_prefix(r.block_num as i64, &key);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL
// ─────────────────────────────────────────────────────────────────────────────

/// Accumulate column writes per (table, primary key) so a block that touches
/// several fields of one row emits a single upsert instead of clobbering itself.
///
/// Every table in schema.sql holds *stateful entities* — a market's totals, a
/// user's position — not append-only events, so the same primary key is written
/// again on every block that touches it. The sink accumulates operations across
/// a whole flush batch (many blocks) before writing, which is why an
/// INSERT-per-touch fails two different ways for one reason:
///
///   * two blocks inside one batch  -> "a primary key ..., that is already
///     scheduled for insertion, insert should only be called once"
///   * two blocks either side of a flush -> "duplicate key value violates
///     unique constraint"
///
/// `flush` therefore schedules UPSERTs. The sink merges repeat touches of a key
/// within a batch and emits `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`, so
/// a re-touched row updates instead of colliding.
#[derive(Default)]
struct RowSet {
    rows: HashMap<(String, String), Vec<(String, String)>>,
    order: Vec<(String, String)>,
    deleted: HashSet<(String, String)>,
}

impl RowSet {
    /// Drop this row. Supersedes any column writes already accumulated for the
    /// key in this block — the row is going away, so its columns are moot.
    fn delete(&mut self, table: &str, key: &str) {
        let k = (table.to_string(), key.to_string());
        if !self.rows.contains_key(&k) {
            self.order.push(k.clone());
        }
        self.rows.insert(k.clone(), Vec::new());
        self.deleted.insert(k);
    }

    fn put(&mut self, table: &str, key: &str, col: &str, val: String) {
        let k = (table.to_string(), key.to_string());
        if !self.rows.contains_key(&k) {
            self.order.push(k.clone());
        }
        // A write after a delete in the same block means the row came back.
        self.deleted.remove(&k);
        let cols = self.rows.entry(k).or_default();
        // Last write wins. A block can touch one column twice (two events moving
        // the same market's totals), and the row must carry the final value, not
        // a replay of every intermediate one.
        match cols.iter_mut().find(|(c, _)| c == col) {
            Some(slot) => slot.1 = val,
            None => cols.push((col.to_string(), val)),
        }
    }

    fn flush(self, tables: &mut Tables) {
        for k in &self.order {
            if self.deleted.contains(k) {
                tables.delete_row(&k.0, k.1.clone());
                continue;
            }
            let cols = &self.rows[k];
            // upsert_row, never create_row: see the note on RowSet.
            let row = tables.upsert_row(&k.0, k.1.clone());
            for (c, v) in cols {
                row.set(c, v.clone());
            }
        }
    }
}

#[substreams::handlers::map]
pub fn db_out(
    clock: Clock,
    market_params: Deltas<DeltaProto<mp::MarketParams>>,
    market_totals: Deltas<DeltaBigInt>,
    positions: Deltas<DeltaBigInt>,
    vaults: Deltas<DeltaProto<mp::VaultMeta>>,
    vault_positions: Deltas<DeltaBigInt>,
    vault_totals: Deltas<DeltaBigInt>,
    market_fees: Deltas<DeltaString>,
    blue_config: Deltas<DeltaString>,
    bad_debt: Deltas<DeltaBigInt>,
    liquidatable: Deltas<DeltaString>,
) -> Result<DatabaseChanges, Error> {
    Ok(build_changes(
        clock,
        market_params,
        market_totals,
        positions,
        vaults,
        vault_positions,
        vault_totals,
        market_fees,
        blue_config,
        bad_debt,
        liquidatable,
    ))
}

/// The body of `db_out`, split out so tests can drive it directly without going
/// through the generated wasm entrypoint.
fn build_changes(
    clock: Clock,
    market_params: Deltas<DeltaProto<mp::MarketParams>>,
    market_totals: Deltas<DeltaBigInt>,
    positions: Deltas<DeltaBigInt>,
    vaults: Deltas<DeltaProto<mp::VaultMeta>>,
    vault_positions: Deltas<DeltaBigInt>,
    vault_totals: Deltas<DeltaBigInt>,
    market_fees: Deltas<DeltaString>,
    blue_config: Deltas<DeltaString>,
    bad_debt: Deltas<DeltaBigInt>,
    liquidatable: Deltas<DeltaString>,
) -> DatabaseChanges {
    let mut tables = Tables::new();
    let mut rs = RowSet::default();
    let block_num = clock.number.to_string();

    for d in market_params.deltas.iter() {
        let p = &d.new_value;
        rs.put("markets", &p.market_id, "loan_token", p.loan_token.clone());
        rs.put("markets", &p.market_id, "collateral_token", p.collateral_token.clone());
        rs.put("markets", &p.market_id, "oracle", p.oracle.clone());
        rs.put("markets", &p.market_id, "irm", p.irm.clone());
        rs.put("markets", &p.market_id, "lltv", p.lltv.clone());
        rs.put("markets", &p.market_id, "created_block", p.created_block.to_string());
        rs.put("markets", &p.market_id, "created_tx", p.created_tx.clone());
        rs.put("markets", &p.market_id, "updated_block", block_num.clone());
    }

    // keys look like `mkt:{market_id}:{field}`
    for d in market_totals.deltas.iter() {
        let parts: Vec<&str> = d.key.splitn(3, ':').collect();
        if parts.len() != 3 || parts[0] != "mkt" {
            continue;
        }
        rs.put("market_states", parts[1], parts[2], d.new_value.to_string());
        rs.put("market_states", parts[1], "updated_block", block_num.clone());
    }

    for d in market_fees.deltas.iter() {
        if let Some(market_id) = d.key.strip_prefix("fee:") {
            rs.put("market_states", market_id, "fee", d.new_value.clone());
            rs.put("market_states", market_id, "updated_block", block_num.clone());
        }
    }

    // keys look like `pos:{market_id}:{user}:{field}`
    for d in positions.deltas.iter() {
        let parts: Vec<&str> = d.key.splitn(4, ':').collect();
        if parts.len() != 4 || parts[0] != "pos" {
            continue;
        }
        let (market_id, user, field) = (parts[1], parts[2], parts[3]);
        let id = format!("{}:{}", market_id, user);
        rs.put("positions", &id, "market_id", market_id.to_string());
        rs.put("positions", &id, "user_address", user.to_string());
        rs.put("positions", &id, field, d.new_value.to_string());
        rs.put("positions", &id, "updated_block", block_num.clone());
    }

    for d in vaults.deltas.iter() {
        let v = &d.new_value;
        rs.put("vaults", &v.vault, "asset", v.asset.clone());
        rs.put("vaults", &v.vault, "name", v.name.clone());
        rs.put("vaults", &v.vault, "symbol", v.symbol.clone());
        rs.put("vaults", &v.vault, "factory", v.factory.clone());
        rs.put("vaults", &v.vault, "initial_owner", v.initial_owner.clone());
        rs.put("vaults", &v.vault, "created_block", v.created_block.to_string());
        rs.put("vaults", &v.vault, "updated_block", block_num.clone());
    }

    // keys look like `vpos:{vault}:{user}`
    for d in vault_positions.deltas.iter() {
        let parts: Vec<&str> = d.key.splitn(3, ':').collect();
        if parts.len() != 3 || parts[0] != "vpos" {
            continue;
        }
        let (vault, user) = (parts[1], parts[2]);
        let id = format!("{}:{}", vault, user);
        rs.put("vault_positions", &id, "vault", vault.to_string());
        rs.put("vault_positions", &id, "user_address", user.to_string());
        rs.put("vault_positions", &id, "shares", d.new_value.to_string());
        rs.put("vault_positions", &id, "updated_block", block_num.clone());
    }

    // keys look like `vtot:{vault}:{field}`
    for d in vault_totals.deltas.iter() {
        let parts: Vec<&str> = d.key.splitn(3, ':').collect();
        if parts.len() != 3 || parts[0] != "vtot" {
            continue;
        }
        rs.put("vault_states", parts[1], parts[2], d.new_value.to_string());
        rs.put("vault_states", parts[1], "updated_block", block_num.clone());
    }

    // `owner`, or `irm:{address}` / `lltv:{value}` membership rows
    for d in blue_config.deltas.iter() {
        let (kind, value) = match d.key.split_once(':') {
            Some((k, v)) => (k, v.to_string()),
            None => (d.key.as_str(), d.new_value.clone()),
        };
        rs.put("blue_config", &d.key, "kind", kind.to_string());
        rs.put("blue_config", &d.key, "value", value);
        rs.put("blue_config", &d.key, "updated_block", block_num.clone());
    }

    // `bd:market:{id}:{field}` and `bd:borrower:{id}:{user}:{field}`
    for d in bad_debt.deltas.iter() {
        let p: Vec<&str> = d.key.split(':').collect();
        if p.len() == 4 && p[1] == "market" {
            rs.put("market_bad_debt", p[2], p[3], d.new_value.to_string());
            rs.put("market_bad_debt", p[2], "updated_block", block_num.clone());
        } else if p.len() == 5 && p[1] == "borrower" {
            let id = format!("{}:{}", p[2], p[3]);
            rs.put("borrower_bad_debt", &id, "market_id", p[2].to_string());
            rs.put("borrower_bad_debt", &id, "borrower", p[3].to_string());
            rs.put("borrower_bad_debt", &id, p[4], d.new_value.to_string());
            rs.put("borrower_bad_debt", &id, "updated_block", block_num.clone());
        }
    }

    // `liq:{market_id}:{user}` — value is the health factor in WAD.
    //
    // store_liquidatable DELETES the key when a position heals, and a delete
    // delta carries an EMPTY new_value. Treating it like any other delta wrote
    // "" into health_factor_wad — a NUMERIC column, so Postgres rejects it —
    // and left the healed position sitting in liquidatable_positions forever,
    // reporting a liquidation that is no longer available. A delete delta has
    // to delete the row.
    for d in liquidatable.deltas.iter() {
        let p: Vec<&str> = d.key.splitn(3, ':').collect();
        if p.len() != 3 || p[0] != "liq" {
            continue;
        }
        let id = format!("{}:{}", p[1], p[2]);
        // Healed, or no health factor to report: either way the row goes. An
        // empty health factor is never written — a blank is not a liquidation.
        if d.operation == DeltaOperation::Delete || d.new_value.trim().is_empty() {
            rs.delete("liquidatable_positions", &id);
            continue;
        }
        rs.put("liquidatable_positions", &id, "market_id", p[1].to_string());
        rs.put("liquidatable_positions", &id, "user_address", p[2].to_string());
        rs.put("liquidatable_positions", &id, "health_factor_wad", d.new_value.clone());
        rs.put("liquidatable_positions", &id, "updated_block", block_num.clone());
    }

    rs.flush(&mut tables);
    tables.to_database_changes()
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use substreams::pb::substreams::store_delta::Operation as DeltaOp;
    use substreams_database_change::pb::sf::substreams::sink::database::v1::{
        table_change::{Operation, PrimaryKey},
        TableChange,
    };

    /// The live WETH/wstETH market (94.5% LLTV).
    const MKT: &str = "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41";
    const USER: &str = "0x1111111111111111111111111111111111111111";
    const BLOCK: u64 = 18926166;

    fn big(key: &str, v: u64) -> DeltaBigInt {
        DeltaBigInt {
            operation: DeltaOp::Update,
            ordinal: 0,
            key: key.to_string(),
            old_value: BigInt::zero(),
            new_value: BigInt::from(v),
        }
    }

    fn text(key: &str, v: &str) -> DeltaString {
        DeltaString {
            operation: DeltaOp::Update,
            ordinal: 0,
            key: key.to_string(),
            old_value: String::new(),
            new_value: v.to_string(),
        }
    }

    /// Every `db_out` input, defaulted to empty so a test names only what it drives.
    #[derive(Default)]
    struct Inputs {
        market_params: Vec<DeltaProto<mp::MarketParams>>,
        market_totals: Vec<DeltaBigInt>,
        positions: Vec<DeltaBigInt>,
        vaults: Vec<DeltaProto<mp::VaultMeta>>,
        vault_positions: Vec<DeltaBigInt>,
        vault_totals: Vec<DeltaBigInt>,
        market_fees: Vec<DeltaString>,
        blue_config: Vec<DeltaString>,
        bad_debt: Vec<DeltaBigInt>,
        liquidatable: Vec<DeltaString>,
    }

    impl Inputs {
        fn run(self) -> DatabaseChanges {
            build_changes(
                Clock {
                    id: String::new(),
                    number: BLOCK,
                    timestamp: None,
                },
                Deltas { deltas: self.market_params },
                Deltas { deltas: self.market_totals },
                Deltas { deltas: self.positions },
                Deltas { deltas: self.vaults },
                Deltas { deltas: self.vault_positions },
                Deltas { deltas: self.vault_totals },
                Deltas { deltas: self.market_fees },
                Deltas { deltas: self.blue_config },
                Deltas { deltas: self.bad_debt },
                Deltas { deltas: self.liquidatable },
            )
        }
    }

    fn rows<'a>(c: &'a DatabaseChanges, table: &str) -> Vec<&'a TableChange> {
        c.table_changes.iter().filter(|t| t.table == table).collect()
    }

    fn pk(t: &TableChange) -> &str {
        match t.primary_key.as_ref().expect("row has no primary key") {
            PrimaryKey::Pk(k) => k,
            PrimaryKey::CompositePk(_) => panic!("unexpected composite primary key"),
        }
    }

    fn field<'a>(t: &'a TableChange, name: &str) -> Option<&'a str> {
        t.fields.iter().find(|f| f.name == name).map(|f| f.value.as_str())
    }

    /// The CrashLoop in issue #1: several deltas touching one market inside a
    /// single block must produce exactly ONE market_states row, not one per delta.
    #[test]
    fn market_states_is_one_upsert_per_market() {
        let changes = Inputs {
            market_totals: vec![
                big(&format!("mkt:{}:total_supply_assets", MKT), 1_000),
                big(&format!("mkt:{}:total_supply_shares", MKT), 2_000),
                big(&format!("mkt:{}:total_borrow_assets", MKT), 3_000),
                big(&format!("mkt:{}:total_borrow_shares", MKT), 4_000),
            ],
            market_fees: vec![text(&format!("fee:{}", MKT), "5")],
            ..Default::default()
        }
        .run();

        let states = rows(&changes, "market_states");
        assert_eq!(states.len(), 1, "expected one market_states row, got {}", states.len());
        assert_eq!(pk(states[0]), MKT);

        // The single row still carries every column those deltas touched.
        assert_eq!(field(states[0], "total_supply_assets"), Some("1000"));
        assert_eq!(field(states[0], "total_supply_shares"), Some("2000"));
        assert_eq!(field(states[0], "total_borrow_assets"), Some("3000"));
        assert_eq!(field(states[0], "total_borrow_shares"), Some("4000"));
        assert_eq!(field(states[0], "fee"), Some("5"));
        assert_eq!(field(states[0], "updated_block"), Some(BLOCK.to_string().as_str()));
    }

    /// The stateful tables are rewritten on every block that touches them, and the
    /// sink batches many blocks per transaction. An INSERT would collide with
    /// itself inside one batch and with committed rows across batches, so nothing
    /// db_out emits may be a CREATE.
    #[test]
    fn every_change_is_an_upsert_never_an_insert() {
        let changes = Inputs {
            market_totals: vec![big(&format!("mkt:{}:total_supply_assets", MKT), 1)],
            market_fees: vec![text(&format!("fee:{}", MKT), "1")],
            positions: vec![big(&format!("pos:{}:{}:borrow_shares", MKT, USER), 7)],
            vault_positions: vec![big(&format!("vpos:{}:{}", USER, USER), 9)],
            vault_totals: vec![big(&format!("vtot:{}:total_shares", USER), 11)],
            blue_config: vec![text("owner", USER)],
            bad_debt: vec![big(&format!("bd:market:{}:assets", MKT), 13)],
            liquidatable: vec![text(&format!("liq:{}:{}", MKT, USER), "900000000000000000")],
            ..Default::default()
        }
        .run();

        assert!(!changes.table_changes.is_empty(), "no changes emitted");
        for t in &changes.table_changes {
            // DELETE is legitimate (a healed liquidatable position); CREATE is
            // the one that collides inside a sink batch.
            assert_ne!(
                t.operation,
                Operation::Create as i32,
                "table {} pk {} emitted CREATE — it collides inside a flush batch",
                t.table,
                pk(t),
            );
            assert!(
                t.operation == Operation::Upsert as i32
                    || t.operation == Operation::Delete as i32,
                "table {} pk {} emitted unexpected operation {}",
                t.table, pk(t), t.operation,
            );
        }
    }

    /// Two events moving the same column in one block: the row must carry the
    /// final value once, not both writes.
    #[test]
    fn repeated_column_write_keeps_the_last_value() {
        let changes = Inputs {
            market_totals: vec![
                big(&format!("mkt:{}:total_supply_assets", MKT), 100),
                big(&format!("mkt:{}:total_supply_assets", MKT), 250),
            ],
            ..Default::default()
        }
        .run();

        let states = rows(&changes, "market_states");
        assert_eq!(states.len(), 1);
        let hits = states[0]
            .fields
            .iter()
            .filter(|f| f.name == "total_supply_assets")
            .count();
        assert_eq!(hits, 1, "column written {} times, expected once", hits);
        assert_eq!(field(states[0], "total_supply_assets"), Some("250"));
    }

    fn text_del(key: &str) -> DeltaString {
        // store.delete_prefix produces a DELETE delta whose new_value is empty.
        DeltaString {
            operation: DeltaOp::Delete,
            ordinal: 0,
            key: key.to_string(),
            old_value: "900000000000000000".to_string(),
            new_value: String::new(),
        }
    }

    /// A healed position must be DELETED, not upserted with a blank health
    /// factor. The blank is what Postgres rejects on a NUMERIC column, and the
    /// stale row is what reports a liquidation that is no longer available.
    #[test]
    fn healed_position_is_deleted_not_blanked() {
        let changes = Inputs {
            liquidatable: vec![text_del(&format!("liq:{}:{}", MKT, USER))],
            ..Default::default()
        }
        .run();

        let rows_out = rows(&changes, "liquidatable_positions");
        assert_eq!(rows_out.len(), 1);
        assert_eq!(pk(rows_out[0]), format!("{}:{}", MKT, USER));
        assert_eq!(rows_out[0].operation, Operation::Delete as i32,
                   "a healed position must be deleted");
        assert_eq!(field(rows_out[0], "health_factor_wad"), None,
                   "a delete must not carry a health factor");
    }

    /// Defence in depth: an empty health factor is never written as a column,
    /// whatever operation the delta claims.
    #[test]
    fn empty_health_factor_is_never_written() {
        for value in ["", "   "] {
            let changes = Inputs {
                liquidatable: vec![text(&format!("liq:{}:{}", MKT, USER), value)],
                ..Default::default()
            }
            .run();
            for t in &changes.table_changes {
                assert!(field(t, "health_factor_wad").is_none(),
                        "empty health factor leaked into a column for {value:?}");
            }
        }
    }

    /// A live liquidatable position still upserts every column.
    #[test]
    fn live_liquidatable_position_still_upserts() {
        let changes = Inputs {
            liquidatable: vec![text(&format!("liq:{}:{}", MKT, USER), "900000000000000000")],
            ..Default::default()
        }
        .run();
        let r = rows(&changes, "liquidatable_positions");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].operation, Operation::Upsert as i32);
        assert_eq!(field(r[0], "health_factor_wad"), Some("900000000000000000"));
        assert_eq!(field(r[0], "market_id"), Some(MKT));
        assert_eq!(field(r[0], "user_address"), Some(USER));
    }

    /// Delete and write in one block resolve to a single operation, never two
    /// changes for one primary key — that would be the CrashLoop all over again.
    #[test]
    fn delete_and_write_in_one_block_emit_one_change() {
        let key = format!("liq:{}:{}", MKT, USER);
        let changes = Inputs {
            liquidatable: vec![text_del(&key), text(&key, "800000000000000000")],
            ..Default::default()
        }
        .run();
        let r = rows(&changes, "liquidatable_positions");
        assert_eq!(r.len(), 1, "one primary key must yield one change");
        // The later write wins: the position is liquidatable again.
        assert_eq!(r[0].operation, Operation::Upsert as i32);
        assert_eq!(field(r[0], "health_factor_wad"), Some("800000000000000000"));
    }

    /// A position keyed `{market}:{user}` is one row carrying both id columns.
    #[test]
    fn position_row_is_keyed_by_market_and_user() {
        let changes = Inputs {
            positions: vec![
                big(&format!("pos:{}:{}:supply_shares", MKT, USER), 10),
                big(&format!("pos:{}:{}:collateral", MKT, USER), 20),
            ],
            ..Default::default()
        }
        .run();

        let p = rows(&changes, "positions");
        assert_eq!(p.len(), 1);
        assert_eq!(pk(p[0]), format!("{}:{}", MKT, USER));
        assert_eq!(field(p[0], "market_id"), Some(MKT));
        assert_eq!(field(p[0], "user_address"), Some(USER));
        assert_eq!(field(p[0], "supply_shares"), Some("10"));
        assert_eq!(field(p[0], "collateral"), Some("20"));
    }
}
