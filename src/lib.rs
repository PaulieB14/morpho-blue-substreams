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

use std::collections::HashMap;
use std::str::FromStr;

use substreams::errors::Error;
use substreams::pb::substreams::Clock;
use substreams::scalar::BigInt;
use substreams::store::{
    DeltaBigInt, DeltaProto, DeltaString, Deltas, StoreAdd, StoreAddBigInt, StoreGet,
    StoreGetProto, StoreGetString, StoreNew, StoreSet, StoreSetProto, StoreSetString,
};
use substreams_database_change::pb::sf::substreams::sink::database::v1::DatabaseChanges;
use substreams_database_change::tables::Tables;
use substreams_ethereum::pb::eth::v2::Block;
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

// ─────────────────────────────────────────────────────────────────────────────
// SQL
// ─────────────────────────────────────────────────────────────────────────────

/// Accumulate column writes per (table, primary key) so a block that touches
/// several fields of one row emits a single upsert instead of clobbering itself.
#[derive(Default)]
struct RowSet {
    rows: HashMap<(String, String), Vec<(String, String)>>,
    order: Vec<(String, String)>,
}

impl RowSet {
    fn put(&mut self, table: &str, key: &str, col: &str, val: String) {
        let k = (table.to_string(), key.to_string());
        if !self.rows.contains_key(&k) {
            self.order.push(k.clone());
        }
        self.rows.entry(k).or_default().push((col.to_string(), val));
    }

    fn flush(self, tables: &mut Tables) {
        for k in &self.order {
            let cols = &self.rows[k];
            let row = tables.create_row(&k.0, k.1.clone());
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
) -> Result<DatabaseChanges, Error> {
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

    rs.flush(&mut tables);
    Ok(tables.to_database_changes())
}
