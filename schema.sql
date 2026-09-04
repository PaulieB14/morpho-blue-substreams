-- Morpho Blue + MetaMorpho — target schema for `db_out`.
--
-- Every numeric column carries DEFAULT 0 on purpose. db_out emits one upsert per
-- (table, primary key) containing ONLY the columns that changed in that block, so
-- the first write for a row is a partial INSERT. Without defaults those inserts
-- would violate NOT NULL.
--
-- Values are raw on-chain integers (no decimal scaling). Shares are not assets:
-- convert with Morpho's virtual-share math (VIRTUAL_SHARES=1e6, VIRTUAL_ASSETS=1)
-- against the matching market_states row. See docs/MORPHO_BLUE_ACCOUNTING.md.

CREATE TABLE IF NOT EXISTS markets (
  id                TEXT PRIMARY KEY,          -- market_id: keccak256(abi.encode(marketParams))
  loan_token        TEXT NOT NULL DEFAULT '',
  collateral_token  TEXT NOT NULL DEFAULT '',
  oracle            TEXT NOT NULL DEFAULT '',  -- price scale is 1e36
  irm               TEXT NOT NULL DEFAULT '',
  lltv              NUMERIC NOT NULL DEFAULT 0,-- WAD (1e18)
  created_block     BIGINT,
  created_tx        TEXT,
  updated_block     BIGINT
);

CREATE TABLE IF NOT EXISTS market_states (
  id                      TEXT PRIMARY KEY,    -- market_id
  total_supply_assets     NUMERIC NOT NULL DEFAULT 0,
  total_supply_shares     NUMERIC NOT NULL DEFAULT 0,
  total_borrow_assets     NUMERIC NOT NULL DEFAULT 0,
  total_borrow_shares     NUMERIC NOT NULL DEFAULT 0,
  total_collateral_assets NUMERIC NOT NULL DEFAULT 0,
  fee                     NUMERIC NOT NULL DEFAULT 0,  -- WAD, from SetFee
  updated_block           BIGINT
);

CREATE TABLE IF NOT EXISTS positions (
  id             TEXT PRIMARY KEY,             -- {market_id}:{user}
  market_id      TEXT NOT NULL DEFAULT '',
  user_address   TEXT NOT NULL DEFAULT '',
  supply_shares  NUMERIC NOT NULL DEFAULT 0,
  borrow_shares  NUMERIC NOT NULL DEFAULT 0,
  collateral     NUMERIC NOT NULL DEFAULT 0,
  updated_block  BIGINT
);

CREATE TABLE IF NOT EXISTS vaults (
  id             TEXT PRIMARY KEY,             -- vault address
  asset          TEXT NOT NULL DEFAULT '',
  name           TEXT NOT NULL DEFAULT '',
  symbol         TEXT NOT NULL DEFAULT '',
  factory        TEXT NOT NULL DEFAULT '',
  initial_owner  TEXT NOT NULL DEFAULT '',
  created_block  BIGINT,
  updated_block  BIGINT
);

CREATE TABLE IF NOT EXISTS vault_positions (
  id             TEXT PRIMARY KEY,             -- {vault}:{user}
  vault          TEXT NOT NULL DEFAULT '',
  user_address   TEXT NOT NULL DEFAULT '',
  shares         NUMERIC NOT NULL DEFAULT 0,
  updated_block  BIGINT
);

-- Vault-level aggregates. total_shares is exact (ERC-4626 mint/burn Transfers).
-- net_deposited_assets is deposit principal only: a vault's real AUM also grows
-- from interest earned in the underlying Blue markets, which emits no vault event.
CREATE TABLE IF NOT EXISTS vault_states (
  id                    TEXT PRIMARY KEY,      -- vault address
  total_shares          NUMERIC NOT NULL DEFAULT 0,
  net_deposited_assets  NUMERIC NOT NULL DEFAULT 0,
  updated_block         BIGINT
);

-- Protocol-level Blue config: `owner`, plus `irm:{address}` and `lltv:{value}`
-- membership rows. Morpho has no disable, so these are append-only.
CREATE TABLE IF NOT EXISTS blue_config (
  id             TEXT PRIMARY KEY,
  kind           TEXT NOT NULL DEFAULT '',     -- 'owner' | 'irm' | 'lltv'
  value          TEXT NOT NULL DEFAULT '',
  updated_block  BIGINT
);

CREATE INDEX IF NOT EXISTS positions_market_idx ON positions (market_id);
CREATE INDEX IF NOT EXISTS positions_user_idx   ON positions (user_address);
CREATE INDEX IF NOT EXISTS vault_pos_vault_idx  ON vault_positions (vault);
CREATE INDEX IF NOT EXISTS vault_pos_user_idx   ON vault_positions (user_address);
