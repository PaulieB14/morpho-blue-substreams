# morpho-blue-substreams (PaulieB14)

Composable **Morpho Blue + MetaMorpho** Substreams for Ethereum mainnet.

## Lego architecture

We **do not fork** StreamingFast’s Blue decoder. We **import** it and stack product modules on top:

| Layer | Source | What you get |
| --- | --- | --- |
| Blue events | `morpho-blue-substreams@v0.1.0` → `morpho_sf:map_events` | CreateMarket, Supply/Borrow/…, Liquidate, AccrueInterest, FlashLoan |
| Stores + MetaMorpho + upsert SQL | **this package** | market params/totals, user positions, MetaMorpho vaults/shares, `db_out` |

```text
morpho_sf:map_events ──► store_market_params
                     ├──► store_positions (+ market totals)
                     └──► (optional raw event tables)

Block ──► map_metamorpho_events ──► store_vaults / store_vault_positions

stores ──► db_out (upserts)
```

StreamingFast stub alone is an append-only event dump ([registry](https://substreams.dev/packages/morpho-blue-substreams/v0.1.0)). This pack targets Morpho API–like shapes: [markets, state, positions, vaults](https://docs.morpho.org/developers/api/morpho/).

## Status

Scaffold + full implementation brief for Claude/Cursor: [`docs/CLAUDE_BRIEF.md`](docs/CLAUDE_BRIEF.md).

Rust WASM modules are next (hand the brief to Claude if Cloud Agents / Pro aren’t available).

## Contracts (Ethereum)

| Contract | Address |
| --- | --- |
| Morpho Blue | `0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb` |
| MetaMorpho Factory V1.1 | `0x1897A8997241C1cD4bD0698647e4EB7213535c24` |
| MetaMorpho Factory (old) | `0xA9c3D3a366466Fa809d1Ae982Fb2c46E5fC41101` |

Initial block: `18883124` (Blue deploy).

## Develop

```bash
substreams info morpho-blue-substreams@v0.1.0   # confirm import target
# after implementation:
substreams build
substreams run . db_out -e mainnet.eth.streamingfast.io:443 --start-block 18883124 --stop-block +50
```

Auth: `substreams auth` / [thegraph.market](https://thegraph.market).

## Not doing

- Large PR into StreamingFast monorepo for stores/MetaMorpho (optional tiny PR later for missing Blue admin events only)
- Base network in v0.1 (SF import is `mainnet`; Base is a follow-up pack or params variant)
