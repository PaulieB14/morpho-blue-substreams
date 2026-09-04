# morpho-blue-substreams (PaulieB14)

Composable Morpho Blue + MetaMorpho Substreams for Ethereum (and Base next).

**Architecture (lego):** import StreamingFast `morpho-blue-substreams@v0.1.0` `map_events`, then add position/market stores, MetaMorpho vault modules, and SQL `db_out` — without forking their decoder.

Target shapes align with [Morpho Blue API](https://docs.morpho.org/developers/api/morpho/): markets, market state, user positions, vault positions (on-chain tape; no USD/APY off-chain fields).

WIP — scaffold landing via cloud agent.
