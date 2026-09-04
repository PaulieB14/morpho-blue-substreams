# Vendored StreamingFast decoder

`morpho-blue-substreams` is not published to the Substreams registry — it exists
only as source in
[streamingfast/substreams-chain-modules](https://github.com/streamingfast/substreams-chain-modules/tree/main/lending/morpho-blue-substreams).
These `.spkg` files are builds of that source, vendored so this package is
self-contained and reproducible.

- `morpho-blue-substreams-v0.1.0.spkg` — `network: mainnet`, initial block 18883124
- `morpho-blue-substreams-base-v0.1.0.spkg` — same code repacked for
  `network: base`, initial block 13977148 (Morpho Blue is deployed at the same
  address on Base; the decoder itself is network-agnostic)

Regenerate both with `make vendor`.

Upstream is licensed **Apache-2.0**, © StreamingFast. No source changes were
made beyond the manifest's `network`, `initialBlock` and package `name`.
