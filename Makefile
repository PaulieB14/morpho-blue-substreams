SPKG      := morpho-blue-paulie-v0.2.3.spkg
SPKG_BASE := morpho-blue-paulie-base-v0.2.3.spkg
WASM      := target/wasm32-unknown-unknown/release/morpho_blue_paulie.wasm
ENDPOINT  ?= mainnet.eth.streamingfast.io:443
VENDOR    := vendor/morpho-blue-substreams-v0.1.0.spkg

.PHONY: all build test pack pack-base protogen run stale vendor base-manifest clean

all: build pack

build:
	cargo build --target wasm32-unknown-unknown --release

# Host-side unit tests for db_out (the crate is cdylib+rlib so this works).
test:
	cargo test

protogen:
	substreams protogen substreams.yaml --exclude-paths="sf/substreams,google"

# `substreams pack` does NOT compile — it packages whatever .wasm sits at the
# manifest path. Always build first, or you ship a stale binary.
pack: build
	substreams pack -o $(SPKG)

stale:
	@test $(WASM) -nt src/lib.rs || (echo "STALE: $(WASM) is older than src/lib.rs — run 'make build'"; exit 1)
	@echo "wasm is newer than sources"

# Rebuild the vendored StreamingFast decoder from source.
# It is not on the Substreams registry, so we ship a build of it to keep this
# package self-contained.
vendor:
	rm -rf /tmp/scm && git clone --depth 1 https://github.com/streamingfast/substreams-chain-modules.git /tmp/scm
	cd /tmp/scm && cargo build --target wasm32-unknown-unknown --release -p morpho_blue_substreams
	cd /tmp/scm/lending/morpho-blue-substreams && substreams pack -o $(CURDIR)/$(VENDOR)
	cd /tmp/scm/lending/morpho-blue-substreams && sed -e 's/^network: mainnet/network: base/' \
	  -e 's/initialBlock: 18883124/initialBlock: 13977148/' \
	  -e 's/^  name: morpho-blue-substreams/  name: morpho-blue-substreams-base/' \
	  substreams.yaml > substreams.base.yaml && \
	  substreams pack substreams.base.yaml -o $(CURDIR)/vendor/morpho-blue-substreams-base-v0.1.0.spkg
	@echo "refreshed both vendored decoders"

pack-base: build
	substreams pack substreams.base.yaml -o $(SPKG_BASE)

# substreams.base.yaml is generated so the two manifests cannot drift.
base-manifest:
	python3 -c "s=open('substreams.yaml').read();\
s=s.replace('  name: morpho_blue_paulie\n','  name: morpho_blue_paulie_base\n');\
s=s.replace('Morpho Blue + MetaMorpho Substreams. Composes','Morpho Blue + MetaMorpho Substreams for BASE. Composes');\
s=s.replace('network: mainnet','network: base');\
s=s.replace('initialBlock: 18883124','initialBlock: 13977148');\
s=s.replace('morpho-blue-substreams-v0.1.0.spkg','morpho-blue-substreams-base-v0.1.0.spkg');\
open('substreams.base.yaml','w').write('# GENERATED from substreams.yaml by \`make base-manifest\` — edit that file, not this one.\n'+s)"

run: pack
	substreams run $(SPKG) db_out -e $(ENDPOINT) --start-block 18883124 --stop-block +1000

clean:
	cargo clean && rm -f $(SPKG) $(SPKG_BASE)
