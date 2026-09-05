use substreams_ethereum::Abigen;

fn main() {
    for (name, abi, out) in [
        ("MorphoBlueAdmin", "abi/MorphoBlueAdmin.json", "src/abi/morpho_blue_admin.rs"),
        ("MetaMorphoFactory", "abi/MetaMorphoFactory.json", "src/abi/metamorpho_factory.rs"),
        ("MetaMorpho", "abi/MetaMorpho.json", "src/abi/metamorpho.rs"),
        ("MorphoOracle", "abi/MorphoOracle.json", "src/abi/morpho_oracle.rs"),
    ] {
        println!("cargo:rerun-if-changed={}", abi);
        Abigen::new(name, abi)
            .unwrap_or_else(|e| panic!("load {} ABI: {}", name, e))
            .generate()
            .unwrap_or_else(|e| panic!("generate {} bindings: {}", name, e))
            .write_to_file(out)
            .unwrap_or_else(|e| panic!("write {}: {}", out, e));
    }
}
