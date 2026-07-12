fn main() {
    let target = std::env::var("TARGET").expect("TARGET is set by Cargo");
    if target.contains("windows") {
        if let Ok(root) = std::env::var("VCPKG_ROOT") {
            let triplet = std::env::var("VCPKG_DEFAULT_TRIPLET")
                .unwrap_or_else(|_| "x64-windows".to_owned());
            println!("cargo:rustc-link-search=native={root}/installed/{triplet}/lib");
        }
        println!("cargo:rustc-link-lib=slirp");
    } else {
        pkg_config::Config::new()
            .atleast_version("4.2.0")
            .probe("slirp")
            .expect("libslirp >= 4.2.0 is required");
    }
}
