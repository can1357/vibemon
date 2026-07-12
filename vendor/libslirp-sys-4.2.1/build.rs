fn main() {
    let target = std::env::var("TARGET").expect("TARGET is set by Cargo");
    if target.contains("windows") {
        println!("cargo:rustc-link-lib=slirp");
    } else {
        pkg_config::Config::new()
            .atleast_version("4.2.0")
            .probe("slirp")
            .expect("libslirp >= 4.2.0 is required");
    }
}
