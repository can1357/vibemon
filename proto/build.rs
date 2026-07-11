fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("cargo:rerun-if-changed=vmon/v1/api.proto");
	println!("cargo:rerun-if-changed=vmon/v1/bridge.proto");
	let fds = protox::compile(["vmon/v1/api.proto", "vmon/v1/bridge.proto"], ["."])?;
	tonic_prost_build::configure().compile_fds(fds)?;
	Ok(())
}
