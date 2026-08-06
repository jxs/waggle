use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out_dir = root.join("src");
    let proto_file = out_dir.join("waggle.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    let file_descriptors =
        protox::compile([&proto_file], [&root.join("src")]).expect("failed to compile proto files");

    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_fds(file_descriptors)
        .expect("failed to compile file descriptors");
}
