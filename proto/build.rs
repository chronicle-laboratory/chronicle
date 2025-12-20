fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                "pb_external.proto",
                "pb_metadata.proto",
                "pb_storage.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
