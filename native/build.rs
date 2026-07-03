fn main() {
    // The `mimalloc` feature rebinds the C allocator for everything linked INTO this library —
    // librdkafka's per-message op calloc/free (unreachable from a Rust #[global_allocator]) and the
    // Rust side's own allocations — by aliasing the libc allocation symbols to mimalloc's at link
    // time. The binding is resolved inside the library only: nothing is exported, no malloc zone is
    // registered, and the hosting process (a Flink JVM) is untouched — the failure mode that made
    // the process-wide override benchmark-grade only. strdup/strndup must be aliased along with
    // malloc/free: leaving them to libc would hand out libc-owned pointers that mimalloc's free
    // would later reject.
    if std::env::var_os("CARGO_FEATURE_MIMALLOC").is_some() {
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        let symbols = [
            "malloc",
            "calloc",
            "realloc",
            "free",
            "strdup",
            "strndup",
            "posix_memalign",
            "aligned_alloc",
        ];
        for symbol in symbols {
            match target_os.as_str() {
                "macos" => {
                    println!("cargo:rustc-link-arg=-Wl,-alias,_mi_{symbol},_{symbol}");
                }
                "linux" => {
                    println!("cargo:rustc-link-arg=-Wl,--defsym={symbol}=mi_{symbol}");
                }
                other => {
                    panic!("the mimalloc feature has no link-alias mapping for target OS {other}");
                }
            }
        }
    }
}
