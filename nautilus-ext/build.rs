fn main() {
    // Obtener flags de compilación para libnautilus-extension-4
    let nautilus = pkg_config::Config::new()
        .atleast_version("4.0")
        .probe("libnautilus-extension-4")
        .expect("libnautilus-extension-4 not found. Install nautilus-devel.");
    
    // Imprimir paths de búsqueda para el linker
    for path in &nautilus.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }
    
    // Linkear con nautilus-extension
    println!("cargo:rustc-link-lib=nautilus-extension");
    
    // Re-run si cambian los headers
    println!("cargo:rerun-if-changed=build.rs");
}
