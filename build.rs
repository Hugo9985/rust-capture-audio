fn main() {
    println!("cargo:rerun-if-changed=assets/icon.ico");
    println!("cargo:rerun-if-changed=build.rs");

    // Intègre l'icône dans le .exe (Windows uniquement)
    #[cfg(windows)]
    {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("assets/icon.ico");
        if let Err(e) = res.compile() {
            // Ne bloque pas la compilation si rc.exe est introuvable
            println!("cargo:warning=Icône non intégrée : {e}");
        }
    }
}
