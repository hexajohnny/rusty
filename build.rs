fn main() {
    #[cfg(windows)]
    {
        let mut res = winres::WindowsResource::new();
        res.set_icon("assets/icon.ico");
        // Populate Windows version info so the binary has sensible metadata in Explorer.
        res.set("CompanyName", "hexajohnny");
        res.set("LegalCopyright", "Copyright (c) hexajohnny");
        res.set("FileDescription", "Rusty");
        res.set("ProductName", "Rusty");
        res.set_manifest(
            r#"
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <application xmlns="urn:schemas-microsoft-com:asm.v3">
    <windowsSettings>
      <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true/pm</dpiAware>
      <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">PerMonitorV2, PerMonitor</dpiAwareness>
    </windowsSettings>
  </application>
</assembly>
"#,
        );

        if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("gnu") {
            let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
            res.set_toolkit_path(&manifest_dir);
            res.set_windres_path(&format!("{manifest_dir}\\.cargo\\windres-ucrt.cmd"));
            res.set_ar_path(&format!("{manifest_dir}\\.cargo\\ar-ucrt.cmd"));
        }

        res.compile().expect("failed to compile Windows resources");

        // With GNU toolchains, linking the resource object through a static archive can be
        // dropped as "unused". Link resource.o directly so Explorer sees the file icon.
        if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("gnu") {
            if let Ok(out_dir) = std::env::var("OUT_DIR") {
                println!("cargo:rustc-link-arg={}\\resource.o", out_dir);
            }
        }
    }
}
