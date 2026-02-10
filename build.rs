fn main() {
    #[cfg(windows)]
    {
        let mut res = winres::WindowsResource::new();
        res.set_icon("assets/icon.ico");
        // Populate Windows version info so the binary has sensible metadata in Explorer.
        res.set("CompanyName", "hexajohnny");
        res.set("LegalCopyright", "Copyright (c) hexajohnny");
        res.set("FileDescription", "Rusty SSH");
        res.set("ProductName", "Rusty SSH");
        res.compile().unwrap();
    }
}
