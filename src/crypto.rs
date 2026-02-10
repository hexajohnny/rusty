use anyhow::{anyhow, Context, Result};

#[cfg(windows)]
mod imp {
    use super::*;
    use std::ptr;

    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Cryptography::{
        CryptProtectData, CryptUnprotectData, CRYPT_INTEGER_BLOB,
    };

    fn to_blob(buf: &[u8]) -> CRYPT_INTEGER_BLOB {
        CRYPT_INTEGER_BLOB {
            cbData: u32::try_from(buf.len()).unwrap_or(u32::MAX),
            pbData: buf.as_ptr() as *mut u8,
        }
    }

    fn take_blob(blob: CRYPT_INTEGER_BLOB) -> Vec<u8> {
        if blob.pbData.is_null() || blob.cbData == 0 {
            return Vec::new();
        }
        unsafe {
            let out = std::slice::from_raw_parts(blob.pbData, blob.cbData as usize).to_vec();
            let _ = LocalFree(blob.pbData as _);
            out
        }
    }

    pub fn encrypt_for_current_user(plaintext: &[u8]) -> Result<Vec<u8>> {
        // DPAPI is fast and leverages the OS; avoids CPU-heavy KDFs like bcrypt.
        let mut in_blob = to_blob(plaintext);
        let mut out_blob = CRYPT_INTEGER_BLOB {
            cbData: 0,
            pbData: ptr::null_mut(),
        };
        // Optional "entropy" isn't necessary here; we rely on per-user DPAPI.
        let ok = unsafe {
            CryptProtectData(
                &mut in_blob as *mut CRYPT_INTEGER_BLOB,
                ptr::null(),
                ptr::null(),
                ptr::null_mut(),
                ptr::null_mut(),
                0,
                &mut out_blob as *mut CRYPT_INTEGER_BLOB,
            )
        };
        if ok == 0 {
            return Err(anyhow!("DPAPI encrypt failed"));
        }
        Ok(take_blob(out_blob))
    }

    pub fn decrypt_for_current_user(ciphertext: &[u8]) -> Result<Vec<u8>> {
        let mut in_blob = to_blob(ciphertext);
        let mut out_blob = CRYPT_INTEGER_BLOB {
            cbData: 0,
            pbData: ptr::null_mut(),
        };
        let ok = unsafe {
            CryptUnprotectData(
                &mut in_blob as *mut CRYPT_INTEGER_BLOB,
                ptr::null_mut(),
                ptr::null(),
                ptr::null_mut(),
                ptr::null_mut(),
                0,
                &mut out_blob as *mut CRYPT_INTEGER_BLOB,
            )
        };
        if ok == 0 {
            return Err(anyhow!("DPAPI decrypt failed"));
        }
        Ok(take_blob(out_blob))
    }
}

#[cfg(not(windows))]
mod imp {
    use super::*;

    pub fn encrypt_for_current_user(plaintext: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    pub fn decrypt_for_current_user(ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }
}

pub fn encrypt_for_current_user(plaintext: &[u8]) -> Result<Vec<u8>> {
    imp::encrypt_for_current_user(plaintext).context("encrypt_for_current_user")
}

pub fn decrypt_for_current_user(ciphertext: &[u8]) -> Result<Vec<u8>> {
    imp::decrypt_for_current_user(ciphertext).context("decrypt_for_current_user")
}
