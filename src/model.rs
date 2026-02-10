use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub private_key_path: String,
    #[serde(default)]
    pub key_passphrase: String,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 22,
            username: String::new(),
            password: String::new(),
            private_key_path: String::new(),
            key_passphrase: String::new(),
        }
    }
}
