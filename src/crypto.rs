use std::path::Path;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use tokio::fs;

#[derive(Clone)]
pub struct TokenCrypto {
    key: [u8; 32],
}

impl TokenCrypto {
    pub async fn load_or_create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }

        let key = match fs::read_to_string(path).await {
            Ok(raw) => decode_key(raw.trim())?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut key = [0_u8; 32];
                OsRng.fill_bytes(&mut key);
                let encoded = STANDARD_NO_PAD.encode(key);
                fs::write(path, encoded.as_bytes())
                    .await
                    .with_context(|| format!("writing encryption key to {}", path.display()))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let perms = std::fs::Permissions::from_mode(0o600);
                    std::fs::set_permissions(path, perms)
                        .with_context(|| format!("chmod 0600 {}", path.display()))?;
                }
                key
            }
            Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
        };

        Ok(Self { key })
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String> {
        let cipher =
            Aes256Gcm::new_from_slice(&self.key).map_err(|_| anyhow!("invalid encryption key"))?;
        let mut nonce = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| anyhow!("encryption failed"))?;
        Ok(format!(
            "v1:{}:{}",
            STANDARD_NO_PAD.encode(nonce),
            STANDARD_NO_PAD.encode(ciphertext)
        ))
    }

    pub fn decrypt(&self, encoded: &str) -> Result<String> {
        let mut parts = encoded.splitn(3, ':');
        let version = parts.next().unwrap_or_default();
        let nonce = parts
            .next()
            .ok_or_else(|| anyhow!("encrypted token missing nonce"))?;
        let ciphertext = parts
            .next()
            .ok_or_else(|| anyhow!("encrypted token missing ciphertext"))?;
        if version != "v1" {
            return Err(anyhow!("unsupported encrypted token version"));
        }
        let nonce = STANDARD_NO_PAD.decode(nonce).context("decoding nonce")?;
        let ciphertext = STANDARD_NO_PAD
            .decode(ciphertext)
            .context("decoding ciphertext")?;
        if nonce.len() != 12 {
            return Err(anyhow!("invalid nonce length"));
        }
        let cipher =
            Aes256Gcm::new_from_slice(&self.key).map_err(|_| anyhow!("invalid encryption key"))?;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| anyhow!("decryption failed"))?;
        String::from_utf8(plaintext).context("decrypted token was not utf-8")
    }
}

fn decode_key(raw: &str) -> Result<[u8; 32]> {
    let decoded = STANDARD_NO_PAD
        .decode(raw)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(raw))
        .context("decoding encryption key")?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("encryption key must decode to exactly 32 bytes"))
}
