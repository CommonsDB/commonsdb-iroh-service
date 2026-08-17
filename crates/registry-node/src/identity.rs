//! Node `SecretKey` handling — docs/operator-guide.md, "Keys". A
//! long-running node (`registryd`) persists a key so its `EndpointId` stays
//! stable across restarts; short-lived reader invocations may use a fresh
//! ephemeral key instead.

use iroh::SecretKey;
use std::path::Path;

pub fn generate_secret_key() -> SecretKey {
    SecretKey::generate()
}

/// Load a persisted node identity from `path`, generating and persisting a
/// new one if it does not exist yet — see docs/operator-guide.md, "Keys".
pub async fn load_or_generate_secret_key(path: &Path) -> anyhow::Result<SecretKey> {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                anyhow::anyhow!("secret key file at {} is not 32 bytes", path.display())
            })?;
            Ok(SecretKey::from_bytes(&arr))
        }
        Err(_) => {
            let key = generate_secret_key();
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(path, key.to_bytes()).await?;
            Ok(key)
        }
    }
}

pub async fn load_secret_key_bytes(path: &Path, bytes: &[u8; 32]) -> anyhow::Result<SecretKey> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, bytes).await?;
    Ok(SecretKey::from_bytes(bytes))
}

/// Generic 32-byte key material loader, used for the `NamespaceSecret` and
/// author keys (docs/operator-guide.md, "Keys"), which are not iroh
/// `SecretKey`s themselves but are constructed the same way (`from_bytes`)
/// on top of 32 random bytes, persisted next to the node identity.
pub async fn load_or_generate_bytes(path: &Path) -> anyhow::Result<[u8; 32]> {
    match tokio::fs::read(path).await {
        Ok(bytes) => bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("key file at {} is not 32 bytes", path.display())),
        Err(_) => {
            let mut bytes = [0u8; 32];
            use rand::RngCore;
            rand::rngs::OsRng.fill_bytes(&mut bytes);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(path, bytes).await?;
            Ok(bytes)
        }
    }
}
