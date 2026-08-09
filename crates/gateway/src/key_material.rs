use std::collections::HashMap;
use std::io;

use crate::config::KeyMaterialSource;
use ring::digest::{Context, SHA256};

#[derive(Debug, thiserror::Error)]
pub enum KeyMaterialError {
    #[error("source env var `{name}` is unset or empty")]
    MissingEnv { name: String },
    #[error("file `{path}` could not be read ({kind}): {error}")]
    FileRead {
        path: String,
        kind: io::ErrorKind,
        error: String,
    },
    #[error("file `{path}` is empty")]
    EmptyFile { path: String },
    #[error("file `{path}` is not valid UTF-8")]
    InvalidUtf8 { path: String },
}

pub fn resolve(
    source: KeyMaterialSource<'_>,
    env: &HashMap<String, String>,
) -> Result<String, KeyMaterialError> {
    match source {
        KeyMaterialSource::Env(name) => env
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| KeyMaterialError::MissingEnv {
                name: name.to_owned(),
            }),
        KeyMaterialSource::File(path) => {
            let bytes = std::fs::read(path).map_err(|error| KeyMaterialError::FileRead {
                path: path.to_owned(),
                kind: error.kind(),
                error: error.to_string(),
            })?;
            if bytes.is_empty() {
                return Err(KeyMaterialError::EmptyFile {
                    path: path.to_owned(),
                });
            }
            #[cfg(unix)]
            warn_if_world_readable(path);
            String::from_utf8(bytes).map_err(|_| KeyMaterialError::InvalidUtf8 {
                path: path.to_owned(),
            })
        }
    }
}

pub fn fingerprint(label: &str, material: &str) -> String {
    let mut context = Context::new(&SHA256);
    context.update(b"axond-key-material-v1\0");
    context.update(label.as_bytes());
    context.update(b"\0");
    context.update(material.as_bytes());
    context
        .finish()
        .as_ref()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(unix)]
fn warn_if_world_readable(path: &str) {
    use std::os::unix::fs::PermissionsExt;

    if let Ok(metadata) = std::fs::metadata(path)
        && metadata.permissions().mode() & 0o077 != 0
    {
        tracing::warn!(path, "key material file is readable by group or others");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn path(name: &str) -> String {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "axond-key-material-{}-{}-{name}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .to_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn resolves_env_without_trimming() {
        let env = HashMap::from([("KEY".to_owned(), "secret\n".to_owned())]);
        assert_eq!(
            resolve(KeyMaterialSource::Env("KEY"), &env).unwrap(),
            "secret\n"
        );
    }

    #[test]
    fn rejects_missing_and_empty_env() {
        for env in [
            HashMap::new(),
            HashMap::from([("KEY".to_owned(), String::new())]),
        ] {
            assert!(matches!(
                resolve(KeyMaterialSource::Env("KEY"), &env),
                Err(KeyMaterialError::MissingEnv { .. })
            ));
        }
    }

    #[test]
    fn rejects_missing_empty_and_invalid_utf8_files() {
        let missing = path("missing");
        assert!(matches!(
            resolve(KeyMaterialSource::File(&missing), &HashMap::new()),
            Err(KeyMaterialError::FileRead {
                kind: std::io::ErrorKind::NotFound,
                ..
            })
        ));

        let empty = path("empty");
        std::fs::write(&empty, []).unwrap();
        assert!(matches!(
            resolve(KeyMaterialSource::File(&empty), &HashMap::new()),
            Err(KeyMaterialError::EmptyFile { .. })
        ));
        let _ = std::fs::remove_file(empty);

        let invalid = path("invalid");
        std::fs::write(&invalid, [0xff, 0xfe]).unwrap();
        assert!(matches!(
            resolve(KeyMaterialSource::File(&invalid), &HashMap::new()),
            Err(KeyMaterialError::InvalidUtf8 { .. })
        ));
        let _ = std::fs::remove_file(invalid);
    }

    #[test]
    fn resolves_file_bytes_without_trimming() {
        let file = path("exact");
        std::fs::write(&file, b"secret\n").unwrap();
        assert_eq!(
            resolve(KeyMaterialSource::File(&file), &HashMap::new()).unwrap(),
            "secret\n"
        );
        let _ = std::fs::remove_file(file);
    }
}
