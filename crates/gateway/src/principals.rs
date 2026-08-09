use std::sync::Arc;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
}

pub(crate) struct GatewayKeyEntry {
    pub(crate) secret: SecretString,
    pub(crate) caller: InboundKey,
}

pub struct Presented<'a> {
    pub credential: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum PrincipalShapeError {
    #[error(
        "principal shape `{shape}` is owned by both `{first}` and `{second}`, so authority cannot be determined"
    )]
    Duplicate {
        shape: &'static str,
        first: &'static str,
        second: &'static str,
    },
    #[error(
        "principal shapes `{first_shape}` and `{second_shape}` overlap between `{first}` and `{second}`, so authority cannot be determined"
    )]
    Overlap {
        first_shape: &'static str,
        second_shape: &'static str,
        first: &'static str,
        second: &'static str,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum PrincipalStoreError {
    #[allow(dead_code)]
    #[error("principal store unavailable")]
    Unavailable,
}

#[async_trait]
pub trait PrincipalStore: Send + Sync {
    fn name(&self) -> &'static str;
    fn shapes(&self) -> &'static [&'static str];
    async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError>;
}

pub struct ConfigPrincipals {
    inbound_keys: Arc<[GatewayKeyEntry]>,
}

impl ConfigPrincipals {
    pub(crate) fn new(inbound_keys: Arc<[GatewayKeyEntry]>) -> Self {
        Self { inbound_keys }
    }

    pub(crate) fn count(&self) -> usize {
        self.inbound_keys.len()
    }

    #[cfg(test)]
    pub(crate) fn first_secret_debug(&self) -> String {
        format!("{:?}", self.inbound_keys[0].secret)
    }

    pub(crate) fn resolve_static(&self, credential: &str) -> Option<InboundKey> {
        resolve_static_key(&self.inbound_keys, credential).cloned()
    }
}

#[async_trait]
impl PrincipalStore for ConfigPrincipals {
    fn name(&self) -> &'static str {
        "config"
    }

    fn shapes(&self) -> &'static [&'static str] {
        &[]
    }

    async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError> {
        Ok(self.resolve_static(presented.credential))
    }
}

pub struct PrincipalStoreChain {
    stores: Vec<Box<dyn PrincipalStore>>,
    config: ConfigPrincipals,
}

impl PrincipalStoreChain {
    pub(crate) fn new(
        stores: Vec<Box<dyn PrincipalStore>>,
        config: ConfigPrincipals,
    ) -> Result<Self, PrincipalShapeError> {
        let mut declared: Vec<(&'static str, &'static str)> = Vec::new();
        for store in &stores {
            for &shape in store.shapes() {
                for &(first_shape, first) in &declared {
                    if first_shape == shape {
                        return Err(PrincipalShapeError::Duplicate {
                            shape,
                            first,
                            second: store.name(),
                        });
                    }
                    // A longer prefix also matches credentials owned by the
                    // shorter prefix, so equality alone cannot establish
                    // unambiguous authority.
                    if first_shape.starts_with(shape) || shape.starts_with(first_shape) {
                        return Err(PrincipalShapeError::Overlap {
                            first_shape,
                            second_shape: shape,
                            first,
                            second: store.name(),
                        });
                    }
                }
                declared.push((shape, store.name()));
            }
        }
        Ok(Self { stores, config })
    }

    pub(crate) async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError> {
        for store in &self.stores {
            if store
                .shapes()
                .iter()
                .any(|shape| presented.credential.starts_with(shape))
            {
                return store.resolve(presented).await;
            }
        }
        self.config.resolve(presented).await
    }

    pub(crate) fn config_count(&self) -> usize {
        self.config.count()
    }

    pub(crate) fn owner_name(&self, presented: &Presented<'_>) -> &'static str {
        self.stores
            .iter()
            .find(|store| {
                store
                    .shapes()
                    .iter()
                    .any(|shape| presented.credential.starts_with(shape))
            })
            .map_or(self.config.name(), |store| store.name())
    }

    #[cfg(test)]
    pub(crate) fn config_first_secret_debug(&self) -> String {
        self.config.first_secret_debug()
    }
}

fn resolve_static_key<'a>(
    entries: &'a [GatewayKeyEntry],
    credential: &str,
) -> Option<&'a InboundKey> {
    entries
        .iter()
        .find(|entry| {
            constant_time_eq(
                entry.secret.expose_secret().as_bytes(),
                credential.as_bytes(),
            )
        })
        .map(|entry| &entry.caller)
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ShapedStore {
        name: &'static str,
        shapes: &'static [&'static str],
    }

    #[async_trait]
    impl PrincipalStore for ShapedStore {
        fn name(&self) -> &'static str {
            self.name
        }

        fn shapes(&self) -> &'static [&'static str] {
            self.shapes
        }

        async fn resolve(
            &self,
            _presented: &Presented<'_>,
        ) -> Result<Option<InboundKey>, PrincipalStoreError> {
            Ok(None)
        }
    }

    struct FailingStore;

    #[async_trait]
    impl PrincipalStore for FailingStore {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn shapes(&self) -> &'static [&'static str] {
            &["axk_"]
        }

        async fn resolve(
            &self,
            _presented: &Presented<'_>,
        ) -> Result<Option<InboundKey>, PrincipalStoreError> {
            Err(PrincipalStoreError::Unavailable)
        }
    }

    fn config_principals() -> ConfigPrincipals {
        ConfigPrincipals::new(Arc::from(vec![GatewayKeyEntry {
            secret: SecretString::from("static-secret"),
            caller: InboundKey {
                namespace: "platform".to_owned(),
                subject: "AXOND_KEY".to_owned(),
            },
        }]))
    }

    #[test]
    fn duplicate_shapes_are_rejected() {
        let Err(err) = PrincipalStoreChain::new(
            vec![
                Box::new(ShapedStore {
                    name: "first",
                    shapes: &["axk_"],
                }),
                Box::new(ShapedStore {
                    name: "second",
                    shapes: &["axk_"],
                }),
            ],
            config_principals(),
        ) else {
            panic!("duplicate shape ownership must be rejected");
        };

        assert!(matches!(
            err,
            PrincipalShapeError::Duplicate {
                shape: "axk_",
                first: "first",
                second: "second",
            }
        ));
    }

    #[test]
    fn overlapping_shapes_are_rejected_when_shorter_shape_comes_first() {
        let Err(err) = PrincipalStoreChain::new(
            vec![
                Box::new(ShapedStore {
                    name: "short",
                    shapes: &["axk_"],
                }),
                Box::new(ShapedStore {
                    name: "long",
                    shapes: &["axk_v2_"],
                }),
            ],
            config_principals(),
        ) else {
            panic!("overlapping shape ownership must be rejected");
        };

        assert!(matches!(
            err,
            PrincipalShapeError::Overlap {
                first_shape: "axk_",
                second_shape: "axk_v2_",
                first: "short",
                second: "long",
            }
        ));
    }

    #[test]
    fn overlapping_shapes_are_rejected_when_longer_shape_comes_first() {
        let Err(err) = PrincipalStoreChain::new(
            vec![
                Box::new(ShapedStore {
                    name: "long",
                    shapes: &["axk_v2_"],
                }),
                Box::new(ShapedStore {
                    name: "short",
                    shapes: &["axk_"],
                }),
            ],
            config_principals(),
        ) else {
            panic!("overlapping shape ownership must be rejected");
        };

        assert!(matches!(
            err,
            PrincipalShapeError::Overlap {
                first_shape: "axk_v2_",
                second_shape: "axk_",
                first: "long",
                second: "short",
            }
        ));
    }

    #[tokio::test]
    async fn an_owned_shape_does_not_fall_back_to_config() {
        let chain = PrincipalStoreChain::new(
            vec![Box::new(ShapedStore {
                name: "store",
                shapes: &["axk_"],
            })],
            config_principals(),
        )
        .expect("unique shape");

        let presented = Presented {
            credential: "axk_key_static-secret",
        };
        assert!(chain.resolve(&presented).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_owned_shape_error_does_not_fall_back_to_config() {
        let chain = PrincipalStoreChain::new(vec![Box::new(FailingStore)], config_principals())
            .expect("unique shape");
        let presented = Presented {
            credential: "axk_key_static-secret",
        };
        assert!(matches!(
            chain.resolve(&presented).await,
            Err(PrincipalStoreError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn an_unowned_shape_uses_config_principals() {
        let chain = PrincipalStoreChain::new(Vec::new(), config_principals()).expect("valid chain");
        let presented = Presented {
            credential: "static-secret",
        };
        let principal = chain
            .resolve(&presented)
            .await
            .expect("config resolution succeeds")
            .expect("static key resolves");
        assert_eq!(principal.namespace, "platform");
        assert_eq!(principal.subject, "AXOND_KEY");
    }
}
