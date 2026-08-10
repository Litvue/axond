fn resolve(variable: &str, value: Option<String>, require_services: bool) -> Option<String> {
    match value {
        Some(value) => Some(value),
        None if require_services => panic!(
            "{variable} is required when AXOND_TEST_REQUIRE_SERVICES=1; \
             CI requires the test service to be available"
        ),
        None => None,
    }
}

pub(crate) fn redis_url() -> Option<String> {
    resolve(
        "AXOND_TEST_REDIS_URL",
        std::env::var("AXOND_TEST_REDIS_URL").ok(),
        std::env::var("AXOND_TEST_REQUIRE_SERVICES").as_deref() == Ok("1"),
    )
}

pub(crate) fn postgres_dsn() -> Option<String> {
    resolve(
        "AXOND_TEST_POSTGRES_DSN",
        std::env::var("AXOND_TEST_POSTGRES_DSN").ok(),
        std::env::var("AXOND_TEST_REQUIRE_SERVICES").as_deref() == Ok("1"),
    )
}

#[cfg(test)]
mod tests {
    use super::resolve;

    #[test]
    fn missing_service_is_optional_by_default() {
        assert_eq!(resolve("SERVICE_URL", None, false), None);
    }

    #[test]
    fn configured_service_is_returned_without_requirement() {
        assert_eq!(
            resolve("SERVICE_URL", Some("http://localhost".to_owned()), false),
            Some("http://localhost".to_owned())
        );
    }

    #[test]
    #[should_panic(expected = "SERVICE_URL is required")]
    fn missing_required_service_panics() {
        resolve("SERVICE_URL", None, true);
    }
}
