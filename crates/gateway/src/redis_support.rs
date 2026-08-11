use redis::aio::ConnectionManagerConfig;

pub(crate) fn connection_manager_config() -> ConnectionManagerConfig {
    // redis-rs 1.4.1 defaults response_timeout to Some(500 ms)
    // (src/client.rs::DEFAULT_RESPONSE_TIMEOUT). Its internal cancellation
    // can drop a multiplexed waiter and misalign later replies, so callers
    // own the operation deadline and keep the manager's response timeout off.
    ConnectionManagerConfig::new().set_response_timeout(None)
}

#[cfg(test)]
mod tests {
    use super::connection_manager_config;

    #[test]
    fn manager_does_not_add_a_library_response_timeout() {
        assert_eq!(connection_manager_config().response_timeout(), None);
    }
}
