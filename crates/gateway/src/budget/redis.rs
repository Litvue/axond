//! Shared budget state in Redis.
//!
//! Reserve and settle each run as one Lua script, so the read-compare-write is
//! atomic on the server: two replicas racing the same key cannot both be
//! admitted against a cap that only covers one. A key's state is two Redis
//! keys, kept in one hash slot by a `{...}` hash tag so the scripts work on a
//! cluster: a counter holding settled spend, and a hash of outstanding
//! reservations.
//!
//! A replica that dies mid-request would otherwise leak its hold forever, so
//! each reservation carries its own deadline and the reserve script reclaims the
//! expired ones before it decides. Holds are therefore self-healing without a
//! sweeper process.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{Script, ScriptInvocation};

use super::{Admission, BudgetError, BudgetKey, BudgetStore, Denial, Reservation, SharedSettings};

const BACKEND: &str = "redis";

/// Admit only if settled spend plus every live hold leaves room for this
/// estimate, and hold it if so. Expired holds are reclaimed first, which is why
/// a crashed replica cannot wedge a budget.
const RESERVE: &str = r#"
local now = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local amount = tonumber(ARGV[4])
local id = ARGV[5]

local held = 0
local reservations = redis.call('HGETALL', KEYS[2])
for i = 1, #reservations, 2 do
  local separator = string.find(reservations[i + 1], ':')
  local value = tonumber(string.sub(reservations[i + 1], 1, separator - 1))
  local expires_at = tonumber(string.sub(reservations[i + 1], separator + 1))
  if expires_at <= now then
    redis.call('HDEL', KEYS[2], reservations[i])
  else
    held = held + value
  end
end

local spent = tonumber(redis.call('GET', KEYS[1]) or '0')
if spent + held + amount > limit then
  return 0
end

redis.call('HSET', KEYS[2], id, amount .. ':' .. (now + ttl_ms))
redis.call('PEXPIRE', KEYS[2], ttl_ms * 2)
return 1
"#;

/// Drop the hold and add the measured spend. Both in one script so a settlement
/// can never release without charging, or charge without releasing.
const SETTLE: &str = r#"
redis.call('HDEL', KEYS[2], ARGV[1])
local actual = tonumber(ARGV[2])
if actual > 0 then
  redis.call('INCRBY', KEYS[1], actual)
end
return 1
"#;

pub struct RedisBudget {
    settings: SharedSettings,
    key_prefix: String,
    /// Reconnects on its own, so a Redis restart does not permanently
    /// fail-closed the gateway.
    connection: ConnectionManager,
    reserve: Script,
    settle: Script,
}

impl RedisBudget {
    /// Connect and prove the server answers, so a wrong URL fails at boot
    /// rather than denying every request once traffic arrives.
    pub async fn connect(
        url: &str,
        key_prefix: String,
        settings: SharedSettings,
    ) -> Result<Self, BudgetError> {
        let client = ::redis::Client::open(url)
            .map_err(|e| BudgetError::invalid(BACKEND, format!("unusable URL: {e}")))?;
        let mut connection = ConnectionManager::new(client).await?;
        ::redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await?;
        Ok(Self {
            settings,
            key_prefix,
            connection,
            reserve: Script::new(RESERVE),
            settle: Script::new(SETTLE),
        })
    }

    /// The two keys one budget occupies. The hash tag pins them to a single
    /// slot, which a script spanning both keys requires on a cluster.
    fn keys(&self, key: &BudgetKey) -> (String, String) {
        let scope = format!("{}:{{{}|{}}}", self.key_prefix, key.namespace, key.subject);
        (format!("{scope}:spent"), format!("{scope}:reservations"))
    }

    fn script<'a>(&self, script: &'a Script, key: &BudgetKey) -> ScriptInvocation<'a> {
        let (spent, reservations) = self.keys(key);
        let mut invocation = script.prepare_invoke();
        invocation.key(spent).key(reservations);
        invocation
    }
}

#[async_trait]
impl BudgetStore for RedisBudget {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission {
        let reservation = Reservation {
            id: Reservation::next_id(),
            estimate_microdollars: estimated_microdollars,
        };
        let ttl_ms = self.settings.reservation_ttl.as_millis() as u64;
        let admitted: Result<i64, ::redis::RedisError> = self
            .script(&self.reserve, key)
            .arg(now_ms())
            .arg(ttl_ms)
            .arg(self.settings.limit_microdollars)
            .arg(estimated_microdollars)
            .arg(&reservation.id)
            .invoke_async(&mut self.connection.clone())
            .await;
        match admitted {
            Ok(1) => Admission::Allowed(reservation),
            Ok(_) => Admission::Denied(Denial::Exceeded),
            Err(e) => self.settings.unavailable.admission(BACKEND, &e),
        }
    }

    /// A settlement that cannot reach Redis leaves the hold to expire on its
    /// own deadline; the alternative — blocking the request path on a retry —
    /// trades a caller's latency for accounting the sweep already recovers.
    /// Fail-open and fail-closed are the same here: nothing is admitted.
    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64) {
        if reservation.id.is_empty() {
            return;
        }
        let settled: Result<i64, ::redis::RedisError> = self
            .script(&self.settle, key)
            .arg(&reservation.id)
            .arg(actual_microdollars)
            .invoke_async(&mut self.connection.clone())
            .await;
        if let Err(e) = settled {
            tracing::error!(
                error = %e,
                namespace = %key.namespace,
                actual_microdollars,
                "budget settlement was lost; the reservation expires on its own deadline"
            );
        }
    }
}

/// Wall-clock milliseconds, which is what the reservation deadlines are in: a
/// deadline is compared against *Redis's* clock in the script, so it must be an
/// absolute time both sides agree on rather than a monotonic instant.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::super::UnavailablePolicy;
    use super::super::tests::key;
    use super::*;

    fn settings(limit: u64) -> SharedSettings {
        SharedSettings {
            limit_microdollars: limit,
            reservation_ttl: Duration::from_secs(300),
            unavailable: UnavailablePolicy::Deny,
        }
    }

    #[test]
    fn a_budgets_keys_share_one_hash_slot() {
        // Constructing the store needs a server, so the key layout is asserted
        // through the same formatting the store uses.
        let scope = format!("axond:budget:{{{}|{}}}", "acme", "subject");
        let spent = format!("{scope}:spent");
        let reservations = format!("{scope}:reservations");
        let tag = |k: &str| {
            let start = k.find('{').expect("hash tag") + 1;
            let end = k.find('}').expect("hash tag");
            k[start..end].to_owned()
        };
        assert_eq!(tag(&spent), tag(&reservations));
        assert_eq!(tag(&spent), "acme|subject");
    }

    #[test]
    fn the_reserve_script_reclaims_expired_holds_before_deciding() {
        assert!(RESERVE.contains("HDEL"));
        assert!(RESERVE.contains("expires_at <= now"));
        // The decision reads spent *and* held, so in-flight requests count.
        assert!(RESERVE.contains("spent + held + amount > limit"));
    }

    /// Exercises the real thing when a server is offered. Skipped (not failed)
    /// otherwise, so the suite stays runnable with no datastore — the same
    /// posture as the gateway itself.
    #[tokio::test]
    async fn two_stores_sharing_one_redis_enforce_a_single_cap() {
        let Ok(url) = std::env::var("AXOND_TEST_REDIS_URL") else {
            return;
        };
        let prefix = format!("axond:test:{}", Reservation::next_id());
        let replica_a = RedisBudget::connect(&url, prefix.clone(), settings(1_000))
            .await
            .expect("connect");
        let replica_b = RedisBudget::connect(&url, prefix, settings(1_000))
            .await
            .expect("connect");
        let k = key();

        let held = replica_a.reserve(&k, 700).await;
        // The second replica sees the first's outstanding hold.
        assert_eq!(
            replica_b.reserve(&k, 700).await,
            Admission::Denied(Denial::Exceeded)
        );

        let Admission::Allowed(reservation) = held else {
            panic!("the first reservation must be admitted");
        };
        replica_a.settle(&k, &reservation, 100).await;
        // Releasing the unused estimate frees it for the other replica.
        let second = replica_b.reserve(&k, 700).await;
        assert!(matches!(second, Admission::Allowed(_)));

        let Admission::Allowed(reservation) = second else {
            unreachable!("just asserted")
        };
        replica_b.settle(&k, &reservation, 700).await;
        // 100 + 700 settled leaves no room for 300.
        assert_eq!(
            replica_a.reserve(&k, 300).await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn an_expired_reservation_stops_counting_against_the_cap() {
        let Ok(url) = std::env::var("AXOND_TEST_REDIS_URL") else {
            return;
        };
        let prefix = format!("axond:test:{}", Reservation::next_id());
        let mut expiring = settings(1_000);
        expiring.reservation_ttl = Duration::from_millis(50);
        let store = RedisBudget::connect(&url, prefix, expiring)
            .await
            .expect("connect");
        let k = key();

        // A replica that died holding this estimate never settles it.
        assert!(matches!(
            store.reserve(&k, 900).await,
            Admission::Allowed(_)
        ));
        assert_eq!(
            store.reserve(&k, 900).await,
            Admission::Denied(Denial::Exceeded)
        );

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(matches!(
            store.reserve(&k, 900).await,
            Admission::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn an_unreachable_server_denies_by_default() {
        // Nothing listens here, so the connection attempt itself fails: the
        // gateway refuses to boot rather than running with an unenforced cap.
        let err = RedisBudget::connect(
            "redis://127.0.0.1:1/",
            "axond:budget".to_owned(),
            settings(1),
        )
        .await
        .err()
        .expect("an unreachable server must fail at boot");
        assert!(matches!(err, BudgetError::Redis(_)), "{err:?}");
    }
}
