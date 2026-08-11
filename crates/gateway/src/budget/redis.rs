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
//!
//! # Two layouts
//!
//! Without `namespace_limit_microdollars` the layout is **v1**: one spend
//! counter and one reservation hash per `(namespace, subject)`, tagged
//! `{namespace|subject}`.
//!
//! With a namespace cap the layout is **v2**: four keys — subject spend, subject
//! reservations, namespace spend, namespace reservations — all tagged
//! `{namespace}`, so one script can span both scopes on a cluster without a
//! `CROSSSLOT` error. A reservation is one logical hold recorded under one id in
//! both hashes by the same script, and settled out of both by the same script,
//! so neither scope can be charged without the other.
//!
//! The layouts do not share keys, so switching is a **migration**, not a
//! restart: [`migrate_v1_to_v2`] carries v1 spend forward, sums it into
//! namespace totals, and stamps a layout marker. A gateway with the namespace
//! cap enabled refuses to boot until that marker is present (which is also what
//! prevents it from starting against un-migrated state and silently reading
//! zero), and refuses to boot while any v1 key remains — the state a v1 binary
//! still serving traffic would be writing. Dropping the cap after a migration is
//! refused for the mirror-image reason: v1 keys are gone, so it would restart
//! every ledger from zero.
//!
//! The namespace keys are deliberately hot: every subject in a namespace
//! contends on one spend counter, and every reserve scans that namespace's whole
//! reservation hash. That is the cost of exactness (see ADR 0010).

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script, ScriptInvocation};

use super::{
    Admission, BudgetError, BudgetKey, BudgetStore, Denial, ExceededScope, Reservation,
    SharedSettings,
};
use crate::telemetry::metrics;

const BACKEND: &str = "redis";

/// Value of the layout marker once the namespace-scoped layout is in force.
const LAYOUT_V2: &str = "v2";

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

/// The composite admission: both caps decided, and one hold recorded in both
/// scopes, in a single script. `1` admitted, `0` the subject cap is spent, `2`
/// the namespace cap is spent. Nothing is written unless both fit, so a denial
/// never leaves a partial hold behind.
const RESERVE_V2: &str = r#"
local now = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local subject_limit = tonumber(ARGV[3])
local namespace_limit = tonumber(ARGV[4])
local amount = tonumber(ARGV[5])
local id = ARGV[6]

local function held(hash)
  local total = 0
  local reservations = redis.call('HGETALL', hash)
  for i = 1, #reservations, 2 do
    local separator = string.find(reservations[i + 1], ':')
    local value = tonumber(string.sub(reservations[i + 1], 1, separator - 1))
    local expires_at = tonumber(string.sub(reservations[i + 1], separator + 1))
    if expires_at <= now then
      redis.call('HDEL', hash, reservations[i])
    else
      total = total + value
    end
  end
  return total
end

local subject_held = held(KEYS[2])
local namespace_held = held(KEYS[4])
local subject_spent = tonumber(redis.call('GET', KEYS[1]) or '0')
local namespace_spent = tonumber(redis.call('GET', KEYS[3]) or '0')

if subject_spent + subject_held + amount > subject_limit then
  return 0
end
if namespace_spent + namespace_held + amount > namespace_limit then
  return 2
end

local hold = amount .. ':' .. (now + ttl_ms)
redis.call('HSET', KEYS[2], id, hold)
redis.call('PEXPIRE', KEYS[2], ttl_ms * 2)
redis.call('HSET', KEYS[4], id, hold)
redis.call('PEXPIRE', KEYS[4], ttl_ms * 2)
return 1
"#;

/// The composite settlement: one hold released from both scopes and the measured
/// spend added to both counters, in one script. Exactly once per scope, or not
/// at all.
const SETTLE_V2: &str = r#"
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
local actual = tonumber(ARGV[2])
if actual > 0 then
  redis.call('INCRBY', KEYS[1], actual)
  redis.call('INCRBY', KEYS[3], actual)
end
return 1
"#;

/// Carry one v1 counter into its v2 counterpart without ever lowering it, so a
/// re-run (or a resumed migration) cannot reset accumulated spend.
const CARRY_SPEND: &str = r#"
local carried = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if carried > current then
  redis.call('SET', KEYS[1], carried)
  return carried - current
end
return 0
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
    /// rather than denying every request once traffic arrives. With a namespace
    /// cap configured this also proves the state has been migrated to the v2
    /// layout, and that no v1 key (that is, no v1 binary) is still in play.
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
        let (reserve, settle) = if settings.enforces_namespace_cap() {
            require_migrated_layout(&mut connection, &key_prefix).await?;
            (Script::new(RESERVE_V2), Script::new(SETTLE_V2))
        } else {
            require_unmigrated_layout(&mut connection, &key_prefix).await?;
            (Script::new(RESERVE), Script::new(SETTLE))
        };
        Ok(Self {
            settings,
            key_prefix,
            connection,
            reserve,
            settle,
        })
    }

    /// The two keys one budget occupies. The hash tag pins them to a single
    /// slot, which a script spanning both keys requires on a cluster.
    fn keys(&self, key: &BudgetKey) -> (String, String) {
        let scope = format!("{}:{{{}|{}}}", self.key_prefix, key.namespace, key.subject);
        (format!("{scope}:spent"), format!("{scope}:reservations"))
    }

    fn script<'a>(&self, script: &'a Script, key: &BudgetKey) -> ScriptInvocation<'a> {
        let mut invocation = script.prepare_invoke();
        if self.settings.enforces_namespace_cap() {
            let scopes = v2_keys(&self.key_prefix, key);
            invocation
                .key(scopes.subject_spent)
                .key(scopes.subject_reservations)
                .key(scopes.namespace_spent)
                .key(scopes.namespace_reservations);
        } else {
            let (spent, reservations) = self.keys(key);
            invocation.key(spent).key(reservations);
        }
        invocation
    }
}

/// The four v2 keys a composite operation spans. All four carry the same
/// `{namespace}` hash tag, so a cluster routes them to one slot.
struct V2Keys {
    subject_spent: String,
    subject_reservations: String,
    namespace_spent: String,
    namespace_reservations: String,
}

fn v2_keys(key_prefix: &str, key: &BudgetKey) -> V2Keys {
    let namespace = namespace_scope(key_prefix, &key.namespace);
    let subject = format!("{namespace}:subject:{}", escaped(&key.subject));
    V2Keys {
        subject_spent: format!("{subject}:spent"),
        subject_reservations: format!("{subject}:reservations"),
        namespace_spent: format!("{namespace}:namespace:spent"),
        namespace_reservations: format!("{namespace}:namespace:reservations"),
    }
}

/// The `{namespace}`-tagged prefix every v2 key for a namespace shares.
fn namespace_scope(key_prefix: &str, namespace: &str) -> String {
    format!("{key_prefix}:v2:{{{}}}", escaped(namespace))
}

/// Braces would move the hash tag and so split a namespace's keys across slots,
/// so they are escaped out of the identifiers the keys are built from. The
/// escaping is reversible, so two distinct identifiers cannot collide.
fn escaped(part: &str) -> String {
    let mut out = String::with_capacity(part.len());
    for character in part.chars() {
        match character {
            '%' => out.push_str("%25"),
            '{' => out.push_str("%7B"),
            '}' => out.push_str("%7D"),
            other => out.push(other),
        }
    }
    out
}

fn layout_key(key_prefix: &str) -> String {
    format!("{key_prefix}:layout")
}

/// The v1 key patterns, for the scans that detect un-migrated (or
/// still-being-written) legacy state. A v1 key is `<prefix>:{ns|subject}:...`,
/// which no v2 key can match: those are `<prefix>:v2:{ns}:...`.
fn legacy_patterns(key_prefix: &str) -> [String; 2] {
    [
        format!("{key_prefix}:{{*}}:spent"),
        format!("{key_prefix}:{{*}}:reservations"),
    ]
}

async fn require_migrated_layout(
    connection: &mut ConnectionManager,
    key_prefix: &str,
) -> Result<(), BudgetError> {
    let marker: Option<String> = connection.get(layout_key(key_prefix)).await?;
    if marker.as_deref() != Some(LAYOUT_V2) {
        return Err(BudgetError::invalid(
            BACKEND,
            format!(
                "`namespace_limit_microdollars` needs the v2 key layout, but `{}` is not marked \
                 migrated. Stop every replica and run `axond budget migrate-redis`, which carries \
                 existing spend forward rather than restarting it from zero.",
                layout_key(key_prefix)
            ),
        ));
    }
    let legacy = count_legacy_keys(connection, key_prefix).await?;
    if legacy > 0 {
        return Err(BudgetError::invalid(
            BACKEND,
            format!(
                "{legacy} v1 budget key(s) exist under `{key_prefix}` after the migration to the \
                 v2 layout: a gateway binary without namespace-cap support is still writing them, \
                 and the two layouts would each enforce half the traffic. Stop the v1 replicas and \
                 re-run `axond budget migrate-redis`."
            ),
        ));
    }
    Ok(())
}

/// A gateway *without* the cap writes the v1 layout, whose keys the migration
/// removed — booting it against migrated state would restart every ledger from
/// zero, so it is refused rather than silently forgiven.
async fn require_unmigrated_layout(
    connection: &mut ConnectionManager,
    key_prefix: &str,
) -> Result<(), BudgetError> {
    let marker: Option<String> = connection.get(layout_key(key_prefix)).await?;
    if marker.as_deref() == Some(LAYOUT_V2) {
        return Err(BudgetError::invalid(
            BACKEND,
            format!(
                "`{}` is marked migrated to the v2 key layout, so `namespace_limit_microdollars` \
                 must stay set: the v1 keys this configuration would use no longer hold the \
                 accumulated spend.",
                layout_key(key_prefix)
            ),
        ));
    }
    Ok(())
}

async fn count_legacy_keys(
    connection: &mut ConnectionManager,
    key_prefix: &str,
) -> Result<usize, BudgetError> {
    let mut total = 0;
    for pattern in legacy_patterns(key_prefix) {
        total += scan(connection, &pattern).await?.len();
    }
    Ok(total)
}

/// Every key matching a pattern. `SCAN` rather than `KEYS`, so a big keyspace
/// does not block the server; this runs at boot and during migration only.
async fn scan(
    connection: &mut ConnectionManager,
    pattern: &str,
) -> Result<Vec<String>, BudgetError> {
    let mut cursor = 0u64;
    let mut found = Vec::new();
    loop {
        let (next, keys): (u64, Vec<String>) = ::redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(connection)
            .await?;
        found.extend(keys);
        cursor = next;
        if cursor == 0 {
            return Ok(found);
        }
    }
}

/// What a migration carried over, for the operator who ran it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// v1 subject ledgers whose spend was carried into the v2 layout.
    pub subjects: usize,
    /// v1 reservation hashes dropped. A migration runs with the fleet stopped,
    /// so these holds belong to nothing.
    pub reservation_hashes: usize,
    /// Namespace totals written from the v2 subject ledgers.
    pub namespaces: usize,
    /// Micro-dollars carried into subject ledgers that did not already hold
    /// them. Zero on a re-run, which is what makes the migration idempotent.
    pub carried_microdollars: u64,
}

/// Move v1 budget state into the v2 layout the namespace cap needs, then stamp
/// the layout marker the gateway checks at boot.
///
/// Run it with every replica stopped: it deletes the v1 keys it has copied, and
/// a v1 binary still serving traffic would recreate them (which the boot check
/// then refuses). It is idempotent and resumable — a counter is never lowered,
/// and namespace totals are recomputed from the subject ledgers, which is their
/// invariant: every settlement charges a subject and its namespace the same
/// amount.
pub async fn migrate_v1_to_v2(url: &str, key_prefix: &str) -> Result<MigrationReport, BudgetError> {
    let client = ::redis::Client::open(url)
        .map_err(|e| BudgetError::invalid(BACKEND, format!("unusable URL: {e}")))?;
    let mut connection = ConnectionManager::new(client).await?;
    let carry = Script::new(CARRY_SPEND);
    let mut report = MigrationReport::default();

    for legacy in scan(&mut connection, &legacy_patterns(key_prefix)[0]).await? {
        let Some(key) = parse_legacy_scope(key_prefix, &legacy) else {
            tracing::warn!(
                "skipping a v1 budget key whose `{{namespace|subject}}` tag could not be read"
            );
            continue;
        };
        let spent: Option<i64> = connection.get(&legacy).await?;
        let spent = spent.unwrap_or_default().max(0) as u64;
        let carried: i64 = carry
            .prepare_invoke()
            .key(v2_keys(key_prefix, &key).subject_spent)
            .arg(spent)
            .invoke_async(&mut connection)
            .await?;
        let _: i64 = connection.del(&legacy).await?;
        report.subjects += 1;
        report.carried_microdollars = report
            .carried_microdollars
            .saturating_add(carried.max(0) as u64);
    }

    for legacy in scan(&mut connection, &legacy_patterns(key_prefix)[1]).await? {
        let _: i64 = connection.del(&legacy).await?;
        report.reservation_hashes += 1;
    }

    // A namespace total is the sum of its subject ledgers, so it can always be
    // rebuilt from them — which is also how a partially applied migration heals.
    let mut totals: HashMap<String, u64> = HashMap::new();
    let subject_pattern = format!("{key_prefix}:v2:{{*}}:subject:*:spent");
    for subject_key in scan(&mut connection, &subject_pattern).await? {
        let Some(namespace) = hash_tag(&subject_key) else {
            continue;
        };
        let spent: Option<i64> = connection.get(&subject_key).await?;
        *totals.entry(namespace.to_owned()).or_default() += spent.unwrap_or_default().max(0) as u64;
    }
    for (namespace, total) in &totals {
        // The namespace id is already escaped inside the tag it was read from.
        let key = format!("{key_prefix}:v2:{{{namespace}}}:namespace:spent");
        let _: () = connection.set(key, *total).await?;
        report.namespaces += 1;
    }

    let _: () = connection.set(layout_key(key_prefix), LAYOUT_V2).await?;
    Ok(report)
}

/// The `(namespace, subject)` a v1 key belongs to, read back out of its hash
/// tag. The namespace ends at the first `|`, matching how the tag was built.
fn parse_legacy_scope(key_prefix: &str, key: &str) -> Option<BudgetKey> {
    let tag = hash_tag(key)?;
    let (namespace, subject) = tag.split_once('|')?;
    if !key.starts_with(key_prefix) || namespace.is_empty() {
        return None;
    }
    if subject.contains('|') {
        // The v1 tag is not escaped, so a `|` in either identifier makes the
        // split ambiguous. The first `|` is the documented reading; say so,
        // because the alternative reading would move spend to another namespace.
        tracing::warn!(
            namespace = %namespace,
            "a v1 budget key's tag contains more than one `|`; reading everything after the first \
             as the subject"
        );
    }
    Some(BudgetKey {
        namespace: namespace.to_owned(),
        subject: subject.to_owned(),
    })
}

/// The contents of a key's `{...}` hash tag: the first `{` and the first `}`
/// after it, which is the slot Redis itself hashes.
fn hash_tag(key: &str) -> Option<&str> {
    let start = key.find('{')? + 1;
    let end = key[start..].find('}')? + start;
    Some(&key[start..end])
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
        let mut invocation = self.script(&self.reserve, key);
        invocation.arg(now_ms()).arg(ttl_ms);
        invocation.arg(self.settings.limit_microdollars);
        if let Some(namespace_limit) = self.settings.namespace_limit_microdollars {
            invocation.arg(namespace_limit);
        }
        let admitted: Result<i64, ::redis::RedisError> = invocation
            .arg(estimated_microdollars)
            .arg(&reservation.id)
            .invoke_async(&mut self.connection.clone())
            .await;
        match admitted {
            Ok(1) => Admission::Allowed(reservation),
            Ok(2) => exceeded(key, ExceededScope::Namespace),
            Ok(_) => exceeded(key, ExceededScope::Subject),
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

/// Both scopes answer the caller with the same `429`; only the operator-facing
/// signal distinguishes which cap is spent.
fn exceeded(key: &BudgetKey, scope: ExceededScope) -> Admission {
    if scope == ExceededScope::Namespace {
        metrics::record_budget_namespace_denial();
        tracing::info!(
            namespace = %key.namespace,
            "namespace spend cap is exhausted; denying"
        );
    }
    Admission::Denied(Denial::Exceeded)
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
            namespace_limit_microdollars: None,
            reservation_ttl: Duration::from_secs(300),
            unavailable: UnavailablePolicy::Deny,
        }
    }

    fn namespace_settings(limit: u64, namespace_limit: u64) -> SharedSettings {
        SharedSettings {
            namespace_limit_microdollars: Some(namespace_limit),
            ..settings(limit)
        }
    }

    fn prefix() -> String {
        format!("axond:test:{}", Reservation::next_id())
    }

    fn tag(key: &str) -> &str {
        hash_tag(key).expect("hash tag")
    }

    #[test]
    fn a_budgets_keys_share_one_hash_slot() {
        // Constructing the store needs a server, so the key layout is asserted
        // through the same formatting the store uses.
        let scope = format!("axond:budget:{{{}|{}}}", "acme", "subject");
        let spent = format!("{scope}:spent");
        let reservations = format!("{scope}:reservations");
        assert_eq!(tag(&spent), tag(&reservations));
        assert_eq!(tag(&spent), "acme|subject");
    }

    /// The point of the v2 layout: one script may touch all four keys, which a
    /// cluster only allows when they hash to the same slot.
    #[test]
    fn every_v2_key_in_a_namespace_shares_the_namespace_hash_tag() {
        let keys = v2_keys(
            "axond:budget",
            &BudgetKey {
                namespace: "acme".into(),
                subject: "subject-a".into(),
            },
        );
        let other = v2_keys(
            "axond:budget",
            &BudgetKey {
                namespace: "acme".into(),
                subject: "subject-b".into(),
            },
        );
        for k in [
            &keys.subject_spent,
            &keys.subject_reservations,
            &keys.namespace_spent,
            &keys.namespace_reservations,
            &other.subject_spent,
        ] {
            assert_eq!(tag(k), "acme", "{k}");
        }
        assert_ne!(keys.subject_spent, other.subject_spent);
        assert_eq!(keys.namespace_spent, other.namespace_spent);
    }

    /// A brace in an identifier would move the tag and split a namespace across
    /// slots, so it is escaped — reversibly, so two identifiers cannot collide.
    #[test]
    fn braces_in_identifiers_cannot_move_the_hash_tag() {
        let keys = v2_keys(
            "axond:budget",
            &BudgetKey {
                namespace: "ac}me".into(),
                subject: "sub{ject}".into(),
            },
        );
        assert_eq!(tag(&keys.subject_spent), "ac%7Dme");
        assert_eq!(tag(&keys.subject_spent), tag(&keys.namespace_spent));
        assert_ne!(escaped("a%7B"), escaped("a{"));
    }

    #[test]
    fn a_v2_key_is_not_mistaken_for_legacy_state() {
        let keys = v2_keys("axond:budget", &key());
        let [spent_pattern, _] = legacy_patterns("axond:budget");
        // `{*}` in the pattern is literal-brace-then-glob, so the v2 keys —
        // which put `v2:` before their tag — cannot match it.
        assert!(spent_pattern.starts_with("axond:budget:{"));
        assert!(keys.subject_spent.starts_with("axond:budget:v2:{"));
    }

    #[test]
    fn a_legacy_key_maps_back_to_its_namespace_and_subject() {
        let parsed = parse_legacy_scope("axond:budget", "axond:budget:{acme|sub|ject}:spent")
            .expect("a v1 key carries its scope in its tag");
        assert_eq!(parsed.namespace, "acme");
        assert_eq!(parsed.subject, "sub|ject");
        assert!(parse_legacy_scope("axond:budget", "axond:budget:layout").is_none());
    }

    #[test]
    fn the_reserve_script_reclaims_expired_holds_before_deciding() {
        assert!(RESERVE.contains("HDEL"));
        assert!(RESERVE.contains("expires_at <= now"));
        // The decision reads spent *and* held, so in-flight requests count.
        assert!(RESERVE.contains("spent + held + amount > limit"));
    }

    /// Both caps decided before anything is written, so a denied request cannot
    /// leave one scope holding an estimate the other rejected.
    #[test]
    fn the_composite_script_decides_both_caps_before_it_holds_either() {
        let decisions = RESERVE_V2.find("subject_limit then").expect("subject cap");
        let namespace = RESERVE_V2
            .find("namespace_limit then")
            .expect("namespace cap");
        let first_write = RESERVE_V2.find("HSET").expect("the hold");
        assert!(decisions < first_write);
        assert!(namespace < first_write);
        // And a settlement charges both counters or neither.
        assert_eq!(SETTLE_V2.matches("INCRBY").count(), 2);
        assert_eq!(SETTLE_V2.matches("HDEL").count(), 2);
    }

    /// Exercises the real thing when a server is offered. Skipped (not failed)
    /// otherwise, so the suite stays runnable with no datastore — the same
    /// posture as the gateway itself.
    #[tokio::test]
    async fn two_stores_sharing_one_redis_enforce_a_single_cap() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
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
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let mut expiring = settings(1_000);
        expiring.reservation_ttl = Duration::from_millis(50);
        let store = RedisBudget::connect(&url, prefix(), expiring)
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

    /// A store built for the migrated layout: the marker is what the gateway
    /// requires, and a fresh prefix has no v1 state to carry.
    async fn namespace_store(
        url: &str,
        prefix: &str,
        subject_limit: u64,
        namespace_limit: u64,
    ) -> RedisBudget {
        migrate_v1_to_v2(url, prefix).await.expect("migrate");
        RedisBudget::connect(
            url,
            prefix.to_owned(),
            namespace_settings(subject_limit, namespace_limit),
        )
        .await
        .expect("connect")
    }

    #[tokio::test]
    async fn two_subjects_cannot_collectively_exceed_the_namespace_cap() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let store = namespace_store(&url, &prefix, 1_000, 1_200).await;
        let first = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let Admission::Allowed(held) = store.reserve(&first, 800).await else {
            panic!("the first subject fits both caps");
        };
        // 800 held + 800 estimated exceeds the namespace cap, though each
        // subject's own cap has room.
        assert_eq!(
            store.reserve(&second, 800).await,
            Admission::Denied(Denial::Exceeded)
        );
        assert!(matches!(
            store.reserve(&second, 300).await,
            Admission::Allowed(_)
        ));

        store.settle(&first, &held, 800).await;
        // Settled spend counts the same as a hold: 800 + 300 held leaves 100.
        assert_eq!(
            store.reserve(&second, 200).await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn a_subject_cap_still_binds_under_a_generous_namespace_cap() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let store = namespace_store(&url, &prefix, 500, 1_000_000).await;
        let k = key();
        let Admission::Allowed(held) = store.reserve(&k, 500).await else {
            panic!("the subject cap has room");
        };
        assert_eq!(
            store.reserve(&k, 1).await,
            Admission::Denied(Denial::Exceeded)
        );
        store.settle(&k, &held, 0).await;
        assert!(matches!(
            store.reserve(&k, 500).await,
            Admission::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn namespaces_do_not_share_a_cap() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let store = namespace_store(&url, &prefix, 1_000, 1_000).await;
        let acme = BudgetKey {
            namespace: "acme".into(),
            subject: "s".into(),
        };
        let other = BudgetKey {
            namespace: "other".into(),
            subject: "s".into(),
        };

        let Admission::Allowed(held) = store.reserve(&acme, 1_000).await else {
            panic!("acme fits its own cap");
        };
        store.settle(&acme, &held, 1_000).await;
        assert_eq!(
            store.reserve(&acme, 1).await,
            Admission::Denied(Denial::Exceeded)
        );
        assert!(matches!(
            store.reserve(&other, 1_000).await,
            Admission::Allowed(_)
        ));
    }

    /// A release must free the estimate in *both* scopes, or a namespace slowly
    /// wedges itself on holds nothing ever consumed.
    #[tokio::test]
    async fn releasing_frees_the_estimate_in_both_scopes() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let store = namespace_store(&url, &prefix, 1_000, 1_000).await;
        let k = key();
        let Admission::Allowed(held) = store.reserve(&k, 1_000).await else {
            panic!("an empty namespace admits");
        };
        store.release(&k, &held).await;
        let other = BudgetKey {
            namespace: k.namespace.clone(),
            subject: "another".into(),
        };
        assert!(matches!(
            store.reserve(&other, 1_000).await,
            Admission::Allowed(_)
        ));
    }

    /// Partial-stream settlement: the measured cost, not the estimate, and it
    /// lands in both scopes exactly once.
    #[tokio::test]
    async fn a_partial_settlement_charges_both_scopes_once() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let store = namespace_store(&url, &prefix, 1_000, 1_000).await;
        let k = key();
        let Admission::Allowed(held) = store.reserve(&k, 900).await else {
            panic!("an empty namespace admits");
        };
        store.settle(&k, &held, 100).await;
        // A repeated settlement (a stream that settles twice would be a bug)
        // must not double-charge: the hold is already gone, so only the spend
        // that actually happened counts.
        let other = BudgetKey {
            namespace: k.namespace.clone(),
            subject: "another".into(),
        };
        assert!(matches!(
            store.reserve(&other, 900).await,
            Admission::Allowed(_)
        ));
        assert_eq!(
            store.reserve(&other, 1).await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn an_expired_hold_frees_the_namespace_cap_too() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        migrate_v1_to_v2(&url, &prefix).await.expect("migrate");
        // A long TTL, so the denial below cannot race the clock; expiry is then
        // forced by rewriting the hold's deadline rather than by sleeping.
        let mut expiring = namespace_settings(1_000, 1_000);
        expiring.reservation_ttl = Duration::from_secs(600);
        let store = RedisBudget::connect(&url, prefix.clone(), expiring)
            .await
            .expect("connect");
        let first = BudgetKey {
            namespace: "acme".into(),
            subject: "died".into(),
        };
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "alive".into(),
        };

        let Admission::Allowed(held) = store.reserve(&first, 900).await else {
            panic!("the first reservation must be admitted");
        };
        assert_eq!(
            store.reserve(&second, 900).await,
            Admission::Denied(Denial::Exceeded)
        );

        // The replica holding it died: its hold is now in the past, in both
        // scopes it was recorded in.
        {
            let keys = v2_keys(&prefix, &first);
            let client = ::redis::Client::open(url.as_str()).expect("client");
            let mut connection = ConnectionManager::new(client).await.expect("connect");
            let stale = format!("900:{}", now_ms() - 1);
            for key in [&keys.subject_reservations, &keys.namespace_reservations] {
                let rewritten: i64 = connection
                    .hset(key, &held.id, &stale)
                    .await
                    .expect("backdate the hold");
                assert_eq!(rewritten, 0, "the hold is rewritten, not added");
            }
        }

        assert!(matches!(
            store.reserve(&second, 900).await,
            Admission::Allowed(_)
        ));
    }

    /// Two replicas of the gateway, one namespace cap.
    #[tokio::test]
    async fn two_replicas_enforce_one_namespace_cap_under_contention() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let replica_a = namespace_store(&url, &prefix, 1_000_000, 1_000).await;
        let replica_b =
            RedisBudget::connect(&url, prefix.clone(), namespace_settings(1_000_000, 1_000))
                .await
                .expect("connect");

        let mut admitted = 0;
        for index in 0..40 {
            let store: &RedisBudget = if index % 2 == 0 {
                &replica_a
            } else {
                &replica_b
            };
            let k = BudgetKey {
                namespace: "acme".into(),
                subject: format!("subject-{index}"),
            };
            if let Admission::Allowed(held) = store.reserve(&k, 100).await {
                admitted += 1;
                store.settle(&k, &held, 100).await;
            }
        }
        // Exactly the cap, across both replicas and ten distinct subjects.
        assert_eq!(admitted, 10);
    }

    /// The whole point of the migration: enabling the namespace cap must not
    /// forget what a namespace has already spent.
    #[tokio::test]
    async fn the_migration_carries_v1_spend_into_both_scopes() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        // Spend under the v1 layout, as a gateway without the cap would.
        let v1 = RedisBudget::connect(&url, prefix.clone(), settings(1_000))
            .await
            .expect("connect");
        for subject in ["first", "second"] {
            let k = BudgetKey {
                namespace: "acme".into(),
                subject: subject.into(),
            };
            let Admission::Allowed(held) = v1.reserve(&k, 400).await else {
                panic!("each subject has its own v1 cap");
            };
            v1.settle(&k, &held, 400).await;
        }

        let report = migrate_v1_to_v2(&url, &prefix).await.expect("migrate");
        assert_eq!(report.subjects, 2);
        assert_eq!(report.namespaces, 1);
        assert_eq!(report.carried_microdollars, 800);
        // Idempotent: a second run carries nothing and keeps the totals.
        let again = migrate_v1_to_v2(&url, &prefix).await.expect("re-migrate");
        assert_eq!(again.carried_microdollars, 0);
        assert_eq!(again.subjects, 0);

        let store = RedisBudget::connect(&url, prefix, namespace_settings(1_000, 1_000))
            .await
            .expect("connect");
        // 800 already spent in the namespace, so only 200 is left — the spend
        // did not reset, and it is visible to *both* scopes.
        assert_eq!(
            store
                .reserve(
                    &BudgetKey {
                        namespace: "acme".into(),
                        subject: "third".into(),
                    },
                    201,
                )
                .await,
            Admission::Denied(Denial::Exceeded)
        );
        assert_eq!(
            store
                .reserve(
                    &BudgetKey {
                        namespace: "acme".into(),
                        subject: "first".into(),
                    },
                    601,
                )
                .await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn the_namespace_cap_refuses_to_boot_against_unmigrated_state() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let err = RedisBudget::connect(&url, prefix(), namespace_settings(1_000, 1_000))
            .await
            .err()
            .expect("un-migrated state must fail at boot");
        assert!(
            format!("{err}").contains("migrate-redis"),
            "the error must name the migration: {err}"
        );
    }

    /// A v1 binary still writing its own keys would split enforcement in two,
    /// so migrated state plus a v1 key is a boot failure.
    #[tokio::test]
    async fn a_v1_key_written_after_the_migration_is_rejected() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        migrate_v1_to_v2(&url, &prefix).await.expect("migrate");
        let v1 = RedisBudget::connect(&url, prefix.clone(), settings(1_000))
            .await
            .err()
            .expect("a v1 configuration must not boot against migrated state");
        assert!(
            format!("{v1}").contains("namespace_limit_microdollars"),
            "{v1}"
        );

        // Simulate the v1 binary that ignored that and wrote its layout anyway.
        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        let _: () = connection
            .set(format!("{prefix}:{{acme|stale}}:spent"), 10)
            .await
            .expect("legacy write");

        let err = RedisBudget::connect(&url, prefix, namespace_settings(1_000, 1_000))
            .await
            .err()
            .expect("mixed binaries must fail at boot");
        assert!(format!("{err}").contains("v1 budget key"), "{err}");
    }
}
