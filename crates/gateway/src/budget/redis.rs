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

use std::collections::HashSet;
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
/// Value of the layout marker while a migration is carrying spend across. Stamped
/// before the first key moves and replaced by [`LAYOUT_V2`] only when every one
/// has, so a run that dies part-way is visible to both boot directions: the
/// carried subjects hold their spend in the v2 layout and the rest in v1, and
/// neither layout alone is the whole ledger.
const LAYOUT_MIGRATING: &str = "v2-migrating";

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

/// Step one of carrying a v1 counter forward: move the whole counter into a
/// `pending` slot next to it, atomically, and stamp the batch with a sequence
/// number. The v1 spend is gone the instant it is claimed, so nothing can read
/// it twice; it is not yet *added* anywhere, so nothing is lost if this is where
/// the migration dies — a re-run finds the same `pending` and finishes it.
///
/// All three keys carry the v1 `{namespace|subject}` tag, so they share one slot.
/// Returns `seq:amount`, or `false` when there is nothing to claim.
const DRAIN_V1: &str = r#"
local pending = redis.call('GET', KEYS[2])
if pending then
  return pending
end
local spent = redis.call('GET', KEYS[1])
if not spent then
  return false
end
local seq = redis.call('INCR', KEYS[3])
redis.call('DEL', KEYS[1])
local claim = seq .. ':' .. spent
redis.call('SET', KEYS[2], claim)
return claim
"#;

/// Step two: **add** a claimed batch to the v2 counters, at most once. The
/// subject total, the namespace total, and the record of having applied the claim
/// all carry the same `{namespace}` tag, so one script covers them in one slot
/// and the two totals cannot drift apart.
///
/// The namespace total is incremented by the claimed delta, never recomputed from
/// the subject counters: a sum-then-write would clobber a settlement that a live
/// v2 replica committed in between. A repeated (or resumed) apply is a no-op, and
/// a *new* batch from the same v1 key carries a new sequence number, so genuine
/// post-migration spend is added rather than mistaken for a replay.
const APPLY_CLAIM: &str = r#"
local amount = tonumber(ARGV[2])
if redis.call('HSETNX', KEYS[3], ARGV[1], amount) == 0 then
  return 0
end
if amount > 0 then
  redis.call('INCRBY', KEYS[1], amount)
  redis.call('INCRBY', KEYS[2], amount)
end
return amount
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
    if marker.as_deref() == Some(LAYOUT_MIGRATING) {
        return Err(unfinished_migration(key_prefix));
    }
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
    require_no_pending_claims(connection, key_prefix).await?;
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
    if marker.as_deref() == Some(LAYOUT_MIGRATING) {
        // A run that carried some subjects and then failed: their v1 counters are
        // gone, so this configuration would read zero for them and hand each a
        // fresh budget. Only finishing the migration resolves it.
        return Err(unfinished_migration(key_prefix));
    }
    // An unmarked prefix has never been touched by a migration, and so cannot hold
    // an outstanding claim either: the marker is stamped before the first key
    // moves. That keeps this path — the one every deployment without the cap takes
    // — a single `GET`, with no scan of the keyspace.
    Ok(())
}

fn unfinished_migration(key_prefix: &str) -> BudgetError {
    BudgetError::invalid(
        BACKEND,
        format!(
            "`{}` is marked `{LAYOUT_MIGRATING}`: a `axond budget migrate-redis` run did not \
             finish, so some subjects hold their spend in the v2 layout and the rest in v1 and \
             neither configuration can enforce a whole ledger. Re-run `axond budget migrate-redis`, \
             which resumes where it stopped and carries each subject exactly once.",
            layout_key(key_prefix)
        ),
    )
}

/// Claims a migration took but did not finish applying. The v1 counter they came
/// from is already deleted, so nothing else accounts for that spend and no other
/// boot check can see it: `PENDING` matches neither [`legacy_patterns`] nor any v2
/// key. Serving would under-count both the subject and its namespace by whatever
/// is parked here, so it is refused and the migration is re-run to finish them.
async fn require_no_pending_claims(
    connection: &mut ConnectionManager,
    key_prefix: &str,
) -> Result<(), BudgetError> {
    let pending = tally(connection, &format!("{key_prefix}:{{*}}{PENDING}")).await?;
    let Some(example) = pending.example else {
        return Ok(());
    };
    Err(BudgetError::invalid(
        BACKEND,
        format!(
            "{} interrupted migration claim(s) are outstanding under `{key_prefix}` (for example \
             `{example}`): a `axond budget migrate-redis` run was cut short after taking spend off \
             the v1 key and before adding it to the v2 counters, so that spend is in neither. \
             Re-run `axond budget migrate-redis`, which finishes them exactly once.",
            pending.count
        ),
    ))
}

async fn count_legacy_keys(
    connection: &mut ConnectionManager,
    key_prefix: &str,
) -> Result<usize, BudgetError> {
    let mut total = 0;
    for pattern in legacy_patterns(key_prefix) {
        total += tally(connection, &pattern).await?.count;
    }
    Ok(total)
}

/// How many keys match a pattern, and one of them to name in an error.
struct Tally {
    count: usize,
    example: Option<String>,
}

/// [`Tally`] for a pattern, without holding the matches: a boot check wants a
/// count and something to point at, and the keyspace it walks may not be only
/// ours.
async fn tally(connection: &mut ConnectionManager, pattern: &str) -> Result<Tally, BudgetError> {
    let mut tally = Tally {
        count: 0,
        example: None,
    };
    scan(connection, pattern, |key| {
        tally.count += 1;
        tally.example.get_or_insert(key);
    })
    .await?;
    Ok(tally)
}

/// Every key matching a pattern. `SCAN` rather than `KEYS`, so a big keyspace
/// does not block the server; this runs at boot and during migration only.
async fn scan(
    connection: &mut ConnectionManager,
    pattern: &str,
    mut each: impl FnMut(String),
) -> Result<(), BudgetError> {
    let mut cursor = 0u64;
    loop {
        let (next, keys): (u64, Vec<String>) = ::redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(connection)
            .await?;
        for key in keys {
            each(key);
        }
        cursor = next;
        if cursor == 0 {
            return Ok(());
        }
    }
}

/// The keys matching a pattern, collected. Only the migration uses this: it runs
/// with the fleet stopped and needs the keys themselves.
async fn scanned(
    connection: &mut ConnectionManager,
    pattern: &str,
) -> Result<Vec<String>, BudgetError> {
    let mut found = Vec::new();
    scan(connection, pattern, |key| found.push(key)).await?;
    Ok(found)
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
    /// Micro-dollars added to v2 subject ledgers by this run. Zero on a re-run
    /// that finds nothing new, which is what makes the migration idempotent; a
    /// non-zero value on a re-run is spend a stray v1 writer recorded after the
    /// first migration, now recovered.
    pub carried_microdollars: u64,
}

/// Move v1 budget state into the v2 layout the namespace cap needs, then stamp
/// the layout marker the gateway checks at boot.
///
/// Run it with every replica stopped: it deletes the v1 keys it has copied, and
/// a v1 binary still serving traffic would recreate them (which the boot check
/// then refuses).
///
/// It is idempotent, resumable, and **additive**: spend is claimed out of a v1
/// counter atomically and then added to its v2 counterpart at most once, keyed by
/// the claim token, so an interrupted run loses nothing and a re-run double-counts
/// nothing. That is also what makes the recovery the boot fence prescribes exact:
/// spend a stray v1 replica wrote *after* an earlier migration is a new claim, so
/// re-running adds it rather than discarding it. Namespace totals are then
/// recomputed from the subject ledgers, which is their invariant — every
/// settlement charges a subject and its namespace the same amount.
///
/// `namespaces` is the configured `[[namespace]]` id list, which is what the
/// unescaped v1 tags are attributed against. Every v1 key is resolved before any
/// of them is written or deleted, so a key that belongs to no configured
/// namespace — or to more than one, and so cannot be split unambiguously — fails
/// the migration with the state untouched and the layout unmarked.
pub async fn migrate_v1_to_v2(
    url: &str,
    key_prefix: &str,
    namespaces: &[String],
) -> Result<MigrationReport, BudgetError> {
    let client = ::redis::Client::open(url)
        .map_err(|e| BudgetError::invalid(BACKEND, format!("unusable URL: {e}")))?;
    let mut connection = ConnectionManager::new(client).await?;
    let drain = Script::new(DRAIN_V1);
    let apply = Script::new(APPLY_CLAIM);
    let mut report = MigrationReport::default();

    // Attribute every v1 key before touching any of them: a key that cannot be
    // resolved to exactly one configured namespace fails the whole migration,
    // with nothing deleted and the layout marker unset, so the operator fixes
    // the configuration and re-runs rather than discovering half-moved state.
    let spend_keys = scanned(&mut connection, &legacy_patterns(key_prefix)[0]).await?;
    let reservation_keys = scanned(&mut connection, &legacy_patterns(key_prefix)[1]).await?;
    // A claim an earlier run took but did not finish applying. Its `:spent` key
    // is already gone, so only this pattern finds it.
    let claimed_keys = scanned(&mut connection, &format!("{key_prefix}:{{*}}{PENDING}")).await?;
    let mut scopes = Vec::with_capacity(spend_keys.len() + claimed_keys.len());
    for legacy in &spend_keys {
        scopes.push(resolve_legacy_scope(
            key_prefix, legacy, ":spent", namespaces,
        )?);
    }
    for claimed in &claimed_keys {
        scopes.push(resolve_legacy_scope(
            key_prefix, claimed, PENDING, namespaces,
        )?);
    }
    for legacy in &reservation_keys {
        resolve_legacy_scope(key_prefix, legacy, ":reservations", namespaces)?;
    }

    // Every key is now attributed, so the first move is about to happen: mark the
    // layout as mid-migration first. A carried subject leaves no v1 key and no
    // claim behind, so without this a run that failed on a *later* subject would
    // be invisible — and the old layout would read zero for everyone already
    // carried. The marker is what makes a partial run refuse to serve.
    if !scopes.is_empty() || !reservation_keys.is_empty() {
        let _: () = connection
            .set(layout_key(key_prefix), LAYOUT_MIGRATING)
            .await?;
    }

    // Each carry adds its subject's spend to that subject's counter *and* to its
    // namespace's, in one script, so the namespace total is built up additively
    // and is never recomputed from a sum that a live settlement could outrun.
    let mut seen = HashSet::new();
    let mut touched = HashSet::new();
    for key in scopes {
        if !seen.insert((key.namespace.clone(), key.subject.clone())) {
            continue;
        }
        touched.insert(key.namespace.clone());
        let carried = carry_forward(&mut connection, &drain, &apply, key_prefix, &key).await?;
        report.subjects += 1;
        report.carried_microdollars = report.carried_microdollars.saturating_add(carried);
    }

    for legacy in &reservation_keys {
        let _: i64 = connection.del(legacy).await?;
        report.reservation_hashes += 1;
    }
    report.namespaces = touched.len();

    // Every claim has been applied and every v1 counter drained, so the two
    // bookkeeping structures have nothing left to protect and are dropped rather
    // than left in Redis for the lifetime of the deployment.
    //
    // The order matters, and it is the record of applied tokens first: a token is
    // `<subject>#<sequence>`, so deleting a subject's sequence resets the tokens a
    // later stray v1 write would produce, and a *surviving* record of the old ones
    // would then read that write as a replay and drop its spend. Cleared in this
    // order, an interruption anywhere leaves either both or only the record gone,
    // and neither can mistake new spend for old.
    for namespace in &touched {
        let _: i64 = connection
            .del(format!(
                "{}:migration:applied",
                namespace_scope(key_prefix, namespace)
            ))
            .await?;
    }
    for key in &seen {
        let _: i64 = connection
            .del(format!("{key_prefix}:{{{}|{}}}{SEQ}", key.0, key.1))
            .await?;
    }

    let _: () = connection.set(layout_key(key_prefix), LAYOUT_V2).await?;
    Ok(report)
}

/// Suffixes of the two bookkeeping keys a carry uses, both tagged like the v1
/// key they belong to so a script may span them. `PENDING` holds a claim until
/// it has been added to v2; `SEQ` makes every claim from the same v1 key
/// distinct, so a later claim is never mistaken for a replay of an earlier one.
/// Neither matches [`legacy_patterns`], so neither is read as v1 state.
const PENDING: &str = ":migration_pending";
const SEQ: &str = ":migration_seq";

/// Claim whatever a v1 subject counter holds and add it to its v2 counterpart,
/// repeating until the v1 side is empty. Each claim is atomic on the v1 side and
/// applied at most once on the v2 side, so this is safe to interrupt and safe to
/// re-run: an interrupted claim is finished by the next run, and an applied claim
/// is skipped. Returns the micro-dollars actually added.
async fn carry_forward(
    connection: &mut ConnectionManager,
    drain: &Script,
    apply: &Script,
    key_prefix: &str,
    key: &BudgetKey,
) -> Result<u64, BudgetError> {
    let scope = format!("{key_prefix}:{{{}|{}}}", key.namespace, key.subject);
    let pending = format!("{scope}{PENDING}");
    let v2 = v2_keys(key_prefix, key);
    let applied = format!(
        "{}:migration:applied",
        namespace_scope(key_prefix, &key.namespace)
    );
    let mut carried: u64 = 0;
    loop {
        let claim: Option<String> = drain
            .prepare_invoke()
            .key(format!("{scope}:spent"))
            .key(&pending)
            .key(format!("{scope}{SEQ}"))
            .invoke_async(connection)
            .await?;
        let Some(claim) = claim else {
            return Ok(carried);
        };
        let (sequence, amount) = claim.split_once(':').ok_or_else(|| {
            BudgetError::invalid(
                BACKEND,
                format!("the migration claim in `{pending}` is malformed: `{claim}`"),
            )
        })?;
        let amount: i64 = amount.parse().unwrap_or_default();
        let added: i64 = apply
            .prepare_invoke()
            .key(&v2.subject_spent)
            .key(&v2.namespace_spent)
            .key(&applied)
            // Unique per subject *and* per claim, so re-applying is a no-op but
            // a fresh claim is additive.
            .arg(format!("{}#{sequence}", escaped(&key.subject)))
            .arg(amount.max(0))
            .invoke_async(connection)
            .await?;
        let _: i64 = connection.del(&pending).await?;
        carried = carried.saturating_add(added.max(0) as u64);
    }
}

/// The `(namespace, subject)` a v1 key belongs to.
///
/// The v1 tag is `{namespace|subject}` with neither half escaped, so the string
/// alone does not say where the namespace ends: `{team|west|sub}` reads as
/// namespace `team` *or* `team|west`, and guessing would move a tenant's spend
/// to a namespace that is not theirs. So the tag is resolved against the
/// configured namespace ids instead of split, and anything that does not match
/// exactly one of them is an error — the migration refuses rather than guesses.
///
/// Matching one id is not sufficient either: a remaining `|` in the subject means
/// the tag also reads as a longer namespace, and a namespace absent from the
/// config is the ordinary reason a key is left over in the first place. So those
/// are refused as well, which is why a v1 subject id containing a `|` cannot be
/// migrated automatically. Nothing constrains the v2 layout that way: it escapes
/// each identifier into its own key segment, so pipes in either are unambiguous
/// once carried.
fn resolve_legacy_scope(
    key_prefix: &str,
    key: &str,
    suffix: &str,
    namespaces: &[String],
) -> Result<BudgetKey, BudgetError> {
    let ambiguous = |detail: &str| {
        BudgetError::invalid(
            BACKEND,
            format!(
                "the v1 budget key `{key}` cannot be attributed: {detail}. The v1 \
                 `{{namespace|subject}}` tag is unescaped, so it is resolved against the \
                 configured `[[namespace]]` ids rather than split; nothing has been migrated or \
                 deleted. Configure the namespace this key belongs to and re-run the migration \
                 — or, if the tag cannot be read one way whatever is configured, carry this key \
                 into the v2 layout by hand (or delete it, accepting the reset of that ledger)."
            ),
        )
    };
    let opening = format!("{key_prefix}:{{");
    let closing = format!("}}{suffix}");
    let tag = key
        .strip_prefix(&opening)
        .and_then(|rest| rest.strip_suffix(&closing))
        .ok_or_else(|| ambiguous("it is not a `<prefix>:{namespace|subject}` key"))?;

    // By resolved key, not by candidate: a config may legitimately declare the
    // same namespace id twice (see `Config::distinct_namespace_count`), and two
    // candidates that agree on the split are not an ambiguity.
    let mut matched = namespaces
        .iter()
        .filter_map(|namespace| {
            tag.strip_prefix(namespace.as_str())
                .and_then(|rest| rest.strip_prefix('|'))
                .map(|subject| BudgetKey {
                    namespace: namespace.clone(),
                    subject: subject.to_owned(),
                })
        })
        .collect::<Vec<_>>();
    matched.sort_by(|a, b| (&a.namespace, &a.subject).cmp(&(&b.namespace, &b.subject)));
    matched.dedup();
    match matched.len() {
        // One configured id matching is not proof that it owns the key: if the
        // subject it leaves still holds a `|`, the same tag reads as a longer
        // namespace that this config simply does not declare — which is exactly
        // what a namespace removed while its spend was still in Redis looks like.
        // Carrying it would merge one tenant's spend into another's and then
        // delete the only evidence, so it is refused like any other ambiguity.
        1 if !matched[0].subject.contains('|') => Ok(matched.remove(0)),
        1 => Err(ambiguous(&format!(
            "`{}` is a configured namespace id, but the tag's remainder (`{}`) holds a `|` too, so \
             the tag reads equally as a longer namespace this config does not declare",
            matched[0].namespace, matched[0].subject
        ))),
        0 => Err(ambiguous(
            "no configured namespace id is a prefix of its tag",
        )),
        found => Err(ambiguous(&format!(
            "{found} configured namespace ids are prefixes of its tag, so the split between \
             namespace and subject is ambiguous"
        ))),
    }
}

/// The contents of a key's `{...}` hash tag: the first `{` and the first `}`
/// after it, which is the slot Redis itself hashes. v2 keys escape braces out
/// of the identifiers inside the tag, so this reads a whole v2 namespace. Only
/// the tests need it: the code builds tags rather than parsing them, deliberately
/// (parsing one is what made the old migration attribute keys by guesswork).
#[cfg(test)]
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
    use std::sync::Arc;

    use tokio::sync::Barrier;

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

    /// The configured namespaces the migration attributes v1 keys against.
    fn namespaces() -> Vec<String> {
        vec!["acme".to_owned()]
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

    /// A v1 key is attributed by resolving its tag against the configured
    /// namespaces, so a delimiter or a brace inside either identifier does not
    /// move spend to a namespace that does not own it.
    #[test]
    fn a_legacy_key_is_attributed_to_a_configured_namespace() {
        let resolve = |key: &str, namespaces: &[&str]| {
            let namespaces: Vec<String> = namespaces.iter().map(|n| (*n).to_owned()).collect();
            resolve_legacy_scope("axond:budget", key, ":spent", &namespaces)
        };

        // `acme` matching is not proof it owns the key: the same tag reads as
        // namespace `acme|sub`, which no config declares here but a removed one
        // may have. Unattributable, so refused rather than merged into `acme`.
        let unattributable = resolve("axond:budget:{acme|sub|ject}:spent", &["acme"])
            .expect_err("a `|` left in the subject is a second reading of the tag");
        assert!(
            format!("{unattributable}").contains("holds a `|` too"),
            "{unattributable}"
        );

        // A `|` in the *namespace* would have been split off by a first-`|`
        // reading, moving `team|west`'s spend into a namespace called `team`.
        let parsed =
            resolve("axond:budget:{team|west|sub}:spent", &["team|west"]).expect("resolved");
        assert_eq!(parsed.namespace, "team|west");
        assert_eq!(parsed.subject, "sub");

        // Braces in either identifier are part of the tag, not a new tag.
        let parsed = resolve("axond:budget:{ac}me|sub{ject}:spent", &["ac}me"]).expect("resolved");
        assert_eq!(parsed.namespace, "ac}me");
        assert_eq!(parsed.subject, "sub{ject");

        // Both `team` and `team|west` could own it: refuse, do not guess.
        let ambiguous = resolve("axond:budget:{team|west|sub}:spent", &["team", "team|west"])
            .expect_err("two candidate namespaces are ambiguous");
        assert!(format!("{ambiguous}").contains("ambiguous"), "{ambiguous}");

        // The same id declared twice is legal in a config and is not an
        // ambiguity: both candidates agree on where the namespace ends.
        let parsed = resolve("axond:budget:{acme|sub}:spent", &["acme", "acme"])
            .expect("a duplicate namespace id resolves to the one key it describes");
        assert_eq!(parsed.namespace, "acme");
        assert_eq!(parsed.subject, "sub");

        // No configured namespace owns it: refuse.
        for (key, namespaces) in [
            ("axond:budget:{gone|sub}:spent", &["acme"][..]),
            ("axond:budget:layout", &["acme"][..]),
            ("axond:budget:{acme}:spent", &["acme"][..]),
        ] {
            resolve(key, namespaces).expect_err(key);
        }
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
        migrate_v1_to_v2(url, prefix, &namespaces())
            .await
            .expect("migrate");
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
        migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate");
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

        // A barrier, so all forty admissions are in flight at once rather than
        // taking turns: the only thing that may serialize them is the script
        // Redis runs atomically.
        let replica_a = Arc::new(replica_a);
        let replica_b = Arc::new(replica_b);
        let contenders = 40;
        let start = Arc::new(Barrier::new(contenders));
        let mut tasks = Vec::with_capacity(contenders);
        for index in 0..contenders {
            // A distinct subject each, well under the subject cap, so only the
            // namespace cap can deny anyone.
            let key = BudgetKey {
                namespace: "acme".into(),
                subject: format!("subject-{index}"),
            };
            let store = if index % 2 == 0 {
                Arc::clone(&replica_a)
            } else {
                Arc::clone(&replica_b)
            };
            let start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                match store.reserve(&key, 100).await {
                    Admission::Allowed(held) => {
                        store.settle(&key, &held, 100).await;
                        true
                    }
                    Admission::Denied(_) => false,
                }
            }));
        }

        let mut admitted = 0;
        for task in tasks {
            if task.await.expect("no task panicked") {
                admitted += 1;
            }
        }
        // The cap divided by the estimate, exactly: forty concurrent requests
        // across two replicas and ten of them fit.
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

        let report = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate");
        assert_eq!(report.subjects, 2);
        assert_eq!(report.namespaces, 1);
        assert_eq!(report.carried_microdollars, 800);
        // Idempotent: a second run carries nothing and keeps the totals.
        let again = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("re-migrate");
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

    /// Spend a stray v1 replica records *after* a migration must be recovered,
    /// not discarded: the carry adds it to what v2 already holds.
    #[tokio::test]
    async fn a_v1_write_after_the_migration_is_added_not_dropped() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let k = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let v1 = RedisBudget::connect(&url, prefix.clone(), settings(10_000))
            .await
            .expect("connect");
        let Admission::Allowed(held) = v1.reserve(&k, 400).await else {
            panic!("admitted");
        };
        v1.settle(&k, &held, 400).await;
        let first = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate");
        assert_eq!(first.carried_microdollars, 400);

        // A replica that did not get the memo settles another 150 under v1.
        let Admission::Allowed(held) = v1.reserve(&k, 150).await else {
            panic!("admitted");
        };
        v1.settle(&k, &held, 150).await;

        let recovery = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("re-migrate");
        assert_eq!(
            recovery.carried_microdollars, 150,
            "the stray write is added to the 400 already carried"
        );
        assert_eq!(spent(&url, &prefix, &k).await, 550);
        assert_eq!(namespace_spent(&url, &prefix, "acme").await, 550);

        // And running it again adds nothing.
        let again = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("re-migrate");
        assert_eq!(again.carried_microdollars, 0);
        assert_eq!(spent(&url, &prefix, &k).await, 550);
    }

    /// The recovery runs against a live v2 fleet, so it must add its delta to the
    /// namespace total rather than recompute that total: a sum taken before a
    /// concurrent settlement and written after it would erase the settlement.
    #[tokio::test]
    async fn recovering_a_stray_v1_write_keeps_the_v2_spend_beside_it() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let carried_subject = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let v2_only_subject = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        // Migrate a namespace that has spend, so the layout is v2 and the
        // namespace total is non-zero to begin with.
        let v1 = RedisBudget::connect(&url, prefix.clone(), settings(10_000))
            .await
            .expect("connect");
        let Admission::Allowed(held) = v1.reserve(&carried_subject, 400).await else {
            panic!("admitted");
        };
        v1.settle(&carried_subject, &held, 400).await;
        migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate");

        // A cap-enabled replica spends under v2 on a different subject, and a
        // stray v1 replica settles under the old layout on the first one.
        let v2 = RedisBudget::connect(&url, prefix.clone(), namespace_settings(10_000, 10_000))
            .await
            .expect("connect");
        let Admission::Allowed(held) = v2.reserve(&v2_only_subject, 250).await else {
            panic!("admitted");
        };
        v2.settle(&v2_only_subject, &held, 250).await;
        let _: () = ::redis::Client::open(url.as_str())
            .expect("client")
            .get_multiplexed_async_connection()
            .await
            .expect("connect")
            .set(format!("{prefix}:{{acme|first}}:spent"), 150)
            .await
            .expect("stray v1 spend");

        let recovery = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("re-migrate");
        assert_eq!(recovery.carried_microdollars, 150);
        assert_eq!(spent(&url, &prefix, &carried_subject).await, 550);
        assert_eq!(
            spent(&url, &prefix, &v2_only_subject).await,
            250,
            "the v2-only subject is untouched by the carry"
        );
        assert_eq!(
            namespace_spent(&url, &prefix, "acme").await,
            800,
            "400 carried, 250 settled under v2, 150 recovered: the recovery added \
             its delta instead of overwriting the total"
        );
    }

    /// A configured id being a prefix of a tag does not prove it owns the key: the
    /// same tag reads as a longer namespace this config no longer declares, which
    /// is what a namespace removed while its spend was still in Redis looks like.
    /// Carrying it would merge one tenant's spend into another's and delete the
    /// evidence, so it is refused with the keyspace untouched.
    #[tokio::test]
    async fn a_configured_namespace_that_is_only_a_prefix_does_not_claim_the_key() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        // Written by namespace `team|west`, which has since left the config.
        let orphan = format!("{prefix}:{{team|west|abc}}:spent");
        let _: () = connection.set(&orphan, 70).await.expect("orphan write");

        let err = migrate_v1_to_v2(&url, &prefix, &["team".to_owned()])
            .await
            .expect_err("a prefix match over a `|`-bearing remainder must abort the migration");
        assert!(format!("{err}").contains(&orphan), "{err}");

        let left: Option<i64> = connection.get(&orphan).await.expect("read");
        assert_eq!(left, Some(70), "nothing is moved or deleted on an abort");
        assert_eq!(namespace_spent(&url, &prefix, "team").await, 0);
        let marker: Option<String> = connection
            .get(layout_key(&prefix))
            .await
            .expect("read marker");
        assert_eq!(marker, None, "and nothing is stamped");

        // A namespace id containing `|` still migrates, as long as what it leaves
        // is a subject that cannot be read another way.
        let report = migrate_v1_to_v2(&url, &prefix, &["team|west".to_owned()])
            .await
            .expect("the owning namespace resolves it");
        assert_eq!(report.carried_microdollars, 70);
        assert_eq!(namespace_spent(&url, &prefix, "team|west").await, 70);
        assert_eq!(namespace_spent(&url, &prefix, "team").await, 0);
    }

    /// The v2 layout escapes each identifier into its own key segment, so a `|` in
    /// either is only ambiguous in the v1 tag the migration has to read.
    #[tokio::test]
    async fn a_pipe_bearing_subject_is_unambiguous_under_the_v2_layout() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate an empty prefix");
        let store = RedisBudget::connect(&url, prefix.clone(), namespace_settings(1_000, 1_000))
            .await
            .expect("connect");
        let piped = BudgetKey {
            namespace: "acme".into(),
            subject: "sub|ject".into(),
        };
        let plain = BudgetKey {
            namespace: "acme".into(),
            subject: "sub".into(),
        };
        let Admission::Allowed(held) = store.reserve(&piped, 300).await else {
            panic!("within both caps");
        };
        store.settle(&piped, &held, 300).await;

        assert_eq!(spent(&url, &prefix, &piped).await, 300);
        assert_eq!(spent(&url, &prefix, &plain).await, 0, "no key collision");
        assert_eq!(namespace_spent(&url, &prefix, "acme").await, 300);
    }

    /// The sequence keys and the record of applied claims exist to make an
    /// interrupted carry resumable exactly once; once the run is over they are dead
    /// state, and nothing else would ever reclaim them.
    #[tokio::test]
    async fn a_finished_migration_leaves_no_bookkeeping_behind() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let k = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        let _: () = connection
            .set(format!("{prefix}:{{acme|first}}:spent"), 250)
            .await
            .expect("v1 spend");

        migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate");
        assert_eq!(spent(&url, &prefix, &k).await, 250);
        for key in [
            format!("{prefix}:{{acme|first}}{SEQ}"),
            format!("{}:migration:applied", namespace_scope(&prefix, "acme")),
        ] {
            let left: bool = connection.exists(&key).await.expect("read");
            assert!(!left, "`{key}` outlived the migration");
        }

        // And a stray v1 write afterwards is still carried exactly once, which is
        // what the deleted state was protecting: its claim token is fresh because
        // the record of the old ones went with the run that made them.
        let _: () = connection
            .set(format!("{prefix}:{{acme|first}}:spent"), 100)
            .await
            .expect("stray v1 spend");
        let recovery = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("recover");
        assert_eq!(recovery.carried_microdollars, 100);
        assert_eq!(spent(&url, &prefix, &k).await, 350);
        assert_eq!(namespace_spent(&url, &prefix, "acme").await, 350);
    }

    /// A carried subject leaves no v1 key and no claim behind, so a run that fails
    /// on a *later* subject is invisible in the keyspace: the old layout would read
    /// zero for everyone already carried and hand each a fresh budget. The
    /// mid-migration marker is what neither configuration will serve past.
    #[tokio::test]
    async fn a_migration_that_stopped_part_way_serves_neither_layout() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let carried = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let waiting = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };
        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");

        // Two v1 subjects; the migration carried the first and then died, which
        // leaves the marker mid-migration and nothing else to give it away.
        for (key, spend) in [(&carried, 300), (&waiting, 200)] {
            let _: () = connection
                .set(
                    format!("{prefix}:{{{}|{}}}:spent", key.namespace, key.subject),
                    spend,
                )
                .await
                .expect("v1 spend");
        }
        let _: () = connection
            .set(layout_key(&prefix), LAYOUT_MIGRATING)
            .await
            .expect("mark mid-migration");
        let scope = format!("{prefix}:{{acme|first}}");
        let claim: Option<String> = Script::new(DRAIN_V1)
            .prepare_invoke()
            .key(format!("{scope}:spent"))
            .key(format!("{scope}{PENDING}"))
            .key(format!("{scope}{SEQ}"))
            .invoke_async(&mut connection)
            .await
            .expect("claim");
        let (sequence, amount) = claim.as_deref().expect("claimed").split_once(':').unwrap();
        let _: i64 = Script::new(APPLY_CLAIM)
            .prepare_invoke()
            .key(v2_keys(&prefix, &carried).subject_spent)
            .key(v2_keys(&prefix, &carried).namespace_spent)
            .key(format!(
                "{}:migration:applied",
                namespace_scope(&prefix, "acme")
            ))
            .arg(format!("first#{sequence}"))
            .arg(amount.parse::<i64>().unwrap())
            .invoke_async(&mut connection)
            .await
            .expect("apply");
        let _: i64 = connection
            .del(format!("{scope}{PENDING}"))
            .await
            .expect("finish the carry");

        // Neither configuration may serve: the ledger is split across layouts.
        for shared in [namespace_settings(1_000, 1_000), settings(1_000)] {
            let refused = RedisBudget::connect(&url, prefix.clone(), shared)
                .await
                .err()
                .expect("a part-way migration must fail at boot");
            assert!(
                format!("{refused}").contains("did not finish"),
                "the error must name the unfinished migration: {refused}"
            );
        }

        // Resuming carries only what is left, and both subjects end up whole.
        let resumed = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("resume");
        assert_eq!(
            resumed.carried_microdollars, 200,
            "the already-carried subject is not charged again"
        );
        assert_eq!(spent(&url, &prefix, &carried).await, 300);
        assert_eq!(spent(&url, &prefix, &waiting).await, 200);
        assert_eq!(namespace_spent(&url, &prefix, "acme").await, 500);
        RedisBudget::connect(&url, prefix.clone(), namespace_settings(1_000, 1_000))
            .await
            .expect("a finished migration boots");
    }

    /// An interrupted claim holds spend that neither layout accounts for, and the
    /// layout marker is already set once a first migration has succeeded — so
    /// neither configuration may boot while one is outstanding, or the cap would
    /// silently be short by whatever is parked in it.
    #[tokio::test]
    async fn an_outstanding_migration_claim_stops_either_configuration_booting() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let k = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let store = namespace_store(&url, &prefix, 1_000, 1_000).await;
        let Admission::Allowed(held) = store.reserve(&k, 400).await else {
            panic!("migrated state serves as usual");
        };
        store.settle(&k, &held, 400).await;
        drop(store);

        // A stray v1 write, and a recovery run that died between claiming it and
        // adding it to the v2 counters: the v1 key is gone, the claim holds 150.
        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        let scope = format!("{prefix}:{{acme|first}}");
        let _: () = connection
            .set(format!("{scope}:spent"), 150)
            .await
            .expect("stray v1 spend");
        let claim: Option<String> = Script::new(DRAIN_V1)
            .prepare_invoke()
            .key(format!("{scope}:spent"))
            .key(format!("{scope}{PENDING}"))
            .key(format!("{scope}{SEQ}"))
            .invoke_async(&mut connection)
            .await
            .expect("claim");
        assert!(claim.is_some(), "the claim took the v1 counter");

        // The cap-aware configuration must refuse, even though the marker is set
        // and no v1 key is left for the legacy check to find.
        let refused = RedisBudget::connect(&url, prefix.clone(), namespace_settings(1_000, 1_000))
            .await
            .err()
            .expect("an outstanding claim must fail at boot");
        assert!(
            format!("{refused}").contains("migrate-redis"),
            "the error must point at the migration: {refused}"
        );

        // The cap-less configuration is refused by the marker alone, which is why
        // its boot needs no scan: a claim only exists on a prefix a migration has
        // already stamped.
        let refused = RedisBudget::connect(&url, prefix.clone(), settings(1_000))
            .await
            .err()
            .expect("migrated state must fail at boot without the cap");
        assert!(
            format!("{refused}").contains("must stay set"),
            "the marker is what refuses the old layout: {refused}"
        );

        // Finishing the migration is what clears it, and the claim is added to
        // the spend already there rather than replacing it.
        let recovery = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("resume");
        assert_eq!(recovery.carried_microdollars, 150);
        assert_eq!(spent(&url, &prefix, &k).await, 550);
        assert_eq!(namespace_spent(&url, &prefix, "acme").await, 550);
        RedisBudget::connect(&url, prefix.clone(), namespace_settings(1_000, 1_000))
            .await
            .expect("a finished migration boots");
    }

    /// A carry is claimed on the v1 side and applied on the v2 side, and either
    /// step can be the last thing that happens before the process dies. Neither
    /// interruption may lose or duplicate spend.
    #[tokio::test]
    async fn an_interrupted_carry_is_finished_exactly_once() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let k = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        let scope = format!("{prefix}:{{acme|first}}");
        let drain = Script::new(DRAIN_V1);
        let apply = Script::new(APPLY_CLAIM);

        // Died after claiming, before applying: the v1 counter is already gone,
        // so only the claim can account for the spend.
        let _: () = connection
            .set(format!("{scope}:spent"), 300)
            .await
            .expect("v1 spend");
        let claim: Option<String> = drain
            .prepare_invoke()
            .key(format!("{scope}:spent"))
            .key(format!("{scope}{PENDING}"))
            .key(format!("{scope}{SEQ}"))
            .invoke_async(&mut connection)
            .await
            .expect("claim");
        assert_eq!(claim.as_deref(), Some("1:300"));
        let v1_gone: Option<i64> = connection
            .get(format!("{scope}:spent"))
            .await
            .expect("read");
        assert_eq!(v1_gone, None, "a claimed counter is drained atomically");

        let resumed = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("resume");
        assert_eq!(
            resumed.carried_microdollars, 300,
            "the orphaned claim is finished, not lost"
        );
        assert_eq!(spent(&url, &prefix, &k).await, 300);

        // Died after applying, before clearing the claim: replaying it must not
        // charge twice, because the claim token was recorded with the increment.
        let claim = "2:75";
        let _: () = connection
            .set(format!("{scope}{PENDING}"), claim)
            .await
            .expect("orphan claim");
        let _: () = connection
            .set(format!("{scope}{SEQ}"), 2)
            .await
            .expect("sequence");
        for expected in [75, 0] {
            let added: i64 = apply
                .prepare_invoke()
                .key(v2_keys(&prefix, &k).subject_spent)
                .key(v2_keys(&prefix, &k).namespace_spent)
                .key(format!(
                    "{}:migration:applied",
                    namespace_scope(&prefix, "acme")
                ))
                .arg("first#2")
                .arg(75)
                .invoke_async(&mut connection)
                .await
                .expect("apply");
            assert_eq!(added, expected, "a claim is applied at most once");
        }
        let recovered = migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("resume");
        assert_eq!(
            recovered.carried_microdollars, 0,
            "the already-applied claim is not charged again"
        );
        assert_eq!(spent(&url, &prefix, &k).await, 375);
        assert_eq!(namespace_spent(&url, &prefix, "acme").await, 375);
    }

    async fn read(url: &str, key: String) -> u64 {
        let client = ::redis::Client::open(url).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        let value: Option<i64> = connection.get(key).await.expect("read");
        value.unwrap_or_default().max(0) as u64
    }

    async fn spent(url: &str, prefix: &str, key: &BudgetKey) -> u64 {
        read(url, v2_keys(prefix, key).subject_spent).await
    }

    async fn namespace_spent(url: &str, prefix: &str, namespace: &str) -> u64 {
        read(
            url,
            format!("{}:namespace:spent", namespace_scope(prefix, namespace)),
        )
        .await
    }

    /// A v1 key the configured namespaces cannot account for stops the whole
    /// migration: guessing its owner would move one tenant's spend to another,
    /// and a half-applied migration would be worse than none.
    #[tokio::test]
    async fn an_unattributable_v1_key_aborts_the_migration_intact() {
        let Some(url) = crate::test_services::redis_url() else {
            return;
        };
        let prefix = prefix();
        let v1 = RedisBudget::connect(&url, prefix.clone(), settings(1_000))
            .await
            .expect("connect");
        let k = BudgetKey {
            namespace: "acme".into(),
            subject: "first".into(),
        };
        let Admission::Allowed(held) = v1.reserve(&k, 400).await else {
            panic!("the v1 cap admits it");
        };
        v1.settle(&k, &held, 400).await;

        let client = ::redis::Client::open(url.as_str()).expect("client");
        let mut connection = ConnectionManager::new(client).await.expect("connect");
        let orphan = format!("{prefix}:{{retired|sub}}:spent");
        let _: () = connection.set(&orphan, 25).await.expect("orphan write");

        // `retired` is not configured, and `team` alone cannot claim
        // `team|west`'s keys either: both are refusals, not guesses.
        for namespaces in [namespaces(), vec!["acme".to_owned(), "team".to_owned()]] {
            let err = migrate_v1_to_v2(&url, &prefix, &namespaces)
                .await
                .expect_err("an unattributable key must abort the migration");
            assert!(format!("{err}").contains(&orphan), "{err}");
        }

        // Nothing was moved, deleted, or stamped, so the operator can fix the
        // configuration and re-run.
        let v1_spend: Option<i64> = connection
            .get(format!("{prefix}:{{acme|first}}:spent"))
            .await
            .expect("read");
        assert_eq!(v1_spend, Some(400), "the v1 ledger is untouched");
        let orphan_spend: Option<i64> = connection.get(&orphan).await.expect("read");
        assert_eq!(orphan_spend, Some(25));
        let marker: Option<String> = connection
            .get(layout_key(&prefix))
            .await
            .expect("read marker");
        assert_eq!(marker, None, "an aborted migration marks nothing");

        // With the namespace configured, the same state migrates cleanly.
        let report = migrate_v1_to_v2(&url, &prefix, &["acme".to_owned(), "retired".to_owned()])
            .await
            .expect("migrate");
        assert_eq!(report.carried_microdollars, 425);
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
        migrate_v1_to_v2(&url, &prefix, &namespaces())
            .await
            .expect("migrate");
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
