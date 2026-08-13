//! Tenant isolation, end to end through the shipped binary (#225).
//!
//! Two tenants share one process. The property under test is that neither one
//! can see, invoke, be served by, or spend against anything of the other's, and
//! that the single case where a caller *is* served by a credential it does not
//! own — platform fallback — is a decision the config made and the usage record
//! names.
//!
//! Black-box on purpose. Every assertion is made from outside: the catalogue a
//! caller is served, the status a refused request gets, the bytes the provider
//! received, and the rows that reached Postgres. A gateway that leaked across
//! tenants but accounted for it correctly in its own internals would still fail
//! here.
//!
//! The layers *below* this one are covered where they live, and this suite
//! deliberately does not restate them:
//!
//! - the domain refuses to build a cross-tenant reference, and hydration
//!   refuses one that storage was made to hold
//!   (`desired_state::revision`, `backends::control_plane::hydration`);
//! - Postgres itself refuses a stored edge across a tenant boundary
//!   (`backends::control_plane::postgres`).
//!
//! What is *not* covered yet, and why, is recorded in
//! `docs/security/tenant-isolation-evidence.md`: the assertions that need
//! durable principals and RBAC, row-level security, or the admin surface cannot
//! be written against a runtime that has none of them, and a test that pretends
//! otherwise is worse than an absent one.

mod support;

use serde_json::{Value, json};
use support::client;
use support::tenancy::{
    ACME, ALL_ALIASES, Deployment, Durability, FALLBACK_KEY, FALLBACK_NAMESPACE, GLOBEX,
    PLATFORM_ALIAS, PLATFORM_CREDENTIAL_ID, PLATFORM_UPSTREAM_KEY, TENANTS, Tenant, boot, connect,
};

/// A minimal chat request; the fake upstream answers every alias from the same
/// committed fixture, so a tenant's request differs only in who sent it.
///
/// `max_tokens` is declared because the budget hold is priced from it: left out,
/// the pre-dispatch estimate reserves a default output allowance worth orders of
/// magnitude more than the fixture actually costs, and a cap small enough to be
/// reached in a test would refuse the first request on its estimate rather than
/// on any spend.
fn chat(alias: &str) -> Value {
    json!({
        "model": alias,
        "messages": [{ "role": "user", "content": "hello" }],
        "max_tokens": 8,
    })
}

async fn models(deployment: &Deployment, key: &str) -> Vec<String> {
    let response = client()
        .get(deployment.gateway.url("/v1/models"))
        .bearer_auth(key)
        .send()
        .await
        .expect("the catalogue answers");
    assert_eq!(response.status(), 200, "an authenticated caller is served");
    let body: Value = response.json().await.expect("a JSON catalogue");
    body["data"]
        .as_array()
        .expect("a list of models")
        .iter()
        .map(|model| model["id"].as_str().expect("a model id").to_owned())
        .collect()
}

async fn post_chat(deployment: &Deployment, key: &str, alias: &str) -> reqwest::Response {
    client()
        .post(deployment.gateway.url("/v1/chat/completions"))
        .bearer_auth(key)
        .json(&chat(alias))
        .send()
        .await
        .expect("the gateway answers")
}

/// The usage record for `alias`, once settlement has written it.
async fn record_for(deployment: &Deployment, alias: &str, count: usize) -> Value {
    deployment
        .gateway
        .await_usage_records(count)
        .await
        .into_iter()
        .find(|record| record["model"] == json!(alias))
        .unwrap_or_else(|| panic!("a usage record for `{alias}`"))
}

/// A tenant's catalogue is what its *own* credentials can serve — never the
/// deployment's alias list.
///
/// The failure this guards is a catalogue built from the config's models rather
/// than from the caller's namespace: a tenant would learn the aliases, targets,
/// and therefore the providers of every other tenant on the process, and the
/// list is the first thing every client fetches.
#[tokio::test]
async fn a_tenant_sees_only_the_models_its_own_credentials_can_serve() {
    let deployment = boot(Durability::None).await.expect("a stateless boot");

    for tenant in TENANTS {
        let visible = models(&deployment, tenant.key).await;
        assert_eq!(
            visible,
            vec![tenant.alias.to_owned()],
            "{} sees only its own alias",
            tenant.namespace
        );
    }

    // The namespace with no credential of its own sees exactly what fallback
    // entitles it to: the platform's alias, and nothing of either tenant's.
    let borrowed = models(&deployment, FALLBACK_KEY).await;
    assert_eq!(
        borrowed,
        vec![PLATFORM_ALIAS.to_owned()],
        "{FALLBACK_NAMESPACE} sees the pool it may borrow, not the ones it may not"
    );

    // Stated as a whole so a new alias added to the fixture cannot quietly
    // become visible everywhere.
    for tenant in TENANTS {
        let visible = models(&deployment, tenant.key).await;
        for alias in ALL_ALIASES.iter().filter(|alias| **alias != tenant.alias) {
            assert!(
                !visible.contains(&(*alias).to_owned()),
                "{} must not see `{alias}`",
                tenant.namespace
            );
        }
    }
}

/// Naming another tenant's alias is refused, and refused *before* the provider
/// is reached.
///
/// Both halves matter. A 502 that had already dispatched upstream would mean the
/// other tenant's credential had been spent, and the caller could infer the
/// alias exists from the latency alone. So the upstream's recorded requests are
/// asserted to be empty, not merely free of the other tenant's key.
#[tokio::test]
async fn a_tenant_cannot_invoke_another_tenants_alias() {
    let deployment = boot(Durability::None).await.expect("a stateless boot");

    for (caller, other) in [(&ACME, &GLOBEX), (&GLOBEX, &ACME)] {
        let response = post_chat(&deployment, caller.key, other.alias).await;
        assert_eq!(
            response.status(),
            502,
            "{} is refused {}: {response:?}",
            caller.namespace,
            other.alias
        );
        let body: Value = response.json().await.expect("a typed error");
        assert_eq!(body["error"]["type"], json!("no_credential"));
        let message = body["error"]["message"]
            .as_str()
            .expect("a message")
            .to_owned();
        assert!(
            message.contains(caller.namespace),
            "the refusal is about the caller's own namespace: {message}"
        );
        assert!(
            !message.contains(other.credential_id) && !message.contains(other.upstream_key),
            "a refusal names nothing of the other tenant's: {message}"
        );
    }

    assert!(
        deployment.upstream.state.requests().is_empty(),
        "a cross-tenant alias is refused before dispatch, so no provider was reached"
    );
}

/// Every provider request carries the credential of the tenant that made it,
/// and no request ever carries another tenant's.
///
/// Interleaved on purpose: a pool keyed by provider alone, or a cache that
/// remembers the last credential resolved, passes a sequential test and fails
/// this one.
#[tokio::test]
async fn a_provider_request_carries_only_the_calling_tenants_credential() {
    let deployment = boot(Durability::None).await.expect("a stateless boot");

    let order = [&ACME, &GLOBEX, &GLOBEX, &ACME];
    for tenant in order {
        let response = post_chat(&deployment, tenant.key, tenant.alias).await;
        assert_eq!(response.status(), 200, "{} is served", tenant.namespace);
    }

    let recorded = deployment.upstream.state.requests();
    assert_eq!(recorded.len(), order.len(), "one request each");
    for (request, tenant) in recorded.iter().zip(order) {
        assert_eq!(
            request.authorization.as_deref(),
            Some(format!("Bearer {}", tenant.upstream_key).as_str()),
            "{} is authenticated with its own credential",
            tenant.namespace
        );
    }

    // Stated over the whole recording as well: no request anywhere in the run
    // carried a credential its caller does not own.
    let foreign_keys = |tenant: &Tenant| {
        [
            ACME.upstream_key,
            GLOBEX.upstream_key,
            PLATFORM_UPSTREAM_KEY,
        ]
        .into_iter()
        .filter(|key| *key != tenant.upstream_key)
        .collect::<Vec<_>>()
    };
    for (request, tenant) in recorded.iter().zip(order) {
        let authorization = request.authorization.clone().unwrap_or_default();
        for foreign in foreign_keys(tenant) {
            assert!(
                !authorization.contains(foreign),
                "{} must never be sent a credential of another namespace",
                tenant.namespace
            );
        }
    }
}

/// Platform fallback serves the namespace that opted in, refuses the ones that
/// did not, and says in the usage record which pool paid.
///
/// This is the audit half of the property: being served by a credential you do
/// not own is legitimate here, so what makes it safe is that it is explicit in
/// the config and attributable afterwards. A record that said `byok` for a
/// borrowed platform key would bill the wrong tenant.
#[tokio::test]
async fn platform_fallback_is_explicit_and_attributed() {
    let deployment = boot(Durability::None).await.expect("a stateless boot");

    // Opted in: served, by the platform's credential.
    let response = post_chat(&deployment, FALLBACK_KEY, PLATFORM_ALIAS).await;
    assert_eq!(response.status(), 200, "the opted-in namespace is served");
    let borrowed = record_for(&deployment, PLATFORM_ALIAS, 1).await;
    assert_eq!(borrowed["namespace"], json!(FALLBACK_NAMESPACE));
    assert_eq!(
        borrowed["credential_source"],
        json!("platform"),
        "spend on a borrowed credential is attributed to the platform pool"
    );
    assert_eq!(borrowed["credential_id"], json!(PLATFORM_CREDENTIAL_ID));

    let request = deployment
        .upstream
        .state
        .requests()
        .pop()
        .expect("the provider was reached");
    assert_eq!(
        request.authorization.as_deref(),
        Some(format!("Bearer {PLATFORM_UPSTREAM_KEY}").as_str()),
        "the platform's own credential, not a tenant's"
    );

    // Not opted in: refused, even though the pool it would borrow exists and is
    // reachable from the same process.
    for tenant in TENANTS {
        let response = post_chat(&deployment, tenant.key, PLATFORM_ALIAS).await;
        assert_eq!(
            response.status(),
            502,
            "{} did not opt in to the platform pool",
            tenant.namespace
        );
    }

    // And a tenant's own spend is still attributed to itself.
    let response = post_chat(&deployment, ACME.key, ACME.alias).await;
    assert_eq!(response.status(), 200);
    let own = record_for(&deployment, ACME.alias, 2).await;
    assert_eq!(own["namespace"], json!(ACME.namespace));
    assert_eq!(own["credential_source"], json!("byok"));
    assert_eq!(own["credential_id"], json!(ACME.credential_id));
}

/// Durable usage rows are partitioned by namespace: a tenant's spend query
/// returns its own requests and nothing else.
///
/// Skipped without a test Postgres, and mandatory in CI, which sets
/// `AXOND_TEST_REQUIRE_SERVICES=1`.
#[tokio::test]
async fn usage_rows_never_cross_a_namespace() {
    let cap = 100_000_000;
    let Some(deployment) = boot(Durability::Postgres {
        namespace_cap_microdollars: cap,
    })
    .await
    else {
        return;
    };

    for tenant in TENANTS {
        let response = post_chat(&deployment, tenant.key, tenant.alias).await;
        assert_eq!(response.status(), 200, "{} is served", tenant.namespace);
    }
    // Settlement is detached from the response, and the sink batches: the
    // stdout sink is the cheap signal that both records exist at all.
    deployment.gateway.await_usage_records(2).await;

    let client = connect(&deployment.objects().dsn).await;
    let rows = await_usage_rows(&client, &deployment.objects().usage_table, 2).await;

    for tenant in TENANTS {
        let owned: Vec<&(String, String, String)> = rows
            .iter()
            .filter(|(namespace, ..)| namespace == tenant.namespace)
            .collect();
        assert_eq!(
            owned.len(),
            1,
            "{} settled exactly one request: {rows:?}",
            tenant.namespace
        );
        let (_, model, credential) = owned[0];
        assert_eq!(model, tenant.alias, "the alias the tenant itself asked for");
        assert_eq!(
            credential, tenant.credential_id,
            "attributed to the tenant's own credential"
        );
    }
}

/// One tenant exhausting its namespace budget does not deny another.
///
/// The failure this guards is a shared ledger: a cap keyed on anything coarser
/// than the namespace turns one noisy tenant into an outage for everyone on the
/// process, and a Postgres budget is shared by construction, so the key is the
/// only thing keeping them apart.
#[tokio::test]
async fn one_tenants_exhausted_budget_does_not_deny_another() {
    // Small enough that a handful of fixture-priced requests exhausts it, and
    // large enough that the first one is admitted rather than refused on its
    // pre-dispatch estimate.
    let cap = 400;
    let Some(deployment) = boot(Durability::Postgres {
        namespace_cap_microdollars: cap,
    })
    .await
    else {
        return;
    };

    // Spend until acme is capped. Bounded: each fixture request costs tens of
    // micro-dollars, so a cap this small cannot survive the loop.
    let mut served = 0;
    let mut denied = false;
    for _ in 0..32 {
        let status = post_chat(&deployment, ACME.key, ACME.alias).await.status();
        if status == 429 {
            denied = true;
            break;
        }
        assert_eq!(status, 200, "an admitted request is served");
        served += 1;
        // Each request must be settled before the next is priced, or the cap is
        // reached by held estimates rather than by measured spend.
        deployment.gateway.await_usage_records(served).await;
    }
    assert!(
        denied,
        "a {cap} micro-dollar namespace cap is reached within 32 requests"
    );
    assert!(served > 0, "the cap admitted at least one request");

    // The other tenant, on the same table and the same process, is untouched.
    let response = post_chat(&deployment, GLOBEX.key, GLOBEX.alias).await;
    assert_eq!(
        response.status(),
        200,
        "{} is served while {} is capped",
        GLOBEX.namespace,
        ACME.namespace
    );

    let client = connect(&deployment.objects().dsn).await;
    let namespaces = format!("{}_namespace", deployment.objects().budget_table);
    let rows = client
        .query(
            &format!("SELECT namespace, spent_microdollars FROM {namespaces} ORDER BY namespace"),
            &[],
        )
        .await
        .expect("the namespace ledger is readable");
    let spend: Vec<(String, i64)> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
        .collect();
    let globex_spend = spend
        .iter()
        .find(|(namespace, _)| namespace == GLOBEX.namespace)
        .map(|(_, spent)| *spent)
        .expect("globex has its own ledger row");
    assert!(
        globex_spend > 0 && globex_spend < i64::try_from(cap).expect("a small cap"),
        "globex's ledger holds only its own single request: {spend:?}"
    );
}

/// `(namespace, model, credential_id)` per settled row, once `count` have
/// landed. The sink batches, so the rows arrive shortly after the response.
async fn await_usage_rows(
    client: &tokio_postgres::Client,
    table: &str,
    count: usize,
) -> Vec<(String, String, String)> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let rows = client
            .query(
                &format!("SELECT namespace, model, credential_id FROM {table} ORDER BY namespace"),
                &[],
            )
            .await
            .expect("the usage table is readable");
        if rows.len() >= count {
            return rows
                .iter()
                .map(|row| (row.get(0), row.get(1), row.get(2)))
                .collect();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected {count} usage rows in {table}, saw {}",
            rows.len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
