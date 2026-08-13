//! A per-run Postgres schema, owned from before it exists.
//!
//! Every stateful fixture here names a schema of its own, so concurrent runs
//! share a database without sharing tables — and the schema has to exist before
//! the fixture that uses it can be built. The `CREATE` is therefore always
//! followed by setup that can fail: a migration that refuses, a config that will
//! not render, an assertion about what the fixture found. A shared CI database
//! keeps whatever a panic in that window abandons, one schema per run, forever.
//!
//! So the cleanup is a value the caller holds from *before* the `CREATE` rather
//! than a step at the end of a setup that may not reach it.

/// The claim on a schema: created by [`Schema::create`], dropped with this
/// value however the run that holds it ends.
pub struct Schema {
    dsn: String,
    name: String,
}

impl Schema {
    /// Claim `name` and create it. The claim is taken first, so a `CREATE` that
    /// half-succeeded — and anything that panics after this returns — is still
    /// cleaned up.
    pub async fn create(dsn: &str, name: &str) -> Self {
        let claimed = Self {
            dsn: dsn.to_owned(),
            name: name.to_owned(),
        };
        connect(dsn)
            .await
            .batch_execute(&format!("CREATE SCHEMA {name}"))
            .await
            .expect("a schema of this run's own");
        claimed
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Whether a schema of this name is in the test database: what a case asserting
/// about a run's leftovers reads, from a connection of its own.
pub async fn exists(dsn: &str, name: &str) -> bool {
    connect(dsn)
        .await
        .query_one(
            "SELECT count(*) FROM pg_namespace WHERE nspname = $1",
            &[&name],
        )
        .await
        .expect("a schema lookup")
        .get::<_, i64>(0)
        > 0
}

/// Cleanup a failing assertion cannot skip: a run that panics half-way through
/// a migration would otherwise leave a fully populated schema behind in a
/// database every other run shares.
///
/// The drop runs on a thread of its own with its own runtime, because [`Drop`]
/// cannot await and the test's runtime may already be shutting down.
impl Drop for Schema {
    fn drop(&mut self) {
        let dsn = self.dsn.clone();
        let name = self.name.clone();
        let cleanup = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a cleanup runtime")
                .block_on(async move {
                    connect(&dsn)
                        .await
                        .batch_execute(&format!("DROP SCHEMA IF EXISTS {name} CASCADE"))
                        .await
                        .expect("drop the run's schema");
                });
        });
        // Joined, so the schema is gone before the process is; and only reported
        // when nothing worse is already unwinding, because a second panic during
        // an unwind aborts and buries the assertion the operator needs to read.
        if cleanup.join().is_err() && !std::thread::panicking() {
            panic!("the run's schema {} was left behind", self.name);
        }
    }
}

/// A plain connection to the test database, its connection task detached: the
/// cleanup statements are short and the client goes with them.
async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
