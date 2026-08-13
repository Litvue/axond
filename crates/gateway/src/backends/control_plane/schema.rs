//! Forward-only migrations, and the status a boot refuses or accepts on.
//!
//! Two properties matter more than the mechanism:
//!
//! - **Forward-only.** A migration is applied once, recorded with a checksum of
//!   its own text, and never edited afterwards. Editing an applied file is the
//!   failure mode a version number alone does not catch — the version still
//!   matches, so the database is silently not the schema the build expects — so
//!   [`SchemaStatus::Drifted`] reports it instead.
//! - **A status is a decision, not a log line.** A gateway that finds a database
//!   [`SchemaStatus::Behind`] must either migrate it or refuse to serve the
//!   control plane; one that finds it [`SchemaStatus::Ahead`] must always refuse,
//!   because a newer writer owns that database and "migrating backwards" is not a
//!   thing. Returning a typed status rather than a boolean is what lets the caller
//!   tell those apart.
//!
//! The DDL itself is the operator contract in `ops/postgres/`, embedded here from
//! the package-local copy under `crates/gateway/sql/`; `tests/shipped_ddl.rs`
//! gates the two against drift.

use std::collections::HashSet;
use std::fmt;

use tokio_postgres::Transaction;

use crate::desired_state::Checksum;

/// One versioned, forward-only migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    /// Monotonic, gapless, and never reused.
    pub version: i32,
    /// The shipped file's stem, so a failure names something greppable.
    pub name: &'static str,
    /// The file's text, applied verbatim.
    pub sql: &'static str,
}

impl Migration {
    /// The checksum recorded when this migration is applied.
    pub fn checksum(&self) -> Checksum {
        Checksum::of(self.sql.as_bytes())
    }

    /// The tables this migration's own text declares, in declaration order.
    ///
    /// Read from the embedded SQL rather than listed beside it, so a migration
    /// that adds a table cannot forget to say so: the evidence adoption checks
    /// for is derived from the file adoption claims was applied.
    pub fn relations(&self) -> Vec<String> {
        let mut relations = Vec::new();
        for statement in statements(self.sql) {
            for expectation in expectations(statement).unwrap_or_default() {
                if let Evidence::Table(name) = expectation.what
                    && expectation.present
                    && !relations.contains(&name)
                {
                    relations.push(name);
                }
            }
        }
        relations
    }
}

/// What one statement of a migration did, when that is something a later
/// connection can be asked about: the thing it acted on, and whether it left it
/// there or took it away.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Expectation {
    what: Evidence,
    /// `false` for a `DROP`, which is confirmed by the thing being gone.
    ///
    /// Absence is real evidence — a constraint an earlier version created and
    /// this one dropped is there until this one runs — but it is never evidence
    /// *for* a version by itself, because a database that never had the earlier
    /// version does not have it either. A migration therefore needs at least one
    /// thing present to be adoptable at all.
    present: bool,
    /// Whether having this thing says *this* version is what put it there.
    ///
    /// `false` for something the migration drops and creates again, which is how
    /// a file replaces a definition an earlier version installed: afterwards the
    /// object is there, but it was there before too, so its presence cannot tell
    /// the two apart. v2 replaces v1's `..._actor_attribution` constraints that
    /// way, and reading them as proof would have every v1-only database refused
    /// as half-way through v2. Still checked — a v2 database missing one is
    /// partly applied — just never counted as evidence the version ran.
    proof: bool,
}

/// Where the lexical region starting at `at` ends, when one starts there: a `--`
/// line comment, a `/* */` block comment, a `'...'` literal, or a `$tag$ ... $tag$`
/// body. `None` when `at` is ordinary text.
///
/// Everything inside such a region is prose or data, never syntax — and both
/// scanners below have to agree about that, because they are what adoption's
/// evidence is derived from. Read as syntax, `/* create table axond_cp_head */`
/// before an `ALTER` supplies that statement's leading keywords and turns an
/// unconfirmable migration into an adoptable one, and a `;` inside a `$$` body
/// splits a function into fragments whose keywords read as top-level DDL. Both
/// are the fail-open direction, which is the one this design exists to close.
/// An unterminated region runs to the end of the text: the alternative is
/// reading its contents, and the contents are the hazard. It is reported as
/// uncertain, because *dropping* statements is only fail-closed while what is
/// dropped might have been evidence — a region that swallows an `ALTER` and
/// leaves the `CREATE TABLE`s above it makes an unadoptable migration look
/// adoptable, so [`lexed`] refuses the file instead.
fn skipped(bytes: &[u8], at: usize) -> Option<Region> {
    let after = |from: usize, needle: &[u8]| {
        (from..=bytes.len().saturating_sub(needle.len()))
            .find(|index| &bytes[*index..index + needle.len()] == needle)
            .map_or(
                Region {
                    end: bytes.len(),
                    certain: false,
                },
                |index| Region {
                    end: index + needle.len(),
                    certain: true,
                },
            )
    };
    match bytes[at] {
        // A comment the file simply ends in is a comment, not a loose end.
        b'-' if bytes.get(at + 1) == Some(&b'-') => Some(Region {
            certain: true,
            ..after(at + 2, b"\n")
        }),
        b'/' if bytes.get(at + 1) == Some(&b'*') => {
            // Block comments nest in PostgreSQL, so the first `*/` need not be
            // this one's.
            let (mut depth, mut index) = (1usize, at + 2);
            while index < bytes.len() {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                    if depth == 0 {
                        return Some(Region {
                            end: index,
                            certain: true,
                        });
                    }
                } else {
                    index += 1;
                }
            }
            Some(Region {
                end: bytes.len(),
                certain: false,
            })
        }
        b'\'' => {
            let literal = after(at + 1, b"'");
            // A backslash inside a literal is an escape under `E'...'` and a
            // plain byte otherwise, so where the literal ends depends on syntax
            // this parse does not track: `E'\''` would be read as closing early,
            // and the `'` left over would open a region swallowing whatever
            // follows.
            Some(Region {
                certain: literal.certain && !bytes[at..literal.end].contains(&b'\\'),
                ..literal
            })
        }
        b'$' => {
            // `$tag$` or `$$`, as against a `$1` parameter, which is ordinary text.
            let tag = bytes[at + 1..]
                .iter()
                .position(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
                .filter(|end| bytes.get(at + 1 + end) == Some(&b'$'))?;
            let delimiter = &bytes[at..=at + 1 + tag];
            Some(after(at + delimiter.len(), delimiter))
        }
        _ => None,
    }
}

/// A comment or quoted region: where it ends, and whether that is where it
/// really ends or only where the text ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Region {
    end: usize,
    certain: bool,
}

/// Whether every comment and quoted region in the file closes where this parse
/// says it does.
///
/// One that does not is not a formatting quibble: it silently removes the rest
/// of the file from the parse, and a removed `ALTER` is the difference between a
/// migration adoption refuses and one it records.
fn lexed(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match skipped(bytes, index) {
            Some(region) if !region.certain => return false,
            Some(region) => index = region.end,
            None => index += 1,
        }
    }
    true
}

/// The migration's text as statements, with comments and quoted regions ignored
/// while looking for the separators.
///
/// A `;` inside `'...'` or a `$$` body is not a statement boundary and a comment
/// is not statement text, so a plain `split(';')` would both cut statements in
/// half and find keywords in prose. The slices point into the embedded SQL, so
/// every name parsed out of one is `'static`.
///
/// A chunk with no word outside its comments is not a statement and is dropped: a
/// file that ends with an explanatory comment, or that has a stray `;;`, is
/// otherwise read as a statement nothing can confirm, which would withdraw
/// adoption from the whole history over a comment.
fn statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut index = 0;
    let bytes = sql.as_bytes();
    while index < bytes.len() {
        if let Some(region) = skipped(bytes, index) {
            index = region.end;
            continue;
        }
        if bytes[index] == b';' {
            let statement = sql[start..index].trim();
            if !words(statement).is_empty() {
                statements.push(statement);
            }
            start = index + 1;
        }
        index += 1;
    }
    let tail = sql[start..].trim();
    if !words(tail).is_empty() {
        statements.push(tail);
    }
    statements
}

/// The words of a statement, with comments and quoted regions skipped.
fn words(statement: &str) -> Vec<&str> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;
    let mut index = 0;
    let bytes = statement.as_bytes();
    while index < bytes.len() {
        // A quote or a comment marker ends the word running up to it as much as a
        // space does: `EXISTS foo--why` names `foo`, and dropping the word would
        // make the next one answer for it.
        if let Some(region) = skipped(bytes, index) {
            if let Some(from) = start.take() {
                words.push(&statement[from..index]);
            }
            index = region.end;
            continue;
        }
        if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' {
            start = start.or(Some(index));
        } else if let Some(from) = start.take() {
            words.push(&statement[from..index]);
        }
        index += 1;
    }
    if let Some(from) = start {
        words.push(&statement[from..]);
    }
    words
}

/// Past `IF NOT EXISTS`, `CONCURRENTLY` and `ONLY`, to the object's own name — or
/// `None` for a form this parse cannot name the object of.
fn past(words: &[&str], mut at: usize) -> usize {
    for skipped in ["CONCURRENTLY", "ONLY", "IF", "NOT", "EXISTS"] {
        if words
            .get(at)
            .is_some_and(|word| word.eq_ignore_ascii_case(skipped))
        {
            at += 1;
        }
    }
    at
}

/// The name at `at`, or `None` when what is there is not one this parse can use.
///
/// Two such forms exist and both would otherwise yield a name that is not one.
/// `CREATE INDEX ON t (c)` is legal and unnamed, which reads as an index called
/// `ON`; `CREATE TABLE other.t` names a schema, and the probe asks about
/// `current_schema()` only, so `other` is a table it would look for in the wrong
/// place. Both are unconfirmable rather than merely absent, so the migration is
/// unadoptable and says so, instead of the refusal naming an object no operator
/// can go and find.
fn named(statement: &str, words: &[&str], at: usize) -> Option<String> {
    let at = past(words, at);
    let name = *words.get(at)?;
    if name.eq_ignore_ascii_case("ON") {
        return None;
    }
    // The words point into the statement, so what surrounds one is readable
    // from the offsets: a `.` on either side makes this a qualified name.
    let from = offset(statement, name);
    let bytes = statement.as_bytes();
    let before = from.checked_sub(1).map(|at| bytes[at]);
    if before == Some(b'.') || bytes.get(from + name.len()) == Some(&b'.') {
        return None;
    }
    Some(name.to_owned())
}

/// Where a word this parse took out of `text` starts in it.
fn offset(text: &str, word: &str) -> usize {
    word.as_ptr() as usize - text.as_ptr() as usize
}

/// `text[from..to]`, split on the commas that are not inside parentheses or a
/// quoted region — an `ALTER TABLE`'s clause list, or a `format()` argument list,
/// neither of which can be split on `,` alone: a `CHECK (a, b)` and a
/// `current_setting('x', true)` both carry commas that separate nothing.
fn split(text: &str, from: usize, to: usize) -> Vec<&str> {
    let bytes = text.as_bytes();
    let (mut depth, mut start, mut index) = (0usize, from, from);
    let mut parts = Vec::new();
    while index < to {
        if let Some(region) = skipped(bytes, index) {
            index = region.end;
            continue;
        }
        match bytes[index] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(text[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    parts.push(text[start..to].trim());
    parts
}

/// What a statement left behind, or `None` for one whose effect the catalogue
/// cannot be asked about — an `UPDATE`, a backfill, a non-idempotent `INSERT`, a
/// `DROP TABLE`, an `ALTER` this parse does not model.
///
/// One statement can leave more than one thing behind: an `ALTER TABLE` carries a
/// list of clauses, and a `DO` block carries the statements it executes.
fn expectations(statement: &str) -> Option<Vec<Expectation>> {
    let words = words(statement);
    let keyword = |position: usize, expected: &str| {
        words
            .get(position)
            .is_some_and(|word| word.eq_ignore_ascii_case(expected))
    };
    let present = |what: Evidence| {
        Some(vec![Expectation {
            what,
            present: true,
            proof: true,
        }])
    };
    if keyword(0, "CREATE") && keyword(1, "TABLE") {
        return present(Evidence::Table(named(statement, &words, 2)?));
    }
    if keyword(0, "CREATE") && keyword(1, "INDEX") {
        return present(Evidence::Index(named(statement, &words, 2)?));
    }
    if keyword(0, "CREATE") && keyword(1, "UNIQUE") && keyword(2, "INDEX") {
        return present(Evidence::Index(named(statement, &words, 3)?));
    }
    // An index dropped is confirmed by its being gone, the same way a dropped
    // constraint or policy is. `DROP TABLE` is deliberately not here: a table is
    // what every other piece of evidence about it hangs off, and a file that takes
    // one away leaves nothing to ask about in its place.
    if keyword(0, "DROP") && keyword(1, "INDEX") {
        return Some(vec![Expectation {
            what: Evidence::Index(named(statement, &words, 2)?),
            present: false,
            proof: true,
        }]);
    }
    if keyword(0, "INSERT") && keyword(1, "INTO") {
        // Only the idempotent form: a plain `INSERT` cannot be told apart from
        // one that never ran, and re-running it would double the rows.
        let idempotent = words.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case("DO") && pair[1].eq_ignore_ascii_case("NOTHING")
        });
        return named(statement, &words, 2)
            .filter(|_| idempotent)
            .and_then(|table| present(Evidence::Seed(table)));
    }
    // A policy is named on the table it guards, so both halves are read: two
    // tables can each have an `..._isolation` policy, and confirming one would
    // otherwise confirm the other.
    if keyword(1, "POLICY") && (keyword(0, "CREATE") || keyword(0, "DROP")) {
        let at = past(&words, 2);
        let policy = named(statement, &words, at)?;
        if !words
            .get(at + 1)
            .is_some_and(|word| word.eq_ignore_ascii_case("ON"))
        {
            return None;
        }
        let table = named(statement, &words, at + 2)?;
        return Some(vec![Expectation {
            what: Evidence::Policy(table, policy),
            present: keyword(0, "CREATE"),
            proof: true,
        }]);
    }
    if keyword(0, "ALTER") && keyword(1, "TABLE") {
        let at = past(&words, 2);
        let table = named(statement, &words, at)?;
        let from = offset(statement, words[at]) + words[at].len();
        return split(statement, from, statement.len())
            .into_iter()
            .map(|clause| altered(&table, clause))
            .collect();
    }
    if keyword(0, "DO") && words.len() == 1 {
        return unrolled(statement);
    }
    None
}

/// What one clause of an `ALTER TABLE` left behind, or `None` for a clause whose
/// effect nothing can be asked about.
///
/// A column, a named constraint, and the two row-security flags are all readable
/// out of the catalogue, which is what makes them adoptable evidence at all. A
/// clause outside that set — a type change, a default, an unnamed constraint
/// PostgreSQL names for itself — is not, and withdraws adoption from the
/// migration that carries it rather than being passed over.
fn altered(table: &str, clause: &str) -> Option<Expectation> {
    let words = words(clause);
    let keyword = |position: usize, expected: &str| {
        words
            .get(position)
            .is_some_and(|word| word.eq_ignore_ascii_case(expected))
    };
    let phrase = |expected: &[&str]| {
        words.len() == expected.len()
            && words
                .iter()
                .zip(expected)
                .all(|(word, expected)| word.eq_ignore_ascii_case(expected))
    };
    let flag = |what: Evidence, present: bool| {
        Some(Expectation {
            what,
            present,
            proof: true,
        })
    };
    if phrase(&["ENABLE", "ROW", "LEVEL", "SECURITY"]) {
        return flag(Evidence::Guarded(table.to_owned()), true);
    }
    if phrase(&["DISABLE", "ROW", "LEVEL", "SECURITY"]) {
        return flag(Evidence::Guarded(table.to_owned()), false);
    }
    if phrase(&["FORCE", "ROW", "LEVEL", "SECURITY"]) {
        return flag(Evidence::Forced(table.to_owned()), true);
    }
    if phrase(&["NO", "FORCE", "ROW", "LEVEL", "SECURITY"]) {
        return flag(Evidence::Forced(table.to_owned()), false);
    }
    // `ADD c text` is legal with `COLUMN` left out, and is deliberately not read:
    // the word after `ADD` would be a column name in that form and a keyword in
    // every other one, so requiring the keyword is what keeps `ADD PRIMARY KEY`
    // from being confirmed as a column called `PRIMARY`.
    if keyword(1, "COLUMN") && (keyword(0, "ADD") || keyword(0, "DROP")) {
        let column = named(clause, &words, 2)?;
        return flag(
            Evidence::Column(table.to_owned(), column),
            keyword(0, "ADD"),
        );
    }
    if keyword(1, "CONSTRAINT") && (keyword(0, "ADD") || keyword(0, "DROP")) {
        let constraint = named(clause, &words, 2)?;
        return flag(
            Evidence::Constraint(table.to_owned(), constraint),
            keyword(0, "ADD"),
        );
    }
    None
}

/// What a `DO $$ ... $$` block leaves behind, or `None` for a block this reading
/// does not recognise from end to end.
///
/// Interpreted, never assumed. The shipped history uses procedural blocks for the
/// three things plain DDL cannot express, and each is read out of the block's own
/// text rather than answered with a hand-written list of what it happens to do
/// today, so a change to one of them changes the evidence with it:
///
/// ```sql
/// -- 1. The same statement for a list of tables: the names come out of the
/// --    array, the SQL out of the templates, and each rendered statement is read
/// --    by the same parser as every other one.
/// FOREACH each IN ARRAY ARRAY['a', 'b'] LOOP
///     EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', each);
/// END LOOP;
/// -- 2. Create-if-absent, which is how a file adds a constraint idempotently.
/// --    The object is there afterwards either way, so it is ordinary evidence —
/// --    but only when the guard asks about that very object, because a condition
/// --    on something else leaves the effect conditional.
/// IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'a'::regclass
///                  AND conname = 'a_slug_unique') THEN
///     ALTER TABLE a ADD CONSTRAINT a_slug_unique UNIQUE (slug) DEFERRABLE;
/// END IF;
/// -- 3. Dropping what a catalogue query names, which is how a file replaces a
/// --    constraint PostgreSQL named for itself. The names are not in the file, so
/// --    the statements cannot be rendered — the query is the evidence: after the
/// --    loop it selects nothing. Never proof (it selects nothing against a
/// --    database that never had them either), always required.
/// FOR stale IN SELECT conname FROM pg_constraint WHERE conrelid = 'a'::regclass
///                AND contype = 'u' LOOP
///     EXECUTE format('ALTER TABLE a DROP CONSTRAINT %I', stale.conname);
/// END LOOP;
/// ```
///
/// Anything else — a branch whose alternative is not the same final state, a
/// literal array built from a query, a template argument that is not the loop
/// value, a loop that does something other than drop what its query named —
/// leaves the block unconfirmable and its migration unadoptable, the same
/// fail-closed answer an `UPDATE` gets.
fn unrolled(statement: &str) -> Option<Vec<Expectation>> {
    let body = quoted(statement)?;
    // The file-level scan skips a `$tag$ ... $tag$` region whole, so this is the
    // first look inside it: the same fail-closed rule applies, or a literal that
    // does not end where this reading says it does would swallow the rest of the
    // block and shorten the evidence rather than void it.
    if !lexed(body) {
        return None;
    }
    interpreted(&statements(body))
}

/// The effects of a block's chunks, read in order.
///
/// The chunks are what `;` leaves, so a control structure's header shares a chunk
/// with the first statement of its body (`LOOP EXECUTE ...`, `THEN ALTER ...`) and
/// its `END` is a chunk of its own. `BEGIN`, `DECLARE`, and those `END`s state no
/// effect; every other chunk must be one this parse can account for.
fn interpreted(chunks: &[&str]) -> Option<Vec<Expectation>> {
    let mut expectations = Vec::new();
    let mut index = 0;
    while index < chunks.len() {
        // `BEGIN` opens the body without ending a statement, so it shares a chunk
        // with the first one.
        let chunk = after(chunks[index], "BEGIN").unwrap_or(chunks[index]);
        let words = words(chunk);
        let word = |position: usize, expected: &str| {
            words
                .get(position)
                .is_some_and(|word| word.eq_ignore_ascii_case(expected))
        };
        // A declaration and a structure's end are not effects. A declaration is
        // not read for its type either: what the loop does with the variable is
        // what the evidence is derived from, and that is read where it happens.
        if words.is_empty()
            || word(0, "DECLARE")
            || (word(0, "END") && (words.len() == 1 || word(1, "IF") || word(1, "LOOP")))
        {
            index += 1;
            continue;
        }
        if word(0, "IF") {
            let (guard, body, next) = guarded(chunk, chunks, index)?;
            for statement in body {
                for expectation in self::expectations(statement)? {
                    // A guarded statement is evidence only when the guard asks
                    // about the thing the statement leaves behind: `IF NOT EXISTS
                    // (this constraint) THEN add it` ends with the constraint
                    // there whichever way it went, while a condition on anything
                    // else is a branch, and which branch ran is not in the file.
                    if !expectation.present || !stated(&expectation.what, &guard) {
                        return None;
                    }
                    expectations.push(expectation);
                }
            }
            index = next;
            continue;
        }
        if let Some(header) = after(chunk, "FOREACH") {
            let (over, body, next) = looped(header, chunks, index)?;
            let (variable, names) = listed(&over)?;
            for name in &names {
                for statement in &body {
                    expectations
                        .extend(self::expectations(&rendered(statement, &variable, name)?)?);
                }
            }
            index = next;
            continue;
        }
        if let Some(header) = after(chunk, "FOR") {
            let (over, body, next) = looped(header, chunks, index)?;
            expectations.push(cleared(&over, &body)?);
            index = next;
            continue;
        }
        expectations.extend(self::expectations(chunk)?);
        index += 1;
    }
    Some(expectations)
}

/// What follows `keyword` in `text`, when `text` starts with it.
fn after<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let words = words(text);
    let first = words.first()?;
    if !first.eq_ignore_ascii_case(keyword) {
        return None;
    }
    Some(text[offset(text, first) + first.len()..].trim())
}

/// An `IF <guard> THEN` structure: its condition, the statements it guards, and
/// the chunk after its `END IF`.
fn guarded<'a>(
    opening: &'a str,
    chunks: &[&'a str],
    index: usize,
) -> Option<(String, Vec<&'a str>, usize)> {
    let (head, first) = divided(opening, "THEN")?;
    // Only `IF NOT EXISTS (...)`: `IF EXISTS` guards a statement whose effect
    // depends on state this parse cannot reconstruct, and so does a comparison.
    let condition = words(&head);
    if !(condition.len() > 3
        && condition[0].eq_ignore_ascii_case("IF")
        && condition[1].eq_ignore_ascii_case("NOT")
        && condition[2].eq_ignore_ascii_case("EXISTS")
        && condition[3].eq_ignore_ascii_case("SELECT"))
    {
        return None;
    }
    let (body, next) = bodied(first, chunks, index, &["END", "IF"])?;
    Some((head, body, next))
}

/// A `FOREACH`/`FOR ... LOOP` structure: what it iterates over, the statements it
/// runs, and the chunk after its `END LOOP`.
fn looped<'a>(
    opening: &'a str,
    chunks: &[&'a str],
    index: usize,
) -> Option<(String, Vec<&'a str>, usize)> {
    let (over, first) = divided(opening, "LOOP")?;
    let (body, next) = bodied(first, chunks, index, &["END", "LOOP"])?;
    Some((over, body, next))
}

/// A chunk split at the first top-level occurrence of `keyword`: what came before
/// it, and what came after.
fn divided<'a>(chunk: &'a str, keyword: &str) -> Option<(String, &'a str)> {
    let words = words(chunk);
    let at = words.iter().position(|word| {
        word.eq_ignore_ascii_case(keyword)
            // A word inside the condition's own parentheses is not the separator:
            // `IF NOT EXISTS (SELECT ... WHERE loop = 1) THEN`.
            && depth(chunk, offset(chunk, word)) == 0
    })?;
    let from = offset(chunk, words[at]);
    Some((
        chunk[..from].trim().to_owned(),
        chunk[from + words[at].len()..].trim(),
    ))
}

/// How many parentheses are open at `at`, with comments and literals skipped.
fn depth(text: &str, at: usize) -> usize {
    let bytes = text.as_bytes();
    let (mut depth, mut index) = (0usize, 0);
    while index < at {
        if let Some(region) = skipped(bytes, index) {
            index = region.end;
            continue;
        }
        match bytes[index] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
    depth
}

/// The statements a structure's body holds: the tail of its own chunk, then the
/// chunks up to the one that closes it. `None` when nothing closes it, or when a
/// structure opens inside it — nesting is not read, because the outer structure's
/// effect would then depend on the inner one's.
fn bodied<'a>(
    first: &'a str,
    chunks: &[&'a str],
    index: usize,
    closing: &[&str],
) -> Option<(Vec<&'a str>, usize)> {
    let mut body = Vec::new();
    if !words(first).is_empty() {
        body.push(first);
    }
    for (at, chunk) in chunks.iter().enumerate().skip(index + 1) {
        let words = words(chunk);
        if words.len() == closing.len()
            && words
                .iter()
                .zip(closing)
                .all(|(word, expected)| word.eq_ignore_ascii_case(expected))
        {
            return Some((body, at + 1));
        }
        if words.first().is_some_and(|word| {
            [
                "IF", "FOR", "FOREACH", "WHILE", "LOOP", "CASE", "BEGIN", "END",
            ]
            .iter()
            .any(|structure| word.eq_ignore_ascii_case(structure))
        }) {
            return None;
        }
        body.push(chunk);
    }
    None
}

/// The loop variable and the literal names a `FOREACH v IN ARRAY ARRAY[...]`
/// header iterates over.
fn listed(header: &str) -> Option<(String, Vec<String>)> {
    let words = words(header);
    let [variable, over @ ..] = words.as_slice() else {
        return None;
    };
    if !over
        .iter()
        .zip(["IN", "ARRAY", "ARRAY"])
        .all(|(word, expected)| word.eq_ignore_ascii_case(expected))
        || over.len() != 3
    {
        return None;
    }
    let names = literals(header);
    if names.is_empty() {
        return None;
    }
    Some(((*variable).to_owned(), names))
}

/// The evidence a `FOR v IN <query> LOOP EXECUTE format(... DROP CONSTRAINT ...)
/// END LOOP` leaves: afterwards the query names nothing the file did not declare
/// itself.
///
/// Read only for the one thing it can be: a loop that drops exactly what its own
/// query named. The rows are not in the file, so no statement can be rendered and
/// no object can be named — but "this query names nothing" is a question the same
/// catalogue answers, and it is the loop's own question.
fn cleared(header: &str, body: &[&str]) -> Option<Expectation> {
    let (variable, query) = divided(header, "IN")?;
    if words(&variable).len() != 1 {
        return None;
    }
    for statement in body {
        // Every statement must drop, and drop something the query named: a loop
        // that also writes, or that drops a fixed name, is not summarised by its
        // query being empty.
        let (dropped, arguments) = templated(statement)?;
        let words = words(&dropped);
        let drops = words.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case("DROP") && pair[1].eq_ignore_ascii_case("CONSTRAINT")
        });
        if !drops
            || !arguments
                .iter()
                .all(|argument| argument.starts_with(&format!("{}.", variable.trim())))
        {
            return None;
        }
    }
    Some(Expectation {
        what: Evidence::Stale {
            table: literals(query).first()?.clone(),
            query: catalogued(query)?,
            // Filled in by `evidence`, which is where the rest of the file — and
            // so what it declares under a name of its own — is in view.
            except: Vec::new(),
        },
        present: false,
        proof: false,
    })
}

/// A read-only catalogue query naming the constraints it selects, or `None` for
/// anything else.
///
/// Adoption probes with the migration's own query, so what may be probed is
/// exactly what a `SELECT` over `pg_constraint` can answer: one statement,
/// changing nothing, yielding the `conname` the loop drops — which the probe needs
/// too, to tell a definition the migration should have removed from one it went on
/// to declare itself.
fn catalogued(query: &str) -> Option<String> {
    let words = words(query);
    if !words.first()?.eq_ignore_ascii_case("SELECT") {
        return None;
    }
    if !words
        .iter()
        .any(|word| word.eq_ignore_ascii_case("pg_constraint"))
    {
        return None;
    }
    let (selected, _) = divided(query, "FROM")?;
    if !self::words(&selected)
        .iter()
        .any(|word| word.eq_ignore_ascii_case("conname"))
    {
        return None;
    }
    if words.iter().any(|word| {
        [
            "INSERT", "UPDATE", "DELETE", "ALTER", "DROP", "CREATE", "GRANT", "REVOKE", "TRUNCATE",
            "COPY", "CALL", "DO", "SET", "LOCK", "NEXTVAL", "PG_SLEEP",
        ]
        .iter()
        .any(|forbidden| word.eq_ignore_ascii_case(forbidden))
    }) {
        return None;
    }
    Some(query.to_owned())
}

/// An `EXECUTE format('...', ...)`'s template with its placeholders removed, and
/// the arguments it fills them from.
fn templated(statement: &str) -> Option<(String, Vec<String>)> {
    let words = words(statement);
    if !(words.first()?.eq_ignore_ascii_case("EXECUTE")
        && words.get(1)?.eq_ignore_ascii_case("format"))
    {
        return None;
    }
    let open = statement.find('(')?;
    let close = statement.rfind(')')?;
    let arguments = split(statement, open + 1, close);
    let (template, arguments) = arguments.split_first()?;
    let quoted = literals(template);
    let [template] = quoted.as_slice() else {
        return None;
    };
    Some((
        template.replace("%I", " ").replace("%s", " "),
        arguments
            .iter()
            .map(|argument| argument.trim().to_owned())
            .collect(),
    ))
}

/// Whether a condition asks about the very thing an expectation is about, by
/// naming every part of it as a literal.
fn stated(what: &Evidence, condition: &str) -> bool {
    let literals = literals(condition);
    let names = match what {
        Evidence::Table(name) | Evidence::Index(name) | Evidence::Seed(name) => vec![name],
        Evidence::Column(table, column) => vec![table, column],
        Evidence::Constraint(table, constraint) => vec![table, constraint],
        Evidence::Policy(table, policy) => vec![table, policy],
        Evidence::Guarded(table) | Evidence::Forced(table) => vec![table],
        Evidence::Stale { .. } => return false,
    };
    names
        .into_iter()
        .all(|name| literals.iter().any(|literal| literal == name))
}

/// The body of the first `$tag$ ... $tag$` region in a statement.
fn quoted(statement: &str) -> Option<&str> {
    let bytes = statement.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let region = skipped(bytes, index);
        if bytes[index] == b'$'
            && let Some(region) = region
        {
            if !region.certain {
                return None;
            }
            let tag = bytes[index + 1..].iter().position(|byte| *byte == b'$')? + 2;
            return statement.get(index + tag..region.end - tag);
        }
        index = region.map_or(index + 1, |region| region.end);
    }
    None
}

/// The single-quoted literals in a fragment, in order, with a doubled quote read
/// as the one character it stands for.
fn literals(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut literals = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\'' {
            index += 1;
            continue;
        }
        let mut value = String::new();
        index += 1;
        while index < bytes.len() {
            if bytes[index] == b'\'' {
                if bytes.get(index + 1) == Some(&b'\'') {
                    value.push('\'');
                    index += 2;
                    continue;
                }
                index += 1;
                break;
            }
            value.push(bytes[index] as char);
            index += 1;
        }
        literals.push(value);
    }
    literals
}

/// One `EXECUTE format('...', ...)` rendered for one loop value, or `None` for
/// any other statement and for any argument that is not the loop variable, the
/// loop variable with a literal suffix, or a literal.
///
/// `%I` is an identifier and `%s` is text, which is all this loop shape uses;
/// `%L` and a placeholder without an argument are not rendered, because guessing
/// at the SQL a template would have produced is guessing at the evidence.
fn rendered(statement: &str, variable: &str, name: &str) -> Option<String> {
    let words = words(statement);
    if !(words.first()?.eq_ignore_ascii_case("EXECUTE")
        && words.get(1)?.eq_ignore_ascii_case("format"))
    {
        return None;
    }
    let open = statement.find('(')?;
    let close = statement.rfind(')')?;
    let arguments = split(statement, open + 1, close);
    let (template, arguments) = arguments.split_first()?;
    let template = match literals(template).as_slice() {
        [only] if template.starts_with('\'') && template.ends_with('\'') => only.clone(),
        _ => return None,
    };
    let mut values = Vec::new();
    for argument in arguments {
        let value = match self::words(argument).as_slice() {
            // `v`, the loop value itself.
            [only] if *only == variable && argument.trim() == variable => name.to_owned(),
            // `v || '_suffix'`, how a derived object is named.
            [only] if *only == variable => match literals(argument).as_slice() {
                [suffix] if argument.contains("||") => format!("{name}{suffix}"),
                _ => return None,
            },
            // A literal, which is text the template carries rather than a name.
            [] => match literals(argument).as_slice() {
                [only] => only.clone(),
                _ => return None,
            },
            _ => return None,
        };
        values.push(value);
    }
    let mut rendered = String::new();
    let mut values = values.iter();
    let mut characters = template.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            rendered.push(character);
            continue;
        }
        match characters.next()? {
            '%' => rendered.push('%'),
            'I' => {
                let value = values.next()?;
                // A rendered identifier this parse cannot spell is one the probe
                // cannot ask about: `%I` quotes whatever it is given, so a value
                // needing quotes names an object no other statement in the file
                // could have named.
                if value.is_empty()
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                {
                    return None;
                }
                rendered.push_str(value);
            }
            's' => rendered.push_str(values.next()?),
            _ => return None,
        }
    }
    values.next().map_or(Some(rendered), |_| None)
}

/// Every migration this build ships, in application order.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "control_plane_0001_initial",
        sql: include_str!("../../../sql/control_plane_0001_initial.sql"),
    },
    Migration {
        version: 2,
        name: "control_plane_0002_tenancy_access",
        sql: include_str!("../../../sql/control_plane_0002_tenancy_access.sql"),
    },
    Migration {
        version: 3,
        name: "control_plane_0003_tenancy_constraints",
        sql: include_str!("../../../sql/control_plane_0003_tenancy_constraints.sql"),
    },
];

/// The schema version this build requires.
pub fn required_version() -> i32 {
    MIGRATIONS
        .last()
        .expect("at least one migration ships")
        .version
}

/// The minimum PostgreSQL the DDL is written against, as `server_version_num`.
///
/// 14 is the floor because the journal uses identity columns and `ON CONFLICT`
/// against partial unique indexes. CI exercises 17.
pub const MINIMUM_SERVER_VERSION_NUM: i32 = 140_000;

/// What a database holds relative to what this build requires.
///
/// The variants are the decision, so they are as fine-grained as the decisions
/// are: an operator told "the schema is wrong" has to go find out *how*, whereas
/// an operator told a version prefix has a hole in it knows a migration was
/// applied out of order or a row was deleted, and one told a name does not match
/// knows a file was renumbered rather than edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaStatus {
    /// No journal at all: the migration bookkeeping table does not exist.
    Absent,
    /// The ledger exists and records nothing.
    ///
    /// Not migratable, and not the same thing as [`SchemaStatus::Absent`]: an
    /// empty ledger is indistinguishable from a database whose objects were
    /// created by hand — the ledger is the only record of what was applied, so
    /// with no rows this build cannot tell an untouched database from a fully
    /// populated one. Migrating from 0 would re-run every shipped file over
    /// objects that may already exist, which survives only as long as every
    /// statement is `IF NOT EXISTS`; the first `ALTER TABLE` or backfill would
    /// double-apply. The baseline is adopted deliberately instead —
    /// [`baseline`] reconciles it against the objects the database actually
    /// holds, and `axond migrate adopt` is the operator command that records it.
    Unrecorded,
    Current {
        version: i32,
    },
    Behind {
        applied: i32,
        required: i32,
    },
    /// A newer build has migrated this database. Always a refusal.
    Ahead {
        applied: i32,
        required: i32,
    },
    /// A recorded migration's text is not the text this build ships.
    Drifted {
        version: i32,
        expected: Checksum,
        found: Checksum,
    },
    /// The applied versions do not form a complete prefix: something applied
    /// v3 without v2, or a ledger row was deleted. Never migratable, because
    /// "apply everything after the maximum" would leave the hole behind.
    Incomplete {
        applied: i32,
        missing: Vec<i32>,
    },
    /// A version is recorded under a name this build does not ship it as. The
    /// checksum may still match — a renumbered or renamed file is the usual
    /// cause — so it is reported separately from drift.
    Renamed {
        version: i32,
        expected: &'static str,
        found: String,
    },
    /// The ledger exists but is not the ledger this build writes: a column is
    /// missing, a version is not a version, or the rows cannot be read as the
    /// journal's own bookkeeping.
    Malformed {
        message: String,
    },
}

impl SchemaStatus {
    /// Whether the control plane may be used as-is.
    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current { .. })
    }

    /// Whether applying this build's migrations would make it current.
    ///
    /// True only for [`SchemaStatus::Absent`] and [`SchemaStatus::Behind`], both
    /// of which say what the database already contains: nothing, or a recorded
    /// prefix. Every other status means the database is not this schema's history, and
    /// writing more DDL over it would make that worse rather than better.
    pub fn is_migratable(&self) -> bool {
        matches!(self, Self::Absent | Self::Behind { .. })
    }
}

impl fmt::Display for SchemaStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => write!(
                f,
                "the control-plane schema is not present; run `axond migrate apply` (or apply \
                 ops/postgres/control_plane_0001_initial.sql)"
            ),
            Self::Unrecorded => write!(
                f,
                "`{MIGRATION_TABLE}` exists but records no migrations, so this build cannot tell \
                 whether the schema it describes was ever applied and will not migrate from zero \
                 over objects that may already exist; if the DDL was applied out of band, run \
                 `axond migrate adopt` to record the baseline the database's own objects account \
                 for, and if nothing was applied, drop the empty `{MIGRATION_TABLE}` table and \
                 run `axond migrate apply`"
            ),
            Self::Current { version } => write!(f, "control-plane schema v{version} is current"),
            Self::Behind { applied, required } => write!(
                f,
                "control-plane schema is v{applied}, but this build requires v{required}; run \
                 `axond migrate apply` before starting replicas"
            ),
            Self::Ahead { applied, required } => write!(
                f,
                "control-plane schema is v{applied}, which is newer than the v{required} this \
                 build knows; a newer gateway owns this database"
            ),
            Self::Drifted {
                version,
                expected,
                found,
            } => write!(
                f,
                "control-plane migration v{version} was applied as {found}, but this build ships \
                 {expected}; an applied migration was edited in place"
            ),
            Self::Incomplete { applied, missing } => write!(
                f,
                "control-plane schema records v{applied} but is missing {}; the applied versions \
                 are not a complete history, so this build cannot tell what the database contains",
                missing
                    .iter()
                    .map(|version| format!("v{version}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Renamed {
                version,
                expected,
                found,
            } => write!(
                f,
                "control-plane migration v{version} is recorded as `{found}`, but this build ships \
                 v{version} as `{expected}`; a migration was renumbered or renamed rather than \
                 added"
            ),
            Self::Malformed { message } => write!(
                f,
                "the control-plane migration ledger is not the one this build writes: {message}"
            ),
        }
    }
}

/// The bookkeeping table's name, unqualified: the caller's `search_path` decides
/// which schema it is read from.
pub(crate) const MIGRATION_TABLE: &str = "axond_cp_schema_migration";

/// Read the status inside a transaction the caller controls.
///
/// Takes a transaction rather than a client so a status read and the migration
/// that follows it see the same snapshot under the same advisory lock.
pub(super) async fn status(
    transaction: &Transaction<'_>,
) -> Result<SchemaStatus, tokio_postgres::Error> {
    let present: Option<String> = transaction
        .query_one("SELECT to_regclass($1)::text", &[&MIGRATION_TABLE])
        .await?
        .get(0);
    if present.is_none() {
        return Ok(SchemaStatus::Absent);
    }
    // A ledger table that will not answer the ledger's own query is a
    // [`SchemaStatus::Malformed`] rather than an error: the table exists, so this
    // is a schema disagreement an operator has to resolve, not a database that
    // could not be reached. Something else owns that name.
    //
    // Only errors that say *that*, though. Every server-reported error carries a
    // SQLSTATE, including `57014 query_canceled` and `40001 serialization_failure`,
    // and calling a cancelled statement a broken schema would tell an operator to
    // go and fix a history that is fine — and would strip the retryable
    // classification the error type exists to carry. Class 42 (syntax and access
    // rules: undefined table, undefined column, insufficient privilege) is the
    // class that means the name is not this build's ledger.
    let rows = match transaction
        .query(
            &format!("SELECT version, name, checksum FROM {MIGRATION_TABLE} ORDER BY version"),
            &[],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) if is_schema_disagreement(&error) => {
            return Ok(SchemaStatus::Malformed {
                message: format!(
                    "reading `{MIGRATION_TABLE}` as (version, name, checksum) failed: {error}"
                ),
            });
        }
        Err(error) => return Err(error),
    };
    // Decoded fallibly for the same reason: a table that answers to those three
    // column *names* with other types (`version text`, `checksum bytea`) makes the
    // query succeed, and `Row::get` would panic on it. That is the documented
    // `Malformed` case — a version that is not a version — not a crash.
    let mut recorded = Vec::with_capacity(rows.len());
    for row in &rows {
        let decoded = row
            .try_get(0)
            .and_then(|version| {
                Ok(Recorded {
                    version,
                    name: row.try_get(1)?,
                    checksum: row.try_get(2)?,
                })
            })
            .map_err(|error| format!("`{MIGRATION_TABLE}` holds a row this build cannot read as (version integer, name text, checksum text): {error}"));
        match decoded {
            Ok(row) => recorded.push(row),
            Err(message) => return Ok(SchemaStatus::Malformed { message }),
        }
    }
    Ok(classify(&recorded))
}

/// Whether a failed ledger read means the table is not this build's ledger, as
/// opposed to a database that had a bad moment.
///
/// SQLSTATE class 42 is "syntax error or access rule violation": `42P01`
/// undefined table, `42703` undefined column, `42501` insufficient privilege.
/// Anything else — a cancelled statement, a serialization failure, a deadlock —
/// stays an error, so it keeps its retryable classification.
fn is_schema_disagreement(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|code| code.code().starts_with("42"))
}

/// One row of the migration ledger, as the database holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    pub version: i32,
    pub name: String,
    pub checksum: String,
}

/// The status a set of ledger rows implies.
///
/// Separated from the query so the decision table is testable without a database:
/// it is the part that has to be right. The checks are ordered by how much they
/// tell an operator — an unknown version outranks a name mismatch, which outranks
/// a checksum mismatch — and the prefix check comes last because it is only
/// meaningful once every recorded row is one this build recognises.
fn classify(recorded: &[Recorded]) -> SchemaStatus {
    let required = required_version();
    if let Some(row) = recorded.iter().find(|row| row.version < 1) {
        return SchemaStatus::Malformed {
            message: format!(
                "v{} is recorded, but migration versions start at 1",
                row.version
            ),
        };
    }
    let mut versions: Vec<i32> = recorded.iter().map(|row| row.version).collect();
    versions.sort_unstable();
    versions.dedup();
    if versions.len() != recorded.len() {
        return SchemaStatus::Malformed {
            message: "a version is recorded more than once, so the ledger's primary key is not \
                      the one this build writes"
                .to_owned(),
        };
    }
    let Some(applied) = versions.last().copied() else {
        // The table exists and is empty, which is not the same question as
        // "has anything been applied?". It is what a database whose DDL was
        // applied by hand looks like, and also what a database with a
        // hand-created ledger and nothing else looks like, and the ledger is
        // the only thing that could tell them apart. Migrating from 0 would
        // replay every file over whatever is there.
        return SchemaStatus::Unrecorded;
    };
    for row in recorded {
        let Some(migration) = MIGRATIONS.iter().find(|m| m.version == row.version) else {
            // A version this build has never heard of: the database's history is
            // longer than ours, whatever the maximum happens to be.
            return SchemaStatus::Ahead {
                applied: row.version,
                required,
            };
        };
        if row.name != migration.name {
            return SchemaStatus::Renamed {
                version: row.version,
                expected: migration.name,
                found: row.name.clone(),
            };
        }
        let expected = migration.checksum();
        if row.checksum != expected.to_string() {
            return SchemaStatus::Drifted {
                version: row.version,
                expected,
                found: Checksum::parse(&row.checksum).unwrap_or(expected),
            };
        }
    }
    // Versions are gapless by construction, so the applied set must be the whole
    // prefix `1..=applied`. A hole means a migration was skipped or a row was
    // deleted, and "apply everything above the maximum" would silently keep it.
    let missing: Vec<i32> = (1..=applied)
        .filter(|version| !versions.contains(version))
        .collect();
    if !missing.is_empty() {
        return SchemaStatus::Incomplete { applied, missing };
    }
    match applied.cmp(&required) {
        std::cmp::Ordering::Equal => SchemaStatus::Current { version: applied },
        std::cmp::Ordering::Less => SchemaStatus::Behind { applied, required },
        std::cmp::Ordering::Greater => SchemaStatus::Ahead { applied, required },
    }
}

/// The versions [`migrate`] would apply from this status, in application order.
///
/// Empty for a status that is already current, and empty for one that must be
/// refused: an operator asking "what would `apply` do?" gets the same answer the
/// apply itself would act on rather than a separately computed guess.
pub fn pending(from: &SchemaStatus) -> Vec<i32> {
    let applied = match from {
        SchemaStatus::Absent => 0,
        SchemaStatus::Behind { applied, .. } => *applied,
        // Everything else is either done or a refusal, and a refusal has no
        // pending set: what an operator has to do about it is not "apply files".
        SchemaStatus::Current { .. }
        | SchemaStatus::Unrecorded
        | SchemaStatus::Ahead { .. }
        | SchemaStatus::Drifted { .. }
        | SchemaStatus::Incomplete { .. }
        | SchemaStatus::Renamed { .. }
        | SchemaStatus::Malformed { .. } => return Vec::new(),
    };
    MIGRATIONS
        .iter()
        .filter(|migration| migration.version > applied)
        .map(|migration| migration.version)
        .collect()
}

/// Apply every migration the database is missing, recording each one.
///
/// The caller holds the advisory lock, so two gateways booting against one empty
/// database serialize here rather than both running the DDL.
pub(super) async fn migrate(
    transaction: &Transaction<'_>,
    from: &SchemaStatus,
) -> Result<(), tokio_postgres::Error> {
    let applied = match from {
        SchemaStatus::Behind { applied, .. } => *applied,
        _ => 0,
    };
    for migration in MIGRATIONS.iter().filter(|m| m.version > applied) {
        transaction.batch_execute(migration.sql).await?;
        transaction
            .execute(
                &format!(
                    "INSERT INTO {MIGRATION_TABLE} (version, name, checksum) VALUES ($1, $2, $3) \
                     ON CONFLICT (version) DO NOTHING"
                ),
                &[
                    &migration.version,
                    &migration.name,
                    &migration.checksum().to_string(),
                ],
            )
            .await?;
    }
    Ok(())
}

/// What recording a baseline into an empty ledger would be asserting.
///
/// Adoption exists for one database: a [`SchemaStatus::Unrecorded`] ledger, which
/// is what applying the shipped DDL with `psql` leaves behind. The ledger cannot
/// answer "was this applied?", so the objects are asked instead — and the answer
/// is only ever a *prefix* of the shipped history, because that is the only shape
/// a forward-only sequence of files can produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Baseline {
    /// Every table versions `1..=n` declare is present, so recording them states
    /// what the database already contains rather than guessing it.
    Applied { versions: Vec<i32> },
    /// No shipped migration's tables are there: nothing was applied out of band,
    /// so there is no baseline to adopt and the empty ledger is the only thing
    /// standing between this database and an ordinary `apply`.
    Nothing,
    /// No baseline is adoptable: a migration is half applied, a later one's tables
    /// exist without an earlier one's, or the history contains a migration that
    /// creates no table and so cannot be reconciled against objects at all. Never
    /// adopted — recording a version whose objects are incomplete would promise a
    /// schema the database does not have, and recording a prefix under an
    /// unobservable version would leave `apply` to re-run it.
    Inconsistent { message: String },
}

/// One thing a migration did that a later connection can be asked to confirm.
///
/// Every variant is a question the catalogue answers about *one* object, which is
/// what makes it evidence: an operator can go and look at the same thing, and a
/// refusal can name it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Evidence {
    /// A table the migration creates, present in this schema.
    Table(String),
    /// An index the migration creates, present in this schema.
    Index(String),
    /// A seed row an idempotent `INSERT ... ON CONFLICT DO NOTHING` writes,
    /// confirmed by the target table not being empty. A migration whose tables
    /// exist and whose seed row does not is a `psql` run that stopped in the
    /// middle, which is why the row counts as evidence rather than as detail.
    Seed(String),
    /// A column an `ALTER TABLE ... ADD COLUMN` adds, by table and name.
    Column(String, String),
    /// A named constraint an `ALTER TABLE ... ADD CONSTRAINT` adds. Named ones
    /// only: a constraint PostgreSQL names for itself is not a name the file
    /// states, so nothing in it says what to look for.
    Constraint(String, String),
    /// Row-level security enabled on a table (`pg_class.relrowsecurity`).
    Guarded(String),
    /// Row-level security forced on a table, so its owner is subject to the
    /// policies too (`pg_class.relforcerowsecurity`).
    Forced(String),
    /// A policy, by the table it guards and its own name.
    Policy(String, String),
    /// Definitions a migration replaces without being able to name them: the
    /// constraints PostgreSQL named for itself when an earlier version declared
    /// them inline. The file finds them with a catalogue query and drops what it
    /// finds, so the query is the evidence — after the migration it selects
    /// nothing — and `table` is the one it asks about, for the refusal to name.
    ///
    /// Absence only, and never proof: a database that never had those definitions
    /// answers the same way as one the migration cleaned up.
    ///
    /// `except` are the constraints the migration goes on to declare by name. A
    /// query written to find the old definitions of a rule matches a new one in the
    /// same shape — v3 drops every check mentioning `actor_kind`, then adds its own
    /// — so what the migration leaves behind is "nothing this query names, other
    /// than what this file declares".
    Stale {
        table: String,
        query: String,
        except: Vec<String>,
    },
}

/// Everything a migration must be able to show for itself to be adoptable, or
/// `None` when the file contains a statement whose effect cannot be confirmed.
///
/// Derived from the migration's own text, so a statement cannot ship without
/// adoption accounting for it — and the accounting is deliberately total: an
/// `UPDATE`, a backfill, a plain `INSERT`, or an `ALTER` clause outside the set
/// the catalogue answers for makes the whole migration unconfirmable rather than
/// being passed over, because "every table is there" says nothing about a column
/// or a row.
///
/// The ledger table is excluded: adoption only runs against a database whose
/// ledger exists, so its presence is the precondition rather than evidence.
/// Counting it would make a bare ledger look half-applied rather than untouched.
fn evidence(migration: &Migration) -> Option<Vec<Expectation>> {
    if !lexed(migration.sql) {
        return None;
    }
    let mut evidence: Vec<Expectation> = Vec::new();
    for statement in statements(migration.sql) {
        for expectation in expectations(statement)? {
            if expectation.what == Evidence::Table(MIGRATION_TABLE.to_owned()) {
                continue;
            }
            // The last statement to touch a thing is what the file leaves behind,
            // so a policy dropped and recreated is present and a table declared
            // twice is one probe with one answer. A seed is the exception: the
            // target having a row confirms one insert and not two, so a repeated
            // seed stays in the list, where the shared-evidence refusal can see
            // that this table is seeded more than once.
            match evidence
                .iter_mut()
                .find(|prior| prior.what == expectation.what)
            {
                Some(prior) if !matches!(expectation.what, Evidence::Seed(_)) => {
                    // Touched twice in opposite directions is a replacement: the
                    // file dropped what was there and put its own version back,
                    // which leaves a database that had the earlier version and
                    // one that had this one holding the same object. Required
                    // still, but no longer proof of which version wrote it.
                    prior.proof = prior.proof && prior.present == expectation.present;
                    prior.present = expectation.present;
                }
                _ => evidence.push(expectation),
            }
        }
    }
    // What the file declares under a name of its own, which is what a query for
    // the definitions it replaces must be allowed to find afterwards: v3 drops
    // every check on the journal mentioning `actor_kind` and then adds one that
    // does, so "the query names nothing" would be false of a database that ran it.
    let declared: Vec<String> = evidence
        .iter()
        .filter_map(|item| match &item.what {
            Evidence::Constraint(_, constraint) if item.present => Some(constraint.clone()),
            _ => None,
        })
        .collect();
    for item in &mut evidence {
        if let Evidence::Stale { except, .. } = &mut item.what {
            *except = declared.clone();
        }
    }
    Some(evidence)
}

/// How a refusal names a thing the database did not agree about.
///
/// Named by what is actually wrong with each one: a table that is not there, a
/// table there without its seed row, and a policy that was supposed to be dropped
/// and is still there are different repairs, and an operator told "`axond_cp_head`
/// is not present" about a table that exists would go looking for the wrong thing.
fn described(expectation: &Expectation) -> String {
    let thing = named_thing(&expectation.what);
    match (&expectation.what, expectation.present) {
        (Evidence::Seed(_), true) => format!("{thing} has no seeded row"),
        (Evidence::Seed(_), false) => format!("{thing} still has its seeded row"),
        (Evidence::Guarded(_) | Evidence::Forced(_), true) => format!("{thing} is not enabled"),
        (Evidence::Guarded(_) | Evidence::Forced(_), false) => format!("{thing} is still enabled"),
        (Evidence::Stale { .. }, _) => format!("{thing} is still there"),
        (_, true) => format!("{thing} is not present"),
        (_, false) => format!("{thing} is still present"),
    }
}

/// What a piece of evidence is, in the words an operator would use to go and look
/// at the same thing.
fn named_thing(what: &Evidence) -> String {
    match what {
        Evidence::Table(name) | Evidence::Index(name) | Evidence::Seed(name) => format!("`{name}`"),
        Evidence::Column(table, column) => format!("`{table}`'s `{column}` column"),
        Evidence::Constraint(table, constraint) => {
            format!("`{table}`'s `{constraint}` constraint")
        }
        Evidence::Guarded(table) => format!("row level security on `{table}`"),
        Evidence::Forced(table) => format!("forced row level security on `{table}`"),
        Evidence::Policy(table, policy) => format!("`{table}`'s `{policy}` policy"),
        Evidence::Stale { table, .. } => {
            format!("a definition on `{table}` that this migration replaces")
        }
    }
}

/// Reconcile an empty ledger against what the database can show.
///
/// Read-only: this is evidence gathering, and the caller decides what to do with
/// it. Deliberately strict — a version is adoptable only when *everything* it
/// declares is confirmed, and a version confirmed after one that is not is a
/// refusal rather than a hole to paper over.
pub(super) async fn baseline(
    transaction: &Transaction<'_>,
) -> Result<Baseline, tokio_postgres::Error> {
    let mut confirmed: HashSet<Evidence> = HashSet::new();
    for item in MIGRATIONS
        .iter()
        .filter_map(evidence)
        .flatten()
        .map(|expectation| expectation.what)
    {
        // Every probe is qualified to the one schema this connection writes in —
        // the schema `[control_plane] schema` selected, or the first on the DSN's
        // own search path, which is where `apply` would have created these
        // objects. An unqualified probe would resolve down the whole search path,
        // so another install's journal sitting in `public` would be read as
        // evidence that *this* schema's DDL was applied.
        //
        // A `DROP` is probed the same way as everything else and answered the same
        // way: this set is what the database has, and the expectation that named it
        // says whether having it is what the migration would have left.
        let found: bool = match &item {
            Evidence::Table(name) | Evidence::Index(name) => transaction
                .query_one(
                    "SELECT EXISTS (\
                       SELECT 1 FROM pg_catalog.pg_class class \
                         JOIN pg_catalog.pg_namespace namespace \
                           ON namespace.oid = class.relnamespace \
                        WHERE class.relname = $1 \
                          AND class.relkind IN ('r', 'p', 'i', 'I') \
                          AND namespace.nspname = current_schema())",
                    &[&name],
                )
                .await?
                .get(0),
            // The table is confirmed by its own probe first, and a table in
            // `current_schema()` shadows one of the same name further down the
            // path, so the row this finds is this schema's. No table means no seed
            // row either.
            Evidence::Seed(name) => {
                if !confirmed.contains(&Evidence::Table(name.clone())) {
                    false
                } else {
                    transaction
                        .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {name})"), &[])
                        .await?
                        .get(0)
                }
            }
            Evidence::Column(table, column) => transaction
                .query_one(
                    "SELECT EXISTS (\
                       SELECT 1 FROM pg_catalog.pg_attribute attribute \
                         JOIN pg_catalog.pg_class class \
                           ON class.oid = attribute.attrelid \
                         JOIN pg_catalog.pg_namespace namespace \
                           ON namespace.oid = class.relnamespace \
                        WHERE class.relname = $1 \
                          AND attribute.attname = $2 \
                          AND attribute.attnum > 0 \
                          AND NOT attribute.attisdropped \
                          AND namespace.nspname = current_schema())",
                    &[&table, &column],
                )
                .await?
                .get(0),
            Evidence::Constraint(table, constraint) => transaction
                .query_one(
                    "SELECT EXISTS (\
                       SELECT 1 FROM pg_catalog.pg_constraint constraint_ \
                         JOIN pg_catalog.pg_class class \
                           ON class.oid = constraint_.conrelid \
                         JOIN pg_catalog.pg_namespace namespace \
                           ON namespace.oid = class.relnamespace \
                        WHERE class.relname = $1 \
                          AND constraint_.conname = $2 \
                          AND namespace.nspname = current_schema())",
                    &[&table, &constraint],
                )
                .await?
                .get(0),
            // Enabled and forced are two flags on the table, and the difference
            // matters: a deployment whose application role owns its tables gets no
            // enforcement at all from `ENABLE` alone, so a migration that asked for
            // both is only applied when both are set.
            Evidence::Guarded(table) | Evidence::Forced(table) => transaction
                .query_one(
                    &format!(
                        "SELECT EXISTS (\
                           SELECT 1 FROM pg_catalog.pg_class class \
                             JOIN pg_catalog.pg_namespace namespace \
                               ON namespace.oid = class.relnamespace \
                            WHERE class.relname = $1 \
                              AND class.{} \
                              AND namespace.nspname = current_schema())",
                        match item {
                            Evidence::Forced(_) => "relforcerowsecurity",
                            _ => "relrowsecurity",
                        }
                    ),
                    &[&table],
                )
                .await?
                .get(0),
            Evidence::Policy(table, policy) => transaction
                .query_one(
                    "SELECT EXISTS (\
                       SELECT 1 FROM pg_catalog.pg_policy policy \
                         JOIN pg_catalog.pg_class class \
                           ON class.oid = policy.polrelid \
                         JOIN pg_catalog.pg_namespace namespace \
                           ON namespace.oid = class.relnamespace \
                        WHERE class.relname = $1 \
                          AND policy.polname = $2 \
                          AND namespace.nspname = current_schema())",
                    &[&table, &policy],
                )
                .await?
                .get(0),
            // The migration's own query, which is the only thing that can answer
            // for definitions the file never names: `catalogued` admitted it as a
            // single read of `pg_constraint` and nothing else. Pinned to this
            // schema like every other probe, by giving the query a search path
            // holding only the one — it resolves its tables with `::regclass`, and
            // a neighbouring schema's tables further down the path would otherwise
            // answer for this one's.
            Evidence::Stale { query, except, .. } => {
                transaction
                    .batch_execute(
                        "SAVEPOINT stale_definitions; \
                         SELECT set_config('search_path', current_schema(), true)",
                    )
                    .await?;
                let found = transaction
                    .query_one(
                        &format!(
                            "SELECT EXISTS (\
                               SELECT 1 FROM ({query}) stale \
                                WHERE stale.conname <> ALL ($1::text[]))"
                        ),
                        &[except],
                    )
                    .await;
                // The savepoint is rolled back either way: the pinned search path
                // is undone with it, and a query resolving its tables with
                // `::regclass` against a database where nothing was applied
                // there raises rather than answering — "no such table" is
                // "nothing of the kind is there", not an outage to report.
                transaction
                    .batch_execute(
                        "ROLLBACK TO SAVEPOINT stale_definitions; \
                         RELEASE SAVEPOINT stale_definitions",
                    )
                    .await?;
                match found {
                    Ok(row) => row.get(0),
                    Err(error)
                        if error.code()
                            == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) =>
                    {
                        false
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        if found {
            confirmed.insert(item);
        }
    }
    Ok(reconcile(MIGRATIONS, &confirmed))
}

/// The prefix a set of confirmed objects and rows accounts for, and nothing more.
///
/// Separated from the probing so the shape of the history — a whole version, a
/// half-applied one, a hole, a version nothing can confirm — is decided by a
/// function that can be examined directly.
///
/// Read prefix by prefix, longest first, because what a migration leaves behind is
/// only fixed once the versions above it are accounted for: v3 drops indexes v2
/// created, so "`axond_cp_tenant_slug_idx` is present" is what a v1+v2 database
/// looks like and "it is gone" is what a v1+v2+v3 one looks like. Asking each
/// migration about its own statements in isolation would call one of those two
/// real databases half-applied.
fn reconcile(migrations: &[Migration], confirmed: &HashSet<Evidence>) -> Baseline {
    let mut declared: Vec<Vec<Expectation>> = Vec::new();
    for migration in migrations {
        // A migration containing a statement whose effect nothing can be asked
        // about — a backfill, an `UPDATE`, a non-idempotent `INSERT`, an `ALTER`
        // clause the catalogue has no answer for — blocks adoption of this
        // database wherever in the history it sits, including the versions below
        // it. So does one that leaves nothing of its own behind: absence is not
        // evidence a version ran, because a database that never had the version
        // before it does not have those things either.
        //
        // Fail-closed on purpose, in both directions. Recording it on the strength
        // of the objects it happens to create would claim a column or a row that
        // may never have been written; recording only the prefix underneath would
        // have the ledger call it pending, so the next `apply` would run it over a
        // database that may already have had it applied out of band. That rerun is
        // precisely the non-idempotent replay adoption exists to prevent, and no
        // ledger row both accounts for the objects and keeps `apply` away.
        let Some(items) =
            evidence(migration).filter(|items| items.iter().any(|item| item.present && item.proof))
        else {
            return Baseline::Inconsistent {
                message: format!(
                    "v{} `{}` contains a statement whose effect this database cannot be asked \
                     about, so whether it was applied is not something adoption can confirm — and \
                     recording a baseline below it would leave `axond migrate apply` to re-run it \
                     over a schema that may already have it. No baseline is adoptable while it \
                     ships unrecorded: state the history with `INSERT INTO {MIGRATION_TABLE} \
                     (version, name, checksum)` if you own the change that applied it, or drop the \
                     empty ledger and apply from zero if nothing was.",
                    migration.version, migration.name,
                ),
            };
        };
        declared.push(items);
    }
    // A seed row is the one piece of evidence that does not say how many
    // statements wrote it: the target has a row or it does not, so two inserts
    // into one table — in a file or across the history — are each "confirmed" by
    // the other's row, and a `psql` run that stopped between them would look
    // finished. Every other kind is a thing that exists once, so a second
    // migration acting on it is a replacement, handled below by no longer letting
    // it prove anything.
    for (migration, items) in migrations.iter().zip(&declared) {
        if let Some(Evidence::Seed(name)) = items
            .iter()
            .map(|item| &item.what)
            .filter(|what| matches!(what, Evidence::Seed(_)))
            .find(|what| {
                declared
                    .iter()
                    .flatten()
                    .filter(|other| &other.what == *what)
                    .count()
                    > 1
            })
        {
            return Baseline::Inconsistent {
                message: format!(
                    "v{} `{}` seeds `{name}`, which the shipped history seeds more than once, so a \
                     row in it proves at most one of those inserts and not this one: whether this \
                     version ran is not something adoption can confirm. No baseline is adoptable \
                     while they all ship: state the history with `INSERT INTO {MIGRATION_TABLE} \
                     (version, name, checksum)` if you own the change that applied it, or drop the \
                     empty ledger and apply from zero if nothing was.",
                    migration.version, migration.name,
                ),
            };
        }
    }
    // Every version needs one thing that is its alone. A thing more than one
    // shipped migration acts on is proof of neither: a table two files declare
    // `IF NOT EXISTS`, or a constraint one drops and the next re-adds under the
    // same name, says at most that one of them ran and not which — and so does an
    // index a later version takes away, which is there when the earlier version ran
    // and the later one did not, and also when neither did. A version whose every
    // effect is shared that way is one no database can be shown to have reached: it
    // is unadoptable for the reason an `UPDATE` is, and blocks the history for the
    // same reason, because recording the prefix under it would leave `apply` to
    // re-run it over a schema that may already have had it.
    //
    // Only *every* effect being shared is fatal. A version with one thing of its
    // own is adoptable on that, which is how the shipped history stays adoptable:
    // v3 takes v2's indexes away, and v2 is proven by the columns and tables it
    // alone adds.
    for (migration, items) in migrations.iter().zip(&declared) {
        let shared = |what: &Evidence| {
            declared
                .iter()
                .filter(|other| other.iter().any(|item| &item.what == what))
                .count()
                > 1
        };
        let mut proof = items.iter().filter(|item| item.present && item.proof);
        if proof.clone().all(|item| shared(&item.what)) {
            let Some(item) = proof.next() else {
                unreachable!("a migration with no proof of its own was refused above");
            };
            return Baseline::Inconsistent {
                message: format!(
                    "v{} `{}` acts on {}, which more than one shipped migration acts on, so what \
                     the database shows proves at most one of them and not this one: whether this \
                     version ran is not something adoption can confirm. No baseline is adoptable \
                     while they all ship: state the history with `INSERT INTO {MIGRATION_TABLE} \
                     (version, name, checksum)` if you own the change that applied it, or drop the \
                     empty ledger and apply from zero if nothing was.",
                    migration.version,
                    migration.name,
                    named_thing(&item.what),
                ),
            };
        }
    }
    for length in (1..=migrations.len()).rev() {
        match fitted(&migrations[..length], &declared[..length], confirmed) {
            // The longest prefix every version of which the database can account
            // for. Longest first, so a database that really is at v3 is recorded
            // as v3 rather than refused for the indexes v3 took away.
            Fit::Baseline => {
                // The prefix accounts for what it declares, but a forward-only
                // history is applied in order: something only a version *above* it
                // leaves means this database is not that prefix, whatever else
                // matches. Recording it anyway would state a history the database
                // does not have and leave `apply` to run files over objects that
                // are already there, so a skipped middle version is refused rather
                // than rounded down.
                if let Some(skipped) = migrations[length..]
                    .iter()
                    .zip(&declared[length..])
                    .find(|(_, items)| {
                        // Everything that version leaves of its own is there: it
                        // ran. One such object on its own is not enough to say so,
                        // because an earlier version can create the same thing
                        // inline in a `CREATE TABLE` this parse reads as a table
                        // and nothing more.
                        let mut proof = items.iter().filter(|item| item.present && item.proof);
                        proof.clone().next().is_some()
                            && proof.all(|item| confirmed.contains(&item.what))
                    })
                    .map(|(migration, _)| migration)
                {
                    return Baseline::Inconsistent {
                        message: format!(
                            "v{} `{}` declares objects that are present while an earlier version's \
                             are not; this database is not a prefix of the shipped migration \
                             history, so no baseline describes it. State the history with `INSERT \
                             INTO {MIGRATION_TABLE} (version, name, checksum)` if you own the \
                             change that applied it.",
                            skipped.version, skipped.name,
                        ),
                    };
                }
                return Baseline::Applied {
                    versions: migrations[..length]
                        .iter()
                        .map(|migration| migration.version)
                        .collect(),
                };
            }
            // A version in this prefix is half-way applied, which no baseline
            // describes: adoption cannot record it, and `apply` cannot run it over
            // what is there. Refused here rather than falling back to a shorter
            // prefix, because recording one would leave `apply` to finish a
            // migration that has already had part of its effect.
            Fit::Refused { message } => return Baseline::Inconsistent { message },
            // Nothing shows this database ever reached the top of this prefix, so
            // try the one below it.
            Fit::Shorter => {}
        }
    }
    if confirmed.is_empty() {
        return Baseline::Nothing;
    }
    // Objects from the shipped history are there, but no prefix of it accounts for
    // them: a later version's objects without an earlier one's, or only things a
    // later version replaced. Named as far as it can be, because "not a prefix" is
    // a schema an operator has to go and look at.
    let proven = |items: &Vec<Expectation>| {
        items
            .iter()
            .any(|item| item.present && item.proof && confirmed.contains(&item.what))
    };
    let hole = migrations
        .iter()
        .zip(&declared)
        .find(|(_, items)| !proven(items))
        .map(|(migration, _)| migration.version);
    let above = migrations
        .iter()
        .zip(&declared)
        .filter(|(migration, items)| Some(migration.version) > hole && proven(items))
        .map(|(migration, _)| (migration.version, migration.name))
        .next_back();
    Baseline::Inconsistent {
        message: match (hole, above) {
            (Some(hole), Some((version, name))) => format!(
                "v{version} `{name}` declares objects that are present while v{hole} declares \
                 objects that are not; this database is not a prefix of the shipped migration \
                 history, so no baseline describes it"
            ),
            _ => format!(
                "objects the shipped migrations act on are present, but nothing in this schema \
                 shows which version put them there — no baseline describes it. State the history \
                 with `INSERT INTO {MIGRATION_TABLE} (version, name, checksum)` if you own the \
                 change that applied it."
            ),
        },
    }
}

/// What a prefix of the shipped history has to say about a database.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Fit {
    /// Every version in the prefix left something only it leaves, and the schema
    /// is exactly what the prefix ends with: the prefix describes this database.
    Baseline,
    /// The prefix went further than this database did.
    Shorter,
    /// This database is part-way through one of the prefix's versions.
    Refused { message: String },
}

/// Whether a prefix of the shipped history describes what the database shows.
///
/// Two questions, and the order matters. Did each version in the prefix leave
/// something behind that only it leaves — if not, the database never got this far,
/// which is not a fault. And is the schema what the prefix *ends* with, the last
/// version to act on a thing deciding whether it is there — if not, some version
/// is half-way applied, which is.
fn fitted(
    migrations: &[Migration],
    declared: &[Vec<Expectation>],
    confirmed: &HashSet<Evidence>,
) -> Fit {
    // The state the prefix leaves: `ALTER ... ENABLE`, then a later `DISABLE`,
    // leaves it disabled, and an index v2 creates and v3 drops is gone. The last
    // version to act on a thing owns it, and owns the refusal that names it.
    let mut left: Vec<(usize, &Expectation)> = Vec::new();
    for (index, items) in declared.iter().enumerate() {
        for item in items {
            match left.iter_mut().find(|(_, prior)| prior.what == item.what) {
                Some(owner) => *owner = (index, item),
                None => left.push((index, item)),
            }
        }
    }
    // Anything more than one version in the prefix acts on is a replacement, and
    // proves nothing about which of them ran: a database holding only v1 holds the
    // constraint v2 drops and re-adds under the same name, and reading that as v2's
    // work would refuse every v1-only database as half-way through v2. Required
    // still — a v2 database missing one is partly applied — just never proof.
    let replaced = |what: &Evidence| {
        declared
            .iter()
            .filter(|items| items.iter().any(|item| &item.what == what))
            .count()
            > 1
    };
    for (migration, items) in migrations.iter().zip(declared) {
        let proven = items.iter().any(|item| {
            item.present && item.proof && !replaced(&item.what) && confirmed.contains(&item.what)
        });
        if !proven {
            return Fit::Shorter;
        }
        // Named by what is actually wrong with each one: a table that is not there
        // and a table that is there without its seed row are different repairs, and
        // an operator told "`axond_cp_head` is not present" about a table that
        // exists would go looking for the wrong thing.
        let missing: Vec<String> = left
            .iter()
            .filter(|(owner, item)| {
                migrations[*owner].version == migration.version
                    && confirmed.contains(&item.what) != item.present
            })
            .map(|(_, item)| described(item))
            .collect();
        if !missing.is_empty() {
            return Fit::Refused {
                message: format!(
                    "v{} `{}` is only partly applied: {}, so this build cannot record it as \
                     applied and cannot apply it over what is there either. Finish or undo that \
                     migration by hand, then re-run.",
                    migration.version,
                    migration.name,
                    missing.join(", "),
                ),
            };
        }
    }
    Fit::Baseline
}

/// Record an adopted baseline: the versions whose objects are already there.
///
/// The same rows [`migrate`] writes, with the same checksums, so a database that
/// was adopted and one that was migrated are afterwards the same database as far
/// as every other classification is concerned. `ON CONFLICT DO NOTHING` for the
/// reason `migrate` has it: the caller holds the advisory lock, and a row that
/// appeared anyway is not one to overwrite.
pub(super) async fn record_baseline(
    transaction: &Transaction<'_>,
    versions: &[i32],
) -> Result<(), tokio_postgres::Error> {
    for migration in MIGRATIONS
        .iter()
        .filter(|migration| versions.contains(&migration.version))
    {
        transaction
            .execute(
                &format!(
                    "INSERT INTO {MIGRATION_TABLE} (version, name, checksum) VALUES ($1, $2, $3) \
                     ON CONFLICT (version) DO NOTHING"
                ),
                &[
                    &migration.version,
                    &migration.name,
                    &migration.checksum().to_string(),
                ],
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded(version: i32) -> Recorded {
        let migration = MIGRATIONS
            .iter()
            .find(|m| m.version == version)
            .expect("shipped migration");
        Recorded {
            version,
            name: migration.name.to_owned(),
            checksum: migration.checksum().to_string(),
        }
    }

    /// A row nothing shipped wrote: the ledger as a restored backup, a manual
    /// `INSERT`, or a newer build left it.
    fn foreign(version: i32, name: &str, checksum: &str) -> Recorded {
        Recorded {
            version,
            name: name.to_owned(),
            checksum: checksum.to_owned(),
        }
    }

    /// Something a fixture's statement leaves behind.
    fn present(what: Evidence) -> Expectation {
        Expectation {
            what,
            present: true,
            proof: true,
        }
    }

    /// Something a fixture dropped and put back: required, but not proof of which
    /// version wrote it.
    fn replaced(what: Evidence) -> Expectation {
        Expectation {
            what,
            present: true,
            proof: false,
        }
    }

    /// Something a fixture's statement takes away, confirmed by its absence.
    fn gone(what: Evidence) -> Expectation {
        Expectation {
            what,
            present: false,
            proof: true,
        }
    }

    fn table(name: &str) -> Evidence {
        Evidence::Table(name.to_owned())
    }

    fn index(name: &str) -> Evidence {
        Evidence::Index(name.to_owned())
    }

    fn seed(name: &str) -> Evidence {
        Evidence::Seed(name.to_owned())
    }

    fn column(table: &str, column: &str) -> Evidence {
        Evidence::Column(table.to_owned(), column.to_owned())
    }

    fn constraint(table: &str, constraint: &str) -> Evidence {
        Evidence::Constraint(table.to_owned(), constraint.to_owned())
    }

    fn policy(table: &str, policy: &str) -> Evidence {
        Evidence::Policy(table.to_owned(), policy.to_owned())
    }

    #[test]
    fn migrations_are_gapless_and_never_reordered() {
        for (index, migration) in MIGRATIONS.iter().enumerate() {
            assert_eq!(
                migration.version,
                i32::try_from(index + 1).expect("small"),
                "migrations are numbered from 1 without gaps, so `applied` is a version count"
            );
            assert!(
                migration.name.starts_with("control_plane_"),
                "a migration's name is its shipped file's stem"
            );
            assert!(!migration.sql.trim().is_empty());
        }
        assert_eq!(required_version(), MIGRATIONS.len() as i32);
    }

    #[test]
    fn an_unrecorded_or_partial_history_is_not_current_and_a_complete_one_is() {
        assert_eq!(classify(&[]), SchemaStatus::Unrecorded);
        let complete: Vec<_> = MIGRATIONS.iter().map(|m| recorded(m.version)).collect();
        assert_eq!(
            classify(&complete),
            SchemaStatus::Current {
                version: required_version()
            }
        );
        assert!(classify(&complete).is_current());
    }

    #[test]
    fn an_unknown_version_is_ahead_and_never_migratable() {
        let status = classify(&[
            recorded(1),
            foreign(
                99,
                "control_plane_0099_future",
                &Checksum::of(b"newer").to_string(),
            ),
        ]);
        assert_eq!(
            status,
            SchemaStatus::Ahead {
                applied: 99,
                required: required_version()
            }
        );
        assert!(!status.is_migratable());
        assert!(status.to_string().contains("newer gateway"));
    }

    #[test]
    fn an_edited_applied_migration_is_drift_rather_than_a_matching_version() {
        let status = classify(&[foreign(
            1,
            MIGRATIONS[0].name,
            &Checksum::of(b"edited in place").to_string(),
        )]);
        let SchemaStatus::Drifted {
            version,
            expected,
            found,
        } = status.clone()
        else {
            panic!("an edited migration must be reported as drift, got {status:?}");
        };
        assert_eq!(version, 1);
        assert_eq!(expected, MIGRATIONS[0].checksum());
        assert_eq!(found, Checksum::of(b"edited in place"));
        assert!(!status.is_migratable());
        assert!(!status.is_current());
    }

    #[test]
    fn a_renamed_migration_is_reported_as_a_rename_even_when_its_text_matches() {
        let mut row = recorded(1);
        row.name = "control_plane_0001_initial_v2".to_owned();
        let status = classify(&[row]);
        assert_eq!(
            status,
            SchemaStatus::Renamed {
                version: 1,
                expected: MIGRATIONS[0].name,
                found: "control_plane_0001_initial_v2".to_owned(),
            }
        );
        assert!(
            !status.is_migratable(),
            "a version this build ships under another name is not a history it can extend"
        );
        assert!(status.to_string().contains("renumbered or renamed"));
    }

    /// A hole in the prefix is the failure `max(version)` alone cannot see: the
    /// maximum is right, so a version-count check would call the database current.
    #[test]
    fn a_hole_in_the_applied_prefix_is_incomplete_rather_than_current_or_behind() {
        // A version past the newest this build ships is a future version before
        // it is a hole: the ledger is ahead, and this build cannot judge what it
        // has never seen.
        let beyond = required_version() + 1;
        let status = classify(&[foreign(
            beyond,
            "control_plane_9999_later",
            &Checksum::of(b"later").to_string(),
        )]);
        assert_eq!(
            status,
            SchemaStatus::Ahead {
                applied: beyond,
                required: required_version()
            },
        );

        // Within the shipped range, the same shape of ledger is a hole: the
        // newest is right, and an earlier version never ran.
        let applied = required_version();
        let missing: Vec<i32> = (1..applied).collect();
        let status = SchemaStatus::Incomplete { applied, missing };
        assert!(!status.is_migratable());
        assert!(!status.is_current());
        assert!(status.to_string().contains("missing v1"), "{status}");
    }

    #[test]
    fn a_ledger_that_is_not_this_ledger_is_malformed_rather_than_behind() {
        let duplicated = classify(&[recorded(1), recorded(1)]);
        assert!(
            matches!(duplicated, SchemaStatus::Malformed { .. }),
            "two rows for one version is not a history: {duplicated:?}"
        );
        assert!(!duplicated.is_migratable());
        let zeroed = classify(&[foreign(0, "control_plane_0000", "sha256:0")]);
        assert!(
            matches!(zeroed, SchemaStatus::Malformed { .. }),
            "versions start at 1: {zeroed:?}"
        );
        assert!(
            zeroed.to_string().contains("not the one this build writes"),
            "{zeroed}"
        );
    }

    /// An empty ledger is not "apply everything": the files it would replay may
    /// already be in the database, and the ledger is the only thing that could
    /// have said so. An *absent* ledger is "apply everything" — nothing has run.
    #[test]
    fn an_empty_ledger_is_refused_while_an_absent_one_pends_every_shipped_version() {
        let empty = classify(&[]);
        assert_eq!(empty, SchemaStatus::Unrecorded);
        assert!(
            !empty.is_migratable() && !empty.is_current(),
            "an empty ledger must not be migrated from zero: {empty:?}"
        );
        assert!(
            pending(&empty).is_empty(),
            "nothing is pending against an empty ledger, or an apply would replay every file"
        );
        let rendered = empty.to_string();
        for expected in [
            "records no migrations",
            "drop the empty",
            "axond migrate adopt",
            "axond migrate apply",
        ] {
            assert!(
                rendered.contains(expected),
                "the refusal has to name the action to take, missing `{expected}`: {rendered}"
            );
        }

        assert_eq!(
            pending(&SchemaStatus::Absent),
            MIGRATIONS.iter().map(|m| m.version).collect::<Vec<_>>()
        );
        for refused in [
            SchemaStatus::Unrecorded,
            SchemaStatus::Ahead {
                applied: 9,
                required: 1,
            },
            SchemaStatus::Drifted {
                version: 1,
                expected: MIGRATIONS[0].checksum(),
                found: Checksum::of(b"edited"),
            },
            SchemaStatus::Current { version: 1 },
        ] {
            assert!(
                pending(&refused).is_empty(),
                "nothing is pending against {refused:?}: an apply must not write there"
            );
        }
    }

    #[test]
    fn the_shipped_ddl_is_the_migration_this_build_applies() {
        let ddl = MIGRATIONS[0].sql;
        for object in [
            "axond_cp_schema_migration",
            "axond_cp_blob",
            "axond_cp_resource_version",
            "axond_cp_resource_dependency",
            "axond_cp_mutation",
            "axond_cp_revision",
            "axond_cp_revision_entry",
            "axond_cp_revision_blob",
            "axond_cp_audit_event",
            "axond_cp_idempotency",
            "axond_cp_head",
        ] {
            assert!(
                ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {object}")),
                "the journal's {object} table is missing from the shipped DDL"
            );
        }
    }

    /// The evidence adoption reconciles an empty ledger against is read out of the
    /// migration's own text, so a migration that adds a table cannot ship without
    /// adoption knowing to look for it. A parse that silently found nothing would
    /// turn every adoption into an unchecked assertion, which is exactly the
    /// failure the operation exists to prevent — so the count is asserted too.
    #[test]
    fn a_migrations_declared_tables_are_read_out_of_the_shipped_ddl() {
        let declared = MIGRATIONS[0].relations();
        assert_eq!(
            declared,
            vec![
                "axond_cp_schema_migration",
                "axond_cp_blob",
                "axond_cp_resource_version",
                "axond_cp_resource_dependency",
                "axond_cp_mutation",
                "axond_cp_revision",
                "axond_cp_revision_entry",
                "axond_cp_revision_blob",
                "axond_cp_audit_event",
                "axond_cp_idempotency",
                "axond_cp_head",
            ],
            "the tables adoption looks for are the ones the shipped file creates"
        );
        // Every statement of the shipped file is accounted for — its indexes and
        // its one idempotent seed row as well as its tables — and the ledger is
        // excluded, being an adoption's precondition rather than evidence for it.
        // A statement adoption cannot confirm withdraws `adopt` outright, so this
        // assertion is where that release decision surfaces.
        assert_eq!(
            evidence(&MIGRATIONS[0]),
            Some(vec![
                present(table("axond_cp_blob")),
                present(table("axond_cp_resource_version")),
                present(index("axond_cp_resource_version_tenant_idx")),
                present(table("axond_cp_resource_dependency")),
                present(table("axond_cp_mutation")),
                present(table("axond_cp_revision")),
                present(index("axond_cp_revision_single_root_idx")),
                present(table("axond_cp_revision_entry")),
                present(table("axond_cp_revision_blob")),
                present(table("axond_cp_audit_event")),
                present(index("axond_cp_audit_event_revision_idx")),
                present(table("axond_cp_idempotency")),
                present(index("axond_cp_idempotency_expires_at_idx")),
                present(table("axond_cp_head")),
                present(seed("axond_cp_head")),
            ]),
            "every statement of the shipped file has to be something adoption confirms"
        );
        for migration in MIGRATIONS.iter() {
            assert!(
                evidence(migration)
                    .is_some_and(|declared| declared.iter().any(|item| item.present)),
                "v{} contains a statement adoption cannot confirm, so no database can be adopted \
                 while it ships",
                migration.version
            );
        }
    }

    /// The tenancy migration, which is what the confirmable set has to cover for
    /// `adopt` to be of any use to a v2 deployment: columns, named constraints,
    /// both row-security flags, policies, and the six tables its `DO` block guards
    /// dynamically. The block's list is read out of the block, so a table added to
    /// it is a policy adoption goes looking for.
    #[test]
    fn the_tenancy_migrations_columns_constraints_and_policies_are_all_confirmable() {
        let declared = evidence(&MIGRATIONS[1]).expect(
            "v2's statements have to be confirmable, or `adopt` refuses every v2 deployment",
        );
        for expected in [
            present(table("axond_cp_tenant")),
            present(index("axond_cp_tenant_slug_idx")),
            present(column("axond_cp_mutation", "actor_tenant_id")),
            present(column("axond_cp_audit_event", "actor_principal_id")),
            // Dropped and added again under v1's own name, so it is required of a
            // v2 database without being evidence v2 is what wrote it.
            replaced(constraint(
                "axond_cp_mutation",
                "axond_cp_mutation_actor_attribution",
            )),
            // Dropped by v2, so a v2 database is one where it is *gone*: the
            // constraint being there is what says v2's replacement has not run.
            gone(constraint(
                "axond_cp_mutation",
                "axond_cp_mutation_actor_kind_check",
            )),
            replaced(policy("axond_cp_tenant", "axond_cp_tenant_isolation")),
        ] {
            assert!(
                declared.contains(&expected),
                "v2 has to be adoptable on {}: {declared:#?}",
                named_thing(&expected.what)
            );
        }
        // The `DO` block's loop: every table in its own array, with row security
        // enabled, forced, and its `_isolation` policy present — the `DROP POLICY`
        // before each `CREATE POLICY` being what the file ends with, not what it
        // leaves behind.
        for guarded in [
            "axond_cp_head",
            "axond_cp_revision",
            "axond_cp_revision_entry",
            "axond_cp_revision_blob",
            "axond_cp_blob",
            "axond_cp_resource_dependency",
        ] {
            for expected in [
                present(Evidence::Guarded(guarded.to_owned())),
                present(Evidence::Forced(guarded.to_owned())),
                replaced(policy(guarded, &format!("{guarded}_isolation"))),
            ] {
                assert!(
                    declared.contains(&expected),
                    "the `DO` block's effect on `{guarded}` has to be evidence: {}",
                    named_thing(&expected.what)
                );
            }
        }
    }

    /// Statement kinds, and what each one leaves for adoption to check. The mixed
    /// case is the one worth pinning: a migration that creates a table *and* alters
    /// another must not be adoptable on the strength of the table, because a `psql`
    /// run that stopped between the two leaves exactly that catalogue.
    #[test]
    fn a_statement_whose_effect_cannot_be_confirmed_makes_its_migration_unadoptable() {
        const CONFIRMABLE: Migration = Migration {
            version: 1,
            name: "confirmable",
            sql: "CREATE TABLE IF NOT EXISTS first (id integer);\n\
                  -- A comment mentioning ALTER TABLE and a ';' should not matter.\n\
                  CREATE UNIQUE INDEX IF NOT EXISTS first_id ON first ((id IS NULL));\n\
                  INSERT INTO first (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n",
        };
        assert_eq!(
            evidence(&CONFIRMABLE),
            Some(vec![
                present(table("first")),
                present(index("first_id")),
                present(seed("first")),
            ])
        );

        for sql in [
            // A table beside an `ALTER` clause the catalogue has no answer for: the
            // reviewed mixed case. A default is set or it is not, and `pg_attrdef`
            // holds an expression, not the fact that this migration wrote it.
            "CREATE TABLE IF NOT EXISTS first (id integer);\n\
             ALTER TABLE first ALTER COLUMN id SET DEFAULT 1;\n",
            // An unnamed constraint, which PostgreSQL names for itself: the file
            // says nothing about what to look for.
            "CREATE TABLE IF NOT EXISTS first (id integer);\n\
             ALTER TABLE first ADD CHECK (id > 0);\n",
            // A backfill beside a table.
            "CREATE TABLE IF NOT EXISTS first (id integer);\nUPDATE second SET n = 1;\n",
            // An `INSERT` that is not idempotent: indistinguishable from one that
            // never ran, and doubling on a rerun.
            "CREATE TABLE IF NOT EXISTS first (id integer);\nINSERT INTO first (id) VALUES (1);\n",
            "DROP TABLE second;\n",
            // A block comment is prose, so the statement after it is what it is:
            // read as syntax, its words would supply the missing `CREATE TABLE
            // first` and make this migration adoptable on the strength of a table
            // it never created.
            "/* create table first, if /* nested */ missing */\n\
             UPDATE first SET note = 'x';\n",
            // A `;` inside a dollar-quoted body is not a statement boundary, so
            // the `CREATE TABLE` a trigger would run is not this migration's.
            "CREATE FUNCTION f() RETURNS trigger AS $body$\n\
             BEGIN CREATE TABLE first (id integer); RETURN NULL; END;\n\
             $body$ LANGUAGE plpgsql;\n",
        ] {
            let migration = Migration {
                version: 2,
                name: "mixed",
                sql,
            };
            assert_eq!(
                evidence(&migration),
                None,
                "a statement whose effect nothing can confirm must void the whole migration: {sql}"
            );
        }

        // Prose is not a statement. A file that ends with an explanation, or that
        // has a stray separator, would otherwise withdraw adoption from the entire
        // history over a comment.
        const COMMENTED: Migration = Migration {
            version: 3,
            name: "commented",
            sql: "CREATE TABLE IF NOT EXISTS first (id integer);;\n\
                  -- Why this table exists, after the last statement.\n\
                  -- And a second line of it.\n",
        };
        assert_eq!(evidence(&COMMENTED), Some(vec![present(table("first"))]));

        // A word ending against a quote or a `--` is still that word. Losing it
        // would have the following one answer for it: the table below would be
        // looked for under the name of its column, and the seed would read as a
        // plain `INSERT` and withdraw adoption from the whole history.
        const TIGHT: Migration = Migration {
            version: 4,
            name: "tight",
            sql: "CREATE TABLE IF NOT EXISTS second--the only row holder\n\
                  (id integer PRIMARY KEY, note text);\n\
                  INSERT INTO second (id, note) VALUES (1, 'only')\n\
                  ON CONFLICT (id) DO NOTHING--idempotent by construction\n\
                  ;\n",
        };
        assert_eq!(
            evidence(&TIGHT),
            Some(vec![present(table("second")), present(seed("second"))])
        );
    }

    /// What an `ALTER TABLE` and a policy leave behind, clause by clause — the
    /// evidence v2's own statements turn into. A clause list is one statement and
    /// several answers, and a `DROP` is confirmed by the thing being gone, which is
    /// only ever half of a story: a migration that just takes things away is
    /// unadoptable, because a database that never had the version before it looks
    /// exactly the same.
    #[test]
    fn an_alter_is_read_clause_by_clause_and_a_drop_is_confirmed_by_absence() {
        const ALTERED: Migration = Migration {
            version: 1,
            name: "altered",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  ALTER TABLE one\n\
                      ADD COLUMN IF NOT EXISTS note text NULL,\n\
                      ADD COLUMN IF NOT EXISTS more text NULL;\n\
                  ALTER TABLE ONLY one\n\
                      DROP CONSTRAINT IF EXISTS one_note_ck,\n\
                      ADD CONSTRAINT one_note_ck CHECK (note IS NULL OR more IS NULL),\n\
                      ENABLE ROW LEVEL SECURITY,\n\
                      FORCE ROW LEVEL SECURITY;\n\
                  DROP POLICY IF EXISTS one_isolation ON one;\n\
                  CREATE POLICY one_isolation ON one USING (id > 0);\n",
        };
        assert_eq!(
            evidence(&ALTERED),
            Some(vec![
                present(table("one")),
                present(column("one", "note")),
                present(column("one", "more")),
                // Dropped and recreated under the same name: what the file leaves
                // behind is the last thing it did to each one, and a database that
                // held the earlier definition holds this name too.
                replaced(constraint("one", "one_note_ck")),
                present(Evidence::Guarded("one".to_owned())),
                present(Evidence::Forced("one".to_owned())),
                replaced(policy("one", "one_isolation")),
            ])
        );

        // A migration that only removes things: each `DROP` is confirmable, and
        // none of it is evidence the version ran, so there is nothing to adopt.
        const REMOVED: Migration = Migration {
            version: 1,
            name: "removed",
            sql: "ALTER TABLE one DROP COLUMN note, DISABLE ROW LEVEL SECURITY;\n\
                  DROP POLICY one_isolation ON one;\n",
        };
        assert_eq!(
            evidence(&REMOVED),
            Some(vec![
                gone(column("one", "note")),
                gone(Evidence::Guarded("one".to_owned())),
                gone(policy("one", "one_isolation")),
            ])
        );
        let Baseline::Inconsistent { message } = reconcile(&[REMOVED], &HashSet::new()) else {
            panic!("a migration that only removes things proves nothing by itself");
        };
        assert!(
            message.contains("v1 `removed` contains a statement"),
            "the refusal has to name the version nothing can account for: {message}"
        );

        // A version that both adds and removes, over a database that has neither:
        // the removal is satisfied by an untouched schema, so counting it would
        // read "nothing was applied" as "half of v2 was".
        const ADDS_AND_REMOVES: Migration = Migration {
            version: 2,
            name: "replaces",
            sql: "ALTER TABLE one DROP CONSTRAINT one_note_ck, ADD CONSTRAINT one_note_ck2 \
                  CHECK (note IS NOT NULL);\n",
        };
        const CREATES: Migration = Migration {
            version: 1,
            name: "creates",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n",
        };
        assert_eq!(
            reconcile(&[CREATES, ADDS_AND_REMOVES], &HashSet::new()),
            Baseline::Nothing,
            "an untouched database is untouched, not part way through the version above it"
        );
        assert_eq!(
            reconcile(&[CREATES, ADDS_AND_REMOVES], &HashSet::from([table("one")])),
            Baseline::Applied { versions: vec![1] },
            "v1's table is v1's baseline, with v2 still pending"
        );
        assert_eq!(
            reconcile(
                &[CREATES, ADDS_AND_REMOVES],
                &HashSet::from([table("one"), constraint("one", "one_note_ck2")])
            ),
            Baseline::Applied {
                versions: vec![1, 2]
            },
            "the constraint v2 adds, with the one it drops gone, is v2 applied"
        );

        // The shape v2 uses on v1's constraints: dropped, then added again under
        // the same name. A v1 database and a v2 database both hold `one_note_ck`
        // afterwards, so its presence cannot say which of them wrote it — read as
        // proof, it would refuse every v1-only database as half-way through v2.
        const REWRITES: Migration = Migration {
            version: 2,
            name: "rewrites",
            sql: "ALTER TABLE one\n\
                      DROP CONSTRAINT IF EXISTS one_note_ck,\n\
                      ADD CONSTRAINT one_note_ck CHECK (note IS NOT NULL),\n\
                      ADD COLUMN IF NOT EXISTS more text NULL;\n",
        };
        let v1_only = HashSet::from([table("one"), constraint("one", "one_note_ck")]);
        assert_eq!(
            reconcile(&[CREATES, REWRITES], &v1_only),
            Baseline::Applied { versions: vec![1] },
            "a constraint v1 leaves behind too is not evidence v2 rewrote it"
        );
        let mut applied = v1_only.clone();
        applied.insert(column("one", "more"));
        assert_eq!(
            reconcile(&[CREATES, REWRITES], &applied),
            Baseline::Applied {
                versions: vec![1, 2]
            },
            "the column only v2 adds is what says v2 ran"
        );
        // Not being proof is not the same as not being checked: a database v2 ran
        // on that is missing the rewritten constraint is still partly applied.
        let Baseline::Inconsistent { message } = reconcile(
            &[CREATES, REWRITES],
            &HashSet::from([table("one"), column("one", "more")]),
        ) else {
            panic!("v2's own constraint has to be required of a database v2 ran on");
        };
        assert!(
            message.contains("`one`'s `one_note_ck` constraint is not present"),
            "the refusal has to name the constraint that is missing: {message}"
        );

        // A clause the catalogue cannot answer for, and an `ADD` without the
        // `COLUMN` keyword — where the word after `ADD` is a name in one form and a
        // keyword in the next — are unconfirmable rather than read as a guess.
        for sql in [
            "ALTER TABLE one ALTER COLUMN note TYPE integer;\n",
            "ALTER TABLE one ADD note text;\n",
            "ALTER TABLE one ADD PRIMARY KEY (id);\n",
            "ALTER TABLE one ADD COLUMN note text, ALTER COLUMN id DROP NOT NULL;\n",
            "CREATE POLICY one_isolation ON other.one USING (true);\n",
            "CREATE POLICY one_isolation FOR SELECT USING (true);\n",
        ] {
            let unconfirmable = Migration {
                version: 1,
                name: "unconfirmable",
                sql,
            };
            assert_eq!(
                evidence(&unconfirmable),
                None,
                "a clause nothing can be asked about voids its migration: {sql}"
            );
        }
    }

    /// The dynamic form v2 guards its chained tables with: a loop over a literal
    /// list of names executing `format()` templates. Read by rendering the block's
    /// own templates for the block's own names and parsing the result, so the
    /// evidence follows the file — and anything outside that one shape is
    /// unconfirmable, which is the same answer a backfill gets.
    #[test]
    fn a_dynamic_loop_is_evidence_for_the_statements_it_renders_and_nothing_else() {
        const LOOPED: Migration = Migration {
            version: 1,
            name: "looped",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  DO $$\n\
                  DECLARE\n\
                      chained text;\n\
                  BEGIN\n\
                      FOREACH chained IN ARRAY ARRAY['one', 'two'] LOOP\n\
                          EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', chained);\n\
                          EXECUTE format('DROP POLICY IF EXISTS %I ON %I', chained || '_isolation', chained);\n\
                          EXECUTE format(\n\
                              'CREATE POLICY %I ON %I USING (%s)',\n\
                              chained || '_isolation',\n\
                              chained,\n\
                              'current_setting(''axond.tenant_id'', true) IS NOT NULL'\n\
                          );\n\
                      END LOOP;\n\
                  END\n\
                  $$;\n",
        };
        assert_eq!(
            evidence(&LOOPED),
            Some(vec![
                present(table("one")),
                present(Evidence::Guarded("one".to_owned())),
                // Each policy is dropped before it is created, so it is required
                // afterwards without saying which version wrote it.
                replaced(policy("one", "one_isolation")),
                present(Evidence::Guarded("two".to_owned())),
                replaced(policy("two", "two_isolation")),
            ]),
            "the loop's evidence is its templates rendered for its own names"
        );

        for body in [
            // A condition: whether the branch was taken is not in the file.
            "IF found THEN EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', chained); END IF;",
            // Names from a query rather than from a literal list.
            "FOREACH chained IN ARRAY (SELECT array_agg(relname) FROM pg_class) LOOP \
             EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', chained); END LOOP;",
            // A template argument that is not the loop value.
            "FOREACH chained IN ARRAY ARRAY['one'] LOOP \
             EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', other); END LOOP;",
            // A rendered statement that is itself unconfirmable.
            "FOREACH chained IN ARRAY ARRAY['one'] LOOP \
             EXECUTE format('UPDATE %I SET note = 1', chained); END LOOP;",
            // A placeholder with no argument, and one this parse does not render.
            "FOREACH chained IN ARRAY ARRAY['one'] LOOP \
             EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY'); END LOOP;",
            "FOREACH chained IN ARRAY ARRAY['one'] LOOP \
             EXECUTE format('CREATE POLICY %I ON %L USING (true)', 'p', chained); END LOOP;",
            // Dynamic SQL that is not a `format()` template at all.
            "FOREACH chained IN ARRAY ARRAY['one'] LOOP \
             EXECUTE 'ALTER TABLE one ENABLE ROW LEVEL SECURITY'; END LOOP;",
        ] {
            let block = format!("DO $$\nDECLARE\n chained text;\nBEGIN\n {body}\nEND\n$$");
            assert_eq!(
                expectations(&block),
                None,
                "a block outside the one shape this reads is unconfirmable: {block}"
            );
        }
    }

    /// The deferred-constraint migration, which is the shape v2 was not: it
    /// creates no table, guards every add with `IF NOT EXISTS (SELECT ...)`, drops
    /// the indexes its constraints replace, and clears definitions PostgreSQL named
    /// for itself — found by a catalogue query, because the file cannot name them.
    #[test]
    fn the_deferred_constraint_migrations_guards_and_cleanups_are_all_confirmable() {
        let declared = evidence(&MIGRATIONS[2])
            .expect("v3's statements have to be confirmable, or `adopt` refuses every deployment");
        for expected in [
            // The undeferrable indexes v2 created, which v3 replaces with
            // constraints: a v3 database is one where they are gone, and one that
            // still has them is one v3 has not run.
            gone(index("axond_cp_tenant_slug_idx")),
            present(constraint("axond_cp_tenant", "axond_cp_tenant_slug_unique")),
            present(constraint(
                "axond_cp_project",
                "axond_cp_project_slug_unique",
            )),
            present(constraint(
                "axond_cp_principal",
                "axond_cp_principal_key_digest_unique",
            )),
            present(constraint(
                "axond_cp_principal",
                "axond_cp_principal_project_fkey",
            )),
            // Added under the name v1 and v2 used — dropped by the loop above it
            // rather than by name, so what v3's own text leaves is the constraint
            // being there. Its presence stops being proof of v3 in `reconcile`,
            // where the other versions that declare it are in view.
            present(constraint(
                "axond_cp_mutation",
                "axond_cp_mutation_actor_attribution",
            )),
            replaced(policy("axond_cp_mutation", "axond_cp_mutation_isolation")),
        ] {
            assert!(
                declared.contains(&expected),
                "v3 has to be adoptable on {}: {declared:#?}",
                named_thing(&expected.what)
            );
        }

        // The four loops that drop what they find. Absence only, and never proof:
        // a database that never had the inline definitions answers exactly as one
        // v3 cleaned up. What the file goes on to declare by name is exempt — v3's
        // journal loop matches every check mentioning `actor_kind` and then adds
        // one that does, so "the query names nothing" would be false of a database
        // that ran it.
        let cleared: Vec<&Expectation> = declared
            .iter()
            .filter(|item| matches!(item.what, Evidence::Stale { .. }))
            .collect();
        assert_eq!(
            cleared.len(),
            4,
            "each of v3's cleanup loops is evidence: {declared:#?}"
        );
        assert!(
            cleared.iter().all(|item| !item.present && !item.proof),
            "a definition being gone is required of a v3 database and proof of nothing"
        );
        assert!(
            cleared.iter().any(|item| matches!(
                &item.what,
                Evidence::Stale { table, except, .. }
                    if table == "axond_cp_mutation"
                        && except.contains(&"axond_cp_mutation_actor_attribution".to_owned())
            )),
            "the journal's loop has to admit the check v3 adds itself: {declared:#?}"
        );
    }

    /// The two dynamic shapes v3 adds, and the fail-closed edges of each: an add
    /// guarded by a query is evidence only when the guard asks about the very thing
    /// it guards, and a loop that drops what its query finds is summarised by that
    /// query — so the query has to be a single catalogue read naming the
    /// constraints it selects, and the loop has to do nothing but drop them.
    #[test]
    fn a_guarded_add_and_a_cleanup_loop_are_read_only_in_the_shapes_they_summarise() {
        let block = |body: &str| format!("DO $$\nDECLARE\n stale record;\nBEGIN\n {body}\nEND\n$$");

        let guarded = block(
            "IF NOT EXISTS (\
               SELECT 1 FROM pg_constraint \
                WHERE conrelid = 'one'::regclass AND conname = 'one_unique'\
             ) THEN \
               ALTER TABLE one ADD CONSTRAINT one_unique UNIQUE (id) DEFERRABLE; \
             END IF;",
        );
        assert_eq!(
            expectations(&guarded),
            Some(vec![present(constraint("one", "one_unique"))]),
            "a guard that asks about the constraint it adds leaves that constraint"
        );

        let looped = block(
            "FOR stale IN \
               SELECT conname FROM pg_constraint \
                WHERE conrelid = 'one'::regclass AND contype = 'u' \
             LOOP \
               EXECUTE format('ALTER TABLE one DROP CONSTRAINT %I', stale.conname); \
             END LOOP;",
        );
        let cleared = expectations(&looped);
        let Some([expectation]) = cleared.as_deref() else {
            panic!("a loop that drops what its own query names is one piece of evidence");
        };
        assert!(
            matches!(&expectation.what, Evidence::Stale { table, .. } if table == "one")
                && !expectation.present
                && !expectation.proof,
            "the loop's evidence is its query naming nothing on `one`: {expectation:?}"
        );

        for body in [
            // A guard about something other than what it guards: whether the
            // branch was taken is then not something the effect can answer.
            "IF NOT EXISTS (\
               SELECT 1 FROM pg_constraint WHERE conname = 'other_unique'\
             ) THEN ALTER TABLE one ADD CONSTRAINT one_unique UNIQUE (id); END IF;",
            // A guard that is not a single catalogue read.
            "IF NOT EXISTS (\
               UPDATE one SET id = 1 RETURNING id\
             ) THEN ALTER TABLE one ADD CONSTRAINT one_unique UNIQUE (id); END IF;",
            // A guarded effect that is unconfirmable in its own right.
            "IF NOT EXISTS (\
               SELECT 1 FROM pg_constraint WHERE conname = 'one_unique'\
             ) THEN UPDATE one SET note = 1; END IF;",
            // A loop that writes as well as drops: its query naming nothing
            // afterwards says nothing about what else it did.
            "FOR stale IN SELECT conname FROM pg_constraint WHERE contype = 'u' LOOP \
               EXECUTE format('ALTER TABLE one DROP CONSTRAINT %I', stale.conname); \
               EXECUTE format('ALTER TABLE one ADD CONSTRAINT %I UNIQUE (id)', stale.conname); \
             END LOOP;",
            // A loop that drops a fixed name rather than what it found.
            "FOR stale IN SELECT conname FROM pg_constraint WHERE contype = 'u' LOOP \
               EXECUTE format('ALTER TABLE %I DROP CONSTRAINT one_unique', 'one'); \
             END LOOP;",
            // A query the probe cannot ask again: not a read, not of the
            // constraint catalogue, or not naming the constraints it selects.
            "FOR stale IN DELETE FROM pg_constraint RETURNING conname LOOP \
               EXECUTE format('ALTER TABLE one DROP CONSTRAINT %I', stale.conname); \
             END LOOP;",
            "FOR stale IN SELECT relname AS conname FROM pg_class LOOP \
               EXECUTE format('ALTER TABLE one DROP CONSTRAINT %I', stale.conname); \
             END LOOP;",
            "FOR stale IN SELECT oid FROM pg_constraint LOOP \
               EXECUTE format('ALTER TABLE one DROP CONSTRAINT %I', stale.oid); \
             END LOOP;",
        ] {
            let block = block(body);
            assert_eq!(
                expectations(&block),
                None,
                "a block outside the shapes this reads is unconfirmable: {block}"
            );
        }
    }

    /// A prefix is read with the versions above it in view, because a later
    /// migration takes earlier ones' objects away: v3 drops the indexes v2 created.
    /// The longest prefix the database can account for is the baseline — an index
    /// that is gone is v3's doing when v3 is claimed and a missing effect when it
    /// is not, and neither reading may be used to record a version the database
    /// cannot show.
    #[test]
    fn a_later_migration_taking_an_earlier_ones_object_away_is_read_as_the_prefix_it_is() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "indexed",
            sql: "CREATE TABLE IF NOT EXISTS two (id integer);\n\
                  CREATE INDEX IF NOT EXISTS one_id ON one (id);\n",
        };
        const V3: Migration = Migration {
            version: 3,
            name: "constrained",
            sql: "DROP INDEX IF EXISTS one_id;\n\
                  ALTER TABLE one ADD CONSTRAINT one_id_unique UNIQUE (id);\n",
        };
        let shipped = [V1, V2, V3];
        let one = table("one");
        let two = table("two");
        let indexed = index("one_id");
        let constrained = constraint("one", "one_id_unique");

        assert_eq!(
            reconcile(
                &shipped,
                &HashSet::from([one.clone(), two.clone(), indexed.clone()])
            ),
            Baseline::Applied {
                versions: vec![1, 2]
            },
            "a database with the index v3 drops is one v3 has not run"
        );
        assert_eq!(
            reconcile(
                &shipped,
                &HashSet::from([one.clone(), two.clone(), constrained.clone()])
            ),
            Baseline::Applied {
                versions: vec![1, 2, 3]
            },
            "the index being gone is what v3 leaves, so its absence is not a hole"
        );

        // Neither: the index is gone and the constraint that replaces it was never
        // added, so one of the two stopped in the middle. No prefix describes that,
        // and recording one would leave `apply` to finish a migration that has
        // already had part of its effect.
        let Baseline::Inconsistent { message } =
            reconcile(&shipped, &HashSet::from([one.clone(), two.clone()]))
        else {
            panic!("a database part-way through the replacement has no adoptable baseline");
        };
        assert!(
            message.contains("`one_id` is not present"),
            "the refusal names the index the prefix it claims leaves behind: {message}"
        );
        assert_eq!(
            reconcile(&shipped, &HashSet::from([one, two, indexed, constrained])),
            Baseline::Inconsistent {
                message: "v3 `constrained` is only partly applied: `one_id` is still present, so \
                          this build cannot record it as applied and cannot apply it over what is \
                          there either. Finish or undo that migration by hand, then re-run."
                    .to_owned()
            },
            "a database with both is one where v3's `DROP INDEX` has not run"
        );
    }

    /// `CREATE TABLE` with and without `IF NOT EXISTS`, a name followed by a
    /// newline rather than a paren, and an index or an `ALTER` that creates no
    /// table: the parse has to be the file's tables and nothing else, because a
    /// name it invents is a table adoption looks for and never finds.
    #[test]
    fn declared_tables_are_parsed_from_either_create_form_and_nothing_else() {
        const MIXED: Migration = Migration {
            version: 7,
            name: "fixture",
            sql: "CREATE TABLE IF NOT EXISTS first (id integer);\n\
                  CREATE TABLE second\n(id integer);\n\
                  CREATE INDEX IF NOT EXISTS second_id ON second (id);\n\
                  ALTER TABLE first ADD COLUMN note text;\n\
                  CREATE TABLE IF NOT EXISTS first (id integer);\n",
        };
        assert_eq!(MIXED.relations(), vec!["first", "second"]);

        // Two legal forms whose object this parse cannot name: an unnamed index,
        // and a name qualified with a schema of its own. Read positionally they
        // would be an index called `ON` and a table called `other` — objects the
        // probe would look for, never find, and name in a refusal that sends an
        // operator after something that does not exist. Unconfirmable instead, so
        // the migration is unadoptable and the refusal says why.
        for sql in [
            "CREATE INDEX ON second (id);\n",
            "CREATE UNIQUE INDEX CONCURRENTLY ON second (id);\n",
            "CREATE TABLE other.third (id integer);\n",
            "INSERT INTO other.third (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n",
        ] {
            let unnameable = Migration {
                version: 8,
                name: "unnameable",
                sql,
            };
            assert_eq!(
                evidence(&unnameable),
                None,
                "an object this parse cannot name is unconfirmable, not absent: {sql}"
            );
        }
    }

    /// A migration that creates no table — an `ALTER`-only or backfill migration,
    /// the first non-idempotent kind — blocks adoption of the whole database,
    /// wherever in the shipped history it sits. Adopting the prefix underneath it
    /// would look safe and would not be: the ledger would then report the opaque
    /// version as pending, and the next `apply` would run it over a schema that may
    /// already have had it applied out of band, which is exactly the replay
    /// adoption exists to prevent.
    #[test]
    fn a_migration_no_object_can_account_for_blocks_adoption_of_the_whole_history() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS axond_cp_schema_migration (version integer);\n\
                  CREATE TABLE IF NOT EXISTS one (id integer);\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "backfill",
            sql: "UPDATE one SET note = 'x';\n",
        };
        const V3: Migration = Migration {
            version: 3,
            name: "third",
            sql: "CREATE TABLE IF NOT EXISTS three (id integer);\n",
        };
        let shipped = &[V1, V2, V3];
        let one = table("one");
        let three = table("three");

        // Every state of such a database refuses, including the one where the
        // prefix below the opaque migration is entirely accounted for.
        for confirmed in [
            HashSet::from([one.clone()]),
            HashSet::from([one.clone(), three.clone()]),
            HashSet::from([three.clone()]),
            HashSet::new(),
        ] {
            let Baseline::Inconsistent { message } = reconcile(shipped, &confirmed) else {
                panic!("a history with an unconfirmable migration has no adoptable baseline");
            };
            assert!(
                message.contains("v2 `backfill` contains a statement")
                    && message.contains("re-run it"),
                "the refusal has to name the version and why nothing under it is safe: {message}"
            );
        }

        // The same reconciliation without that migration still adopts the prefix
        // its objects prove, so the refusals above are the opaque version's doing
        // rather than a blanket one.
        let confirmable = &[V1, V3];
        assert_eq!(
            reconcile(confirmable, &HashSet::from([one])),
            Baseline::Applied { versions: vec![1] }
        );
        assert_eq!(reconcile(confirmable, &HashSet::new()), Baseline::Nothing);
        let Baseline::Inconsistent { message } = reconcile(confirmable, &HashSet::from([three]))
        else {
            panic!("a hole in the applied prefix is not a baseline");
        };
        assert!(
            message.contains("not a prefix"),
            "the refusal has to say why the objects describe no baseline: {message}"
        );
    }

    /// A seed is confirmed by its target having a row, which is one answer for the
    /// whole table. Two migrations seeding the same table would both read as
    /// applied off whichever row is there, so a database that only ever had the
    /// first would have the second recorded and `apply` would never write its row —
    /// the fail-open direction, and the one this refuses.
    #[test]
    fn a_second_seed_into_an_already_seeded_table_blocks_adoption() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  INSERT INTO one (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "second seed",
            sql: "INSERT INTO one (id) VALUES (2) ON CONFLICT (id) DO NOTHING;\n",
        };
        let one = table("one");
        let seeded = seed("one");

        // Including the state where the table has a row: that row is v1's, and
        // nothing here can tell whether v2's is beside it.
        for confirmed in [
            HashSet::from([one.clone(), seeded.clone()]),
            HashSet::from([one.clone()]),
            HashSet::new(),
        ] {
            let Baseline::Inconsistent { message } = reconcile(&[V1, V2], &confirmed) else {
                panic!("a seed no row can be attributed to has no adoptable baseline");
            };
            assert!(
                message.contains("which the shipped history seeds more than once"),
                "the refusal has to say why a row in it proves nothing: {message}"
            );
        }

        // The same table seeded twice inside one file, which a `psql -f` can stop
        // between: the row that is there is the first insert's, so the second is
        // unprovable for exactly the same reason and must not be de-duplicated
        // away before the refusal can see it.
        const TWICE: Migration = Migration {
            version: 1,
            name: "twice",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  INSERT INTO one (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n\
                  INSERT INTO one (id) VALUES (2) ON CONFLICT (id) DO NOTHING;\n",
        };
        assert_eq!(
            evidence(&TWICE),
            Some(vec![
                present(one.clone()),
                present(seeded.clone()),
                present(seeded.clone()),
            ]),
            "a repeated seed is two expectations, unlike a relation declared twice"
        );
        let Baseline::Inconsistent { message } =
            reconcile(&[TWICE], &HashSet::from([one.clone(), seeded.clone()]))
        else {
            panic!("a file seeding one table twice has no adoptable baseline");
        };
        assert!(
            message.contains("which the shipped history seeds more than once"),
            "the refusal has to say why a row in it proves nothing: {message}"
        );

        // One migration seeding it once is still evidence, so the refusals above
        // are the second seed's doing rather than a withdrawal of seed evidence.
        assert_eq!(
            reconcile(&[V1], &HashSet::from([one, seeded])),
            Baseline::Applied { versions: vec![1] }
        );
    }

    /// The same argument as the shared seed, for a relation: a table being there
    /// proves at most one of the `CREATE TABLE IF NOT EXISTS` statements that
    /// declare it. A later migration that only re-declares an earlier one's
    /// objects would otherwise come out with nothing missing and be recorded as
    /// applied over a database that never ran it, which `apply` would then never
    /// put right.
    #[test]
    fn an_object_more_than_one_migration_declares_blocks_adoption() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "re-declares",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  CREATE INDEX IF NOT EXISTS one_id ON one (id);\n",
        };
        let one = table("one");
        let declared = index("one_id");

        for confirmed in [
            HashSet::from([one.clone(), declared.clone()]),
            HashSet::from([one.clone()]),
            HashSet::new(),
        ] {
            let Baseline::Inconsistent { message } = reconcile(&[V1, V2], &confirmed) else {
                panic!("an object no version can be attributed to has no adoptable baseline");
            };
            assert!(
                message.contains("acts on `one`, which more than one shipped migration acts on"),
                "the refusal has to say why the object's presence proves nothing: {message}"
            );
        }

        // A migration declaring it once is still evidence, and a version whose
        // own objects are its own still adopts.
        const V2_OWN: Migration = Migration {
            version: 2,
            name: "own objects",
            sql: "CREATE TABLE IF NOT EXISTS two (id integer);\n",
        };
        assert_eq!(
            reconcile(&[V1, V2_OWN], &HashSet::from([one, table("two")])),
            Baseline::Applied {
                versions: vec![1, 2]
            }
        );
    }

    /// A comment or a literal that never closes swallows the rest of the file,
    /// and what it swallows might have been the `ALTER` that made the migration
    /// unadoptable — so an uncertain parse is a refusal rather than a shorter
    /// list of evidence. The reachable case is `E'...\'...'`: the escape is
    /// syntax this parse does not track, so the literal reads as closing early
    /// and the leftover quote opens a region running to the next apostrophe
    /// anywhere in the file.
    #[test]
    fn a_region_that_does_not_close_where_this_parse_says_makes_the_migration_unadoptable() {
        for sql in [
            // A backslash escape: everything from the stray quote onwards,
            // `ALTER` included, would otherwise vanish from the evidence.
            "CREATE TABLE IF NOT EXISTS one (id integer, note text DEFAULT E'a\\'b');\n\
             ALTER TABLE one ADD COLUMN more text;\n",
            "CREATE TABLE IF NOT EXISTS one (id integer);\n/* never closed\n",
            "CREATE TABLE IF NOT EXISTS one (id integer);\nINSERT INTO one VALUES ('open);\n",
            "CREATE FUNCTION f() RETURNS void AS $body$ BEGIN END;\n",
        ] {
            let unlexable = Migration {
                version: 9,
                name: "unlexable",
                sql,
            };
            assert_eq!(
                evidence(&unlexable),
                None,
                "a region this parse cannot close makes the file unconfirmable: {sql}"
            );
        }

        // A file that simply ends in a line comment closes fine, and a `$1`
        // parameter is not a dollar quote.
        assert!(lexed(
            "CREATE TABLE IF NOT EXISTS one (id integer);\n-- and that is all"
        ));
        assert!(lexed("CREATE POLICY p ON one USING (id = $1);\n"));

        // The same rule inside a block. The file-level scan skips a `$$ ... $$`
        // region whole, so this is the first reading of its contents: a literal
        // that does not close where it says would otherwise swallow the rest of
        // the block and shorten its evidence instead of voiding it.
        let inside = Migration {
            version: 9,
            name: "unlexable_block",
            sql: "DO $$\nBEGIN\n  \
                  EXECUTE format('ALTER TABLE one ADD COLUMN note text DEFAULT E''a\\''b''');\n  \
                  ALTER TABLE one ENABLE ROW LEVEL SECURITY;\nEND\n$$;\n",
        };
        assert_eq!(
            evidence(&inside),
            None,
            "a region a block's own body does not close makes the migration unconfirmable"
        );
    }

    /// A forward-only history is applied in order, so objects only a later
    /// version creates mean this database is not the prefix underneath it — even
    /// when that prefix accounts for everything it declares. Rounding down would
    /// record a history the database does not have and leave `apply` to run the
    /// skipped file over objects that are already there.
    #[test]
    fn a_skipped_middle_version_is_refused_rather_than_recorded_as_the_prefix_below_it() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "second",
            sql: "CREATE TABLE IF NOT EXISTS two (id integer);\n",
        };
        const V3: Migration = Migration {
            version: 3,
            name: "third",
            sql: "CREATE TABLE IF NOT EXISTS three (id integer);\n",
        };
        let shipped = [V1, V2, V3];

        let Baseline::Inconsistent { message } =
            reconcile(&shipped, &HashSet::from([table("one"), table("three")]))
        else {
            panic!("v3's table without v2's is not a prefix of the shipped history");
        };
        assert!(
            message.contains("v3 `third` declares objects that are present"),
            "the refusal names the version whose objects are there out of order: {message}"
        );

        // The prefix itself is still adoptable when nothing above it is there.
        assert_eq!(
            reconcile(&shipped, &HashSet::from([table("one")])),
            Baseline::Applied { versions: vec![1] },
            "v1 alone is the ordinary hand-applied prefix"
        );
    }
}
