//! The journal contract, asserted against the in-memory oracle.
//!
//! These are the properties a durable implementation has to reproduce, written
//! as the failures they exist to prevent: an event lost because the process died
//! between settlement and delivery, an event delivered twice and billed twice, a
//! caller's events reordered, one bad event stalling every later one, and a
//! journal that fills up without saying so.

use std::time::{Duration, SystemTime};

use super::super::ObservedRecord;
use super::super::identity::next_request_id;
use super::super::tests::sample_record;
use super::oracle::InMemoryUsageJournal;
use super::*;

/// A settled record for `subject`, with a fresh event identity.
fn event_for(subject: &str) -> UsageEvent {
    let mut record = sample_record();
    record.request_id = next_request_id().to_string();
    record.subject = subject.to_owned();
    UsageEvent::new(ObservedRecord::now(record)).expect("a minted id is an event identity")
}

fn event() -> UsageEvent {
    event_for("GW_INBOUND_ACME_KEY")
}

fn consumer(name: &str) -> ConsumerId {
    ConsumerId::parse(name).expect("a valid consumer name")
}

fn claim_of(max_events: usize, now: SystemTime) -> Claim {
    Claim {
        max_events,
        lease: Duration::from_secs(30),
        now,
    }
}

fn claim(max_events: usize) -> Claim {
    claim_of(max_events, SystemTime::now())
}

#[test]
fn a_record_that_lost_its_identity_is_not_an_event() {
    let mut record = sample_record();
    // The shape a counter-era writer produced. Journaling it would mean minting a
    // new identity for an event a consumer may already hold under another one.
    record.request_id = "req_0000000000000001".to_owned();
    let error = UsageEvent::new(ObservedRecord::now(record)).expect_err("must be refused");
    assert!(matches!(error, InvalidEvent::Identity { .. }), "{error:?}");
}

#[test]
fn an_event_carries_the_key_a_consumer_deduplicates_on() {
    let event = event();
    assert_eq!(event.idempotency_key().as_str(), event.id().to_string());
    assert_eq!(
        event.idempotency_key().as_str(),
        event.record().request_id,
        "the key and the stored column must be the same string"
    );
    assert_eq!(
        event.ordering_key(),
        &OrderingKey {
            namespace: event.record().namespace.clone(),
            subject: event.record().subject.clone(),
        }
    );
}

#[test]
fn the_default_delivery_mode_promises_nothing_durable() {
    assert_eq!(DeliveryMode::default(), DeliveryMode::TelemetryGrade);
    assert!(!DeliveryMode::default().is_durable());
    assert!(DeliveryMode::BillingGrade.is_durable());
    // The oracle implements every operation and still refuses to claim the
    // guarantee, because an in-memory log cannot keep it.
    assert_eq!(
        InMemoryUsageJournal::new().mode(),
        DeliveryMode::TelemetryGrade
    );
}

#[test]
fn a_billing_grade_journal_refuses_rather_than_loses_by_default() {
    assert_eq!(Capacity::BILLING_GRADE.policy, CapacityPolicy::Refuse);
    assert!(!Capacity::BILLING_GRADE.policy.can_lose_events());
    assert!(CapacityPolicy::DropOldest.can_lose_events());
}

#[tokio::test]
async fn appending_the_same_event_twice_journals_it_once() {
    let journal = InMemoryUsageJournal::new();
    let event = event();
    let first = journal.append(&event).await.expect("append");
    let second = journal.append(&event).await.expect("re-append");
    assert!(first.is_new());
    assert!(!second.is_new(), "{second:?}");
    assert_eq!(first.position(), second.position());
    assert_eq!(journal.stored_events(), 1);
    assert_eq!(
        journal
            .stats(&consumer("billing"))
            .await
            .expect("stats")
            .pending,
        1
    );
}

#[tokio::test]
async fn the_same_identity_with_different_content_is_a_conflict() {
    let journal = InMemoryUsageJournal::new();
    let event = event();
    journal.append(&event).await.expect("append");

    let mut mutated = event.record().clone();
    mutated.cost_microdollars += 1;
    let mutated = UsageEvent::new(ObservedRecord {
        record: mutated,
        observed_at: event.observed_at(),
    })
    .expect("identity is unchanged");

    let error = journal
        .append(&mutated)
        .await
        .expect_err("a reused identity must not overwrite a journaled fact");
    assert!(
        matches!(&error, JournalError::Conflict { key } if key == event.idempotency_key()),
        "{error:?}"
    );
    assert_eq!(journal.stored_events(), 1);
}

/// The failure #155 exists for: the request settled, the process died before any
/// sink acknowledged the event, and the event has to still be there.
#[tokio::test]
async fn a_crash_after_settlement_keeps_the_event_deliverable() {
    let journal = InMemoryUsageJournal::new();
    let event = event();
    journal.append(&event).await.expect("append");

    let restarted = journal.restart();
    let claimed = restarted
        .claim(&consumer("billing"), claim(10))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].event, event);
    assert_eq!(claimed[0].id.attempt, 1);
    assert!(!claimed[0].id.is_redelivery());
}

#[tokio::test]
async fn a_restart_resumes_delivery_state_instead_of_redelivering_everything() {
    let journal = InMemoryUsageJournal::new();
    let billing = consumer("billing");
    let first = event_for("acme-one");
    let second = event_for("acme-two");
    journal.append(&first).await.expect("append");
    journal.append(&second).await.expect("append");

    let claimed = journal.claim(&billing, claim(1)).await.expect("claim");
    assert_eq!(claimed[0].event, first);
    journal.ack(&claimed[0].id).await.expect("ack");

    let restarted = journal.restart();
    // Well past the lease, so nothing is held back by an in-flight claim.
    let later = SystemTime::now() + Duration::from_secs(300);
    let replayed = restarted
        .claim(&billing, claim_of(10, later))
        .await
        .expect("claim");
    assert_eq!(
        replayed.iter().map(|d| d.event.id()).collect::<Vec<_>>(),
        vec![second.id()],
        "an acknowledged event must not be delivered again"
    );
}

#[tokio::test]
async fn an_expired_lease_redelivers_the_same_event_as_a_new_attempt() {
    let journal = InMemoryUsageJournal::new();
    let billing = consumer("billing");
    let event = event();
    journal.append(&event).await.expect("append");

    let now = SystemTime::now();
    let first = journal
        .claim(&billing, claim_of(10, now))
        .await
        .expect("claim");
    assert_eq!(first.len(), 1);

    // While the lease holds, the event is invisible: two workers must not
    // deliver it at once.
    assert!(
        journal
            .claim(&billing, claim_of(10, now + Duration::from_secs(1)))
            .await
            .expect("claim")
            .is_empty()
    );

    let second = journal
        .claim(&billing, claim_of(10, now + Duration::from_secs(31)))
        .await
        .expect("claim");
    assert_eq!(second.len(), 1);
    let (first, second) = (&first[0], &second[0]);
    assert_eq!(
        first.event.idempotency_key(),
        second.event.idempotency_key(),
        "a replay is the same billable fact"
    );
    assert_ne!(first.id, second.id, "and a distinguishable attempt");
    assert_eq!(second.id.attempt, 2);
    assert!(second.id.is_redelivery());
    assert_eq!(
        second.id.to_string(),
        format!("billing:{}#2", event.id()),
        "a delivery identity names the consumer, the event, and the attempt"
    );
}

#[tokio::test]
async fn acknowledging_twice_acknowledges_once() {
    let journal = InMemoryUsageJournal::new();
    let billing = consumer("billing");
    journal.append(&event()).await.expect("append");
    let claimed = journal.claim(&billing, claim(10)).await.expect("claim");

    journal.ack(&claimed[0].id).await.expect("ack");
    // The recovery case: the destination row was written, the acknowledgement's
    // outcome was unknown, so the worker repeats it.
    journal
        .ack(&claimed[0].id)
        .await
        .expect("a repeated ack must not fail");
    let stats = journal.stats(&billing).await.expect("stats");
    assert!(stats.is_drained());
    assert_eq!(stats.quarantined, 0);
}

#[tokio::test]
async fn a_delivery_that_was_never_claimed_cannot_be_acknowledged() {
    let journal = InMemoryUsageJournal::new();
    let event = event();
    journal.append(&event).await.expect("append");
    let delivery = DeliveryId {
        consumer: consumer("billing"),
        event: event.id(),
        attempt: 1,
    };
    let error = journal.ack(&delivery).await.expect_err("not outstanding");
    assert!(
        matches!(error, JournalError::NotOutstanding { .. }),
        "{error:?}"
    );

    let unknown = DeliveryId {
        consumer: consumer("billing"),
        event: next_request_id(),
        attempt: 1,
    };
    assert!(journal.ack(&unknown).await.is_err());
}

#[tokio::test]
async fn one_callers_events_are_delivered_in_append_order() {
    let journal = InMemoryUsageJournal::new();
    let billing = consumer("billing");
    let first = event_for("acme");
    let second = event_for("acme");
    let other = event_for("globex");
    for event in [&first, &second, &other] {
        journal.append(event).await.expect("append");
    }

    let claimed = journal.claim(&billing, claim(10)).await.expect("claim");
    assert_eq!(
        claimed.iter().map(|d| d.event.id()).collect::<Vec<_>>(),
        vec![first.id(), other.id()],
        "one event per ordering key is in flight; a second caller is not held up by the first"
    );

    // The next event for `acme` becomes claimable only once its predecessor is
    // settled, which is what "per-key ordering" means under a replaying consumer.
    journal.ack(&claimed[0].id).await.expect("ack");
    let next = journal.claim(&billing, claim(10)).await.expect("claim");
    assert_eq!(
        next.iter().map(|d| d.event.id()).collect::<Vec<_>>(),
        vec![second.id()]
    );
}

#[tokio::test]
async fn a_quarantined_event_stops_blocking_its_ordering_key() {
    let journal = InMemoryUsageJournal::new();
    let billing = consumer("billing");
    let poison = event_for("acme");
    let next = event_for("acme");
    journal.append(&poison).await.expect("append");
    journal.append(&next).await.expect("append");

    let claimed = journal.claim(&billing, claim(10)).await.expect("claim");
    assert_eq!(claimed[0].event.id(), poison.id());
    journal
        .quarantine(&claimed[0].id, PoisonReason::Malformed)
        .await
        .expect("quarantine");

    let after = journal.claim(&billing, claim(10)).await.expect("claim");
    assert_eq!(
        after.iter().map(|d| d.event.id()).collect::<Vec<_>>(),
        vec![next.id()],
        "the caller's later events must not be stuck behind a poison event"
    );
    let stats = journal.stats(&billing).await.expect("stats");
    assert_eq!(stats.quarantined, 1);
    assert_eq!(PoisonReason::Malformed.as_str(), "malformed");
}

#[tokio::test]
async fn an_event_that_exhausts_its_attempts_is_quarantined_not_retried_forever() {
    let capacity = Capacity {
        max_delivery_attempts: 2,
        ..Capacity::BILLING_GRADE
    };
    let journal = InMemoryUsageJournal::with_capacity(capacity);
    let billing = consumer("billing");
    let event = event();
    journal.append(&event).await.expect("append");

    let mut now = SystemTime::now();
    for attempt in 1..=capacity.max_delivery_attempts {
        let claimed = journal
            .claim(&billing, claim_of(10, now))
            .await
            .expect("claim");
        assert_eq!(claimed[0].id.attempt, attempt);
        // The worker crashes every time, so every lease expires unacknowledged.
        now += Duration::from_secs(31);
    }

    let after = journal
        .claim(&billing, claim_of(10, now))
        .await
        .expect("claim");
    assert!(
        after.is_empty(),
        "an event past its attempt budget must leave the delivery path"
    );
    let stats = journal.stats(&billing).await.expect("stats");
    assert_eq!(stats.quarantined, 1);
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.capacity.max_delivery_attempts, 2);
    assert_eq!(
        PoisonReason::AttemptsExhausted.as_str(),
        "attempts_exhausted"
    );
}

#[tokio::test]
async fn a_full_journal_refuses_the_append_and_says_what_it_is_bounded_by() {
    let capacity = Capacity {
        max_events: 2,
        ..Capacity::BILLING_GRADE
    };
    let journal = InMemoryUsageJournal::with_capacity(capacity);
    journal.append(&event()).await.expect("append");
    journal.append(&event()).await.expect("append");

    let error = journal
        .append(&event())
        .await
        .expect_err("a full journal must refuse");
    assert!(
        matches!(
            &error,
            JournalError::AtCapacity { pending, capacity } if *pending == 2 && capacity.max_events == 2
        ),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("was not journaled"),
        "the caller has to be able to tell the event is not durable: {error}"
    );
    assert_eq!(journal.stored_events(), 2);
    let stats = journal.stats(&consumer("billing")).await.expect("stats");
    assert_eq!(stats.dropped, 0, "refusing is not losing");
    assert!(stats.oldest_pending_age.is_some());
}

#[tokio::test]
async fn drop_oldest_bounds_storage_and_counts_what_it_lost() {
    let capacity = Capacity {
        max_events: 2,
        policy: CapacityPolicy::DropOldest,
        ..Capacity::BILLING_GRADE
    };
    let journal = InMemoryUsageJournal::with_capacity(capacity);
    let oldest = event_for("acme-one");
    journal.append(&oldest).await.expect("append");
    journal
        .append(&event_for("acme-two"))
        .await
        .expect("append");
    journal
        .append(&event_for("acme-three"))
        .await
        .expect("append");

    assert_eq!(journal.stored_events(), 2, "storage stays bounded");
    let billing = consumer("billing");
    let claimed = journal.claim(&billing, claim(10)).await.expect("claim");
    assert!(
        !claimed.iter().any(|d| d.event.id() == oldest.id()),
        "the dropped event is gone, not merely deprioritised"
    );
    let stats = journal.stats(&billing).await.expect("stats");
    assert_eq!(stats.dropped, 1, "a lossy policy has to report its cost");
}

#[tokio::test]
async fn consumers_acknowledge_independently() {
    let journal = InMemoryUsageJournal::new();
    let (billing, warehouse) = (consumer("billing"), consumer("warehouse"));
    let event = event();
    journal.append(&event).await.expect("append");

    let for_billing = journal.claim(&billing, claim(10)).await.expect("claim");
    let for_warehouse = journal.claim(&warehouse, claim(10)).await.expect("claim");
    assert_eq!(for_billing[0].event.id(), event.id());
    assert_eq!(for_warehouse[0].event.id(), event.id());
    assert_ne!(for_billing[0].id, for_warehouse[0].id);

    journal.ack(&for_billing[0].id).await.expect("ack");
    assert!(journal.stats(&billing).await.expect("stats").is_drained());
    assert_eq!(
        journal.stats(&warehouse).await.expect("stats").in_flight,
        1,
        "one consumer's acknowledgement is not another's"
    );
}

#[tokio::test]
async fn a_claim_is_bounded_by_what_the_caller_asked_for() {
    let journal = InMemoryUsageJournal::new();
    for index in 0..5 {
        journal
            .append(&event_for(&format!("acme-{index}")))
            .await
            .expect("append");
    }
    let claimed = journal
        .claim(&consumer("billing"), claim(2))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);
    assert!(
        claimed
            .iter()
            .all(|delivery| delivery.lease_expires_at > SystemTime::now())
    );
}

#[test]
fn consumer_names_are_narrow_enough_to_be_storage_keys() {
    assert_eq!(consumer("billing-postgres").as_str(), "billing-postgres");
    for bad in [
        "",
        "Billing",
        "billing table",
        "billing;drop",
        &"x".repeat(64),
    ] {
        assert!(ConsumerId::parse(bad).is_err(), "accepted `{bad}`");
    }
}

#[test]
fn the_event_carries_what_a_sink_needs_unchanged() {
    let event = event();
    let observed = event.observed();
    assert_eq!(observed.record.request_id, event.record().request_id);
    assert_eq!(
        observed.observed_at,
        event.observed_at(),
        "a replay written later still says when the request happened"
    );
}
