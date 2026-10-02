use ar3_server_prototype::{
    Error,
    api::{App, Service, router, validate_bind},
    model::*,
    provider::*,
    store::Store,
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::path::PathBuf;
use tower::ServiceExt;

const SIMPLE: &[u8] = include_bytes!("fixtures/wire/simple.json");
fn principal() -> Principal {
    Principal::synthetic("builder")
}
fn consumers() -> Vec<Principal> {
    vec![principal(), Principal::synthetic("analyst")]
}
fn setup() -> (tempfile::TempDir, Store, LocalStorage) {
    let t = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(t.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let s = Store::init(t.path().join("state.sqlite"), "00000000000000000000000000").unwrap();
    let storage = LocalStorage::new(t.path().join("objects")).unwrap();
    (t, s, storage)
}
fn ingest(s: &mut Store, storage: &mut LocalStorage) {
    assert!(
        s.ingest(
            "synthetic",
            &Envelope::parse(SIMPLE).unwrap().key().unwrap(),
            SIMPLE,
            &consumers(),
            storage
        )
        .unwrap()
    );
}
fn claim(s: &mut Store, key: &str, time: i64) -> Value {
    s.claim(
        &principal(),
        "builder",
        key,
        Claim {
            consumer_id: "builder-device".into(),
            max_messages: 1,
            wait_seconds: 0,
        },
        time,
    )
    .unwrap()["claims"][0]
        .clone()
}
fn ack(c: &Value, outcome: &str) -> Disposition {
    Disposition {
        lease_generation: c["lease_generation"].as_i64().unwrap(),
        lease_proof: c["lease_proof"].as_str().unwrap().into(),
        outcome: outcome.into(),
        handling_reference: "synthetic-result".into(),
    }
}
fn send() -> Send {
    Send {
        recipient: "builder".into(),
        content: "$ ` λ\n\\".into(),
        attachment_ids: vec![],
    }
}

#[test]
fn python_fixtures_and_s3_routing() {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/wire/manifest.json")).unwrap();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wire");
    for case in manifest["cases"].as_array().unwrap() {
        let raw = std::fs::read(root.join(case["file"].as_str().unwrap())).unwrap();
        let e = Envelope::parse(&raw).unwrap();
        if let Some(expected) = case["expected_key"].as_str() {
            assert_eq!(e.key().unwrap(), expected);
        }
        assert_eq!(e, Envelope::parse(&e.bytes().unwrap()).unwrap());
    }
    let mut e = Envelope::parse(include_bytes!("fixtures/wire/receipt_inbox_write.json")).unwrap();
    for stage in [
        "inbox_write",
        "attachment_fetched",
        "attachment_failed",
        "deferred",
        "expired",
        "no_local_recipient",
    ] {
        e.extensions.get_mut("a8s_control").unwrap()["stage"] = json!(stage);
        assert!(e.validate().is_ok());
        assert_eq!(e.mailbox().unwrap(), "analyst");
    }
    e.extensions.get_mut("a8s_control").unwrap()["version"] = json!(3);
    assert_eq!(e.validate().unwrap_err(), Error("invalid_input"));
}
#[test]
fn ulid_policy_filenames_and_prefix_overlap() {
    assert!(valid_id("01M3Y7RKG00000000000000001"));
    for bad in [
        "81M3Y7RKG00000000000000001",
        "01m3y7rkg00000000000000001",
        "01M3Y7RKG0000000000000000I",
        "x",
    ] {
        assert!(!valid_id(bad));
    }
    for bad in ["../x", "a/b", "a\\b", "%2e%2e", ".", "/etc/passwd"] {
        assert!(filename(bad).is_err());
    }
    assert!(disjoint("mail/a", "mail/ab"));
    assert!(!disjoint("mail", "mail/a"));
    assert!(!disjoint("mail/a", "mail"));
    assert!(validate_bind("127.0.0.1:9000".parse().unwrap()).is_ok());
    assert!(validate_bind("0.0.0.0:9000".parse().unwrap()).is_err());
}
#[test]
fn custody_receipt_is_durable_but_read_and_claim_are_not_handled() {
    let (t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let pending = s.outbox("synthetic").unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["kind"], "receipt");
    let first = s.inbox(&principal(), "builder").unwrap();
    assert_eq!(first[0]["state"], "ready");
    let c = claim(&mut s, "claim-1", 100);
    assert!(c.is_object());
    drop(s);
    let mut s = Store::open(t.path().join("state.sqlite")).unwrap();
    assert_eq!(s.outbox("synthetic").unwrap(), pending);
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "leased"
    );
    let next = claim(&mut s, "claim-2", 163);
    assert_eq!(next["lease_generation"], 2);
    assert_eq!(
        s.disposition(
            &principal(),
            next["delivery_id"].as_str().unwrap(),
            "ack",
            ack(&c, "handled"),
            164
        )
        .unwrap_err(),
        Error("stale_lease")
    );
    s.disposition(
        &principal(),
        next["delivery_id"].as_str().unwrap(),
        "ack",
        ack(&next, "handled"),
        164,
    )
    .unwrap();
    drop(s);
    let mut s = Store::open(t.path().join("state.sqlite")).unwrap();
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "handled"
    );
    assert_eq!(
        s.claim(
            &principal(),
            "builder",
            "claim-3",
            Claim {
                consumer_id: "builder-device".into(),
                max_messages: 1,
                wait_seconds: 0
            },
            1000
        )
        .unwrap()["claims"],
        json!([])
    );
    assert_eq!(
        s.disposition(
            &principal(),
            next["delivery_id"].as_str().unwrap(),
            "ack",
            ack(&next, "handled"),
            1000
        )
        .unwrap()["outcome"],
        "handled"
    );
}
#[test]
fn two_claimers_cannot_share_a_lease_and_wrong_owner_cannot_ack() {
    let (_t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let first = claim(&mut s, "one", 100);
    assert!(claim(&mut s, "two", 100).is_null());
    assert!(
        s.disposition(
            &Principal::synthetic("analyst"),
            first["delivery_id"].as_str().unwrap(),
            "ack",
            ack(&first, "handled"),
            101
        )
        .is_err()
    );
    assert!(
        s.claim(
            &principal(),
            "analyst",
            "foreign",
            Claim {
                consumer_id: "analyst-device".into(),
                max_messages: 1,
                wait_seconds: 0
            },
            100
        )
        .is_err()
    );
}
#[test]
fn failed_commit_does_not_consume_or_generate_receipt() {
    let (_t, mut s, mut storage) = setup();
    s.fail_next_commit();
    assert!(
        s.ingest(
            "synthetic",
            &Envelope::parse(SIMPLE).unwrap().key().unwrap(),
            SIMPLE,
            &consumers(),
            &mut storage
        )
        .is_err()
    );
    assert!(s.inbox(&principal(), "builder").unwrap().is_empty());
    assert!(s.outbox("synthetic").unwrap().is_empty());
    ingest(&mut s, &mut storage);
    assert_eq!(s.inbox(&principal(), "builder").unwrap().len(), 1);
}
#[test]
fn duplicate_conflict_is_quarantined_without_replacement() {
    let (_t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    ingest(&mut s, &mut storage);
    let e = Envelope::parse(SIMPLE).unwrap();
    assert!(
        !s.ingest(
            "synthetic",
            &e.key().unwrap(),
            include_bytes!("fixtures/wire/conflicting_duplicate.json"),
            &consumers(),
            &mut storage
        )
        .unwrap()
    );
    assert!(
        !s.ingest(
            "synthetic",
            "a8s-test-mail/builder/wrong.json",
            SIMPLE,
            &consumers(),
            &mut storage
        )
        .unwrap()
    );
    assert_eq!(s.inbox(&principal(), "builder").unwrap().len(), 1);
    assert_eq!(s.outbox("synthetic").unwrap().len(), 1);
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["envelope"]["content"],
        e.content
    );
}
#[test]
fn acceptance_idempotency_survives_restart_and_auth_is_scoped() {
    let (t, mut s, _) = setup();
    let p = Principal::synthetic("analyst");
    s.fail_next_commit();
    assert!(s.accept(&p, "fault", send()).is_err());
    let accepted = s.accept(&p, "send-one", send()).unwrap();
    drop(s);
    let mut s = Store::open(t.path().join("state.sqlite")).unwrap();
    assert_eq!(s.accept(&p, "send-one", send()).unwrap(), accepted);
    let mut changed = send();
    changed.content = "changed".into();
    assert_eq!(
        s.accept(&p, "send-one", changed).unwrap_err(),
        Error("idempotency_conflict")
    );
    assert!(
        s.operation_status(&principal(), accepted["operation_id"].as_str().unwrap())
            .is_err()
    );
    let mut other = p.clone();
    other.tenant = "other".into();
    assert_ne!(
        s.accept(&other, "send-one", send()).unwrap()["message_id"],
        accepted["message_id"]
    );
}
struct Ambiguous {
    bytes: Vec<Vec<u8>>,
    fail: bool,
}
impl Remote for Ambiguous {
    fn publish(&mut self, _: &str, b: &[u8]) -> ar3_server_prototype::Result<()> {
        self.bytes.push(b.to_vec());
        if self.fail {
            self.fail = false;
            Err(Error("provider_unavailable"))
        } else {
            Ok(())
        }
    }
    fn poll(&mut self, _: &str, _: Option<&str>, _: usize) -> ar3_server_prototype::Result<Page> {
        unreachable!()
    }
}
#[test]
fn unknown_publish_and_restart_retry_exact_original_id_and_bytes() {
    let (t, mut s, mut storage) = setup();
    let p = Principal::synthetic("analyst");
    let accepted = s.accept(&p, "one", send()).unwrap();
    let mid = accepted["message_id"].as_str().unwrap();
    let mut remote = Ambiguous {
        bytes: vec![],
        fail: true,
    };
    assert!(
        s.publish("synthetic", mid, &mut remote, &mut storage)
            .is_err()
    );
    assert_eq!(
        s.operation_status(&p, accepted["operation_id"].as_str().unwrap())
            .unwrap()["state"],
        "blocked"
    );
    drop(s);
    let mut s = Store::open(t.path().join("state.sqlite")).unwrap();
    assert_eq!(remote.bytes.len(), 1);
    s.publish("synthetic", mid, &mut remote, &mut storage)
        .unwrap();
    assert_eq!(remote.bytes[0], remote.bytes[1]);
    assert_eq!(Envelope::parse(&remote.bytes[0]).unwrap().id, mid);
    s.publish("synthetic", mid, &mut remote, &mut storage)
        .unwrap();
    assert_eq!(remote.bytes.len(), 2);
}
#[test]
fn attachments_roundtrip_with_no_listing_and_no_ambient_fallback() {
    let (t, mut s, mut storage) = setup();
    let p = Principal::synthetic("analyst");
    let data = include_bytes!("fixtures/wire/result.txt");
    let upload = s.upload(&p, "file", "result.txt", data).unwrap();
    let aid = upload["attachment_id"].as_str().unwrap();
    assert!(s.attachment(&principal(), aid).is_err());
    let accepted = s
        .accept(
            &p,
            "mail",
            Send {
                recipient: "builder".into(),
                content: "file".into(),
                attachment_ids: vec![aid.into()],
            },
        )
        .unwrap();
    let mid = accepted["message_id"].as_str().unwrap();
    let mut remote = S3Remote(LocalObjects::new(t.path().join("objects")).unwrap());
    s.publish("synthetic", mid, &mut remote, &mut storage)
        .unwrap();
    let page = remote.poll("builder", None, 16).unwrap();
    assert_eq!(page.objects.len(), 1);
    let (key, bytes) = &page.objects[0];
    let e = Envelope::parse(bytes).unwrap();
    let capability = &e.files[0].storage[0];
    assert_eq!(storage.retrieve_capability(capability).unwrap(), data);
    assert!(
        storage
            .retrieve_capability(&capability.replace("cap=", "cap=forged"))
            .is_err()
    );
    assert!(
        storage
            .retrieve_capability(&capability.replace("objects.example.invalid", "127.0.0.1"))
            .is_err()
    );
    let mut objects = LocalObjects::new(t.path().join("objects")).unwrap();
    assert_eq!(
        objects
            .list_mail("a8s-test-files/", "/", None, 16)
            .unwrap_err(),
        Error("forbidden")
    );
    assert!(
        s.ingest("synthetic", key, bytes, &consumers(), &mut storage)
            .unwrap()
    );
    assert_eq!(
        s.inbound_attachment(&principal(), mid, "result.txt")
            .unwrap(),
        data
    );
    assert!(s.inbound_attachment(&p, mid, "result.txt").is_err());
    assert!(std::path::Path::new(&t.path().join("objects").join(key)).exists());
}
#[test]
fn unavailable_attachment_stays_deferred_and_cannot_be_claimed() {
    let (_t, mut s, mut storage) = setup();
    let bytes = include_bytes!("fixtures/wire/attachment.json");
    let e = Envelope::parse(bytes).unwrap();
    assert!(
        s.ingest(
            "synthetic",
            &e.key().unwrap(),
            bytes,
            &consumers(),
            &mut storage
        )
        .unwrap()
    );
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "deferred"
    );
    assert!(claim(&mut s, "one", 100).is_null());
}
#[test]
fn control_evidence_is_not_a_task_and_does_not_erase_other_receipts() {
    let (_t, mut s, mut storage) = setup();
    for raw in [
        include_bytes!("fixtures/wire/receipt_inbox_write.json").as_slice(),
        include_bytes!("fixtures/wire/receipt_no_local_recipient.json").as_slice(),
    ] {
        let e = Envelope::parse(raw).unwrap();
        assert!(
            s.ingest(
                "synthetic",
                &e.key().unwrap(),
                raw,
                &consumers(),
                &mut storage
            )
            .unwrap()
        );
    }
    let p = Principal::synthetic("analyst");
    assert_eq!(s.inbox(&p, "analyst").unwrap().len(), 2);
    assert!(s.outbox("synthetic").unwrap().is_empty());
    assert_eq!(
        s.claim(
            &p,
            "analyst",
            "one",
            Claim {
                consumer_id: p.consumer.clone(),
                max_messages: 8,
                wait_seconds: 0
            },
            100
        )
        .unwrap()["claims"],
        json!([])
    );
}
#[test]
fn separate_registered_consumers_receive_independent_copies() {
    let (_t, mut s, mut storage) = setup();
    let mut other = principal();
    other.consumer = "second-device".into();
    let e = Envelope::parse(SIMPLE).unwrap();
    s.ingest(
        "synthetic",
        &e.key().unwrap(),
        SIMPLE,
        &[principal(), other.clone()],
        &mut storage,
    )
    .unwrap();
    assert_eq!(s.inbox(&principal(), "builder").unwrap().len(), 1);
    assert_eq!(s.inbox(&other, "builder").unwrap().len(), 1);
}
#[test]
fn restored_valid_snapshot_is_held_and_every_restore_has_a_fresh_epoch() {
    let (t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let c = claim(&mut s, "claim", 100);
    let snapshot = t.path().join("backup.sqlite");
    s.backup(&snapshot).unwrap();
    s.disposition(
        &principal(),
        c["delivery_id"].as_str().unwrap(),
        "ack",
        ack(&c, "handled"),
        101,
    )
    .unwrap();
    let mut first = Store::restore(&snapshot, &t.path().join("restore1/state.sqlite")).unwrap();
    let second = Store::restore(&snapshot, &t.path().join("restore2/state.sqlite")).unwrap();
    assert_eq!(first.status().unwrap()["recovery_hold"], true);
    assert_ne!(
        first.status().unwrap()["epoch"],
        second.status().unwrap()["epoch"]
    );
    assert!(
        first
            .claim(
                &principal(),
                "builder",
                "x",
                Claim {
                    consumer_id: "builder-device".into(),
                    max_messages: 1,
                    wait_seconds: 0
                },
                101
            )
            .is_err()
    );
    first.cutover().unwrap();
    assert_eq!(
        first.inbox(&principal(), "builder").unwrap()[0]["state"],
        "uncertain"
    );
    assert!(
        first
            .disposition(
                &principal(),
                c["delivery_id"].as_str().unwrap(),
                "old",
                ack(&c, "handled"),
                101
            )
            .is_err()
    );
    first
        .reviewed_requeue("synthetic", Envelope::parse(SIMPLE).unwrap().id.as_str())
        .unwrap();
    assert_eq!(
        first.inbox(&principal(), "builder").unwrap()[0]["state"],
        "ready"
    );
}
#[test]
fn missing_marker_and_old_database_rollback_fail_closed() {
    let (t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let snapshot = t.path().join("backup.sqlite");
    s.backup(&snapshot).unwrap();
    let c = claim(&mut s, "claim", 100);
    s.disposition(
        &principal(),
        c["delivery_id"].as_str().unwrap(),
        "ack",
        ack(&c, "handled"),
        101,
    )
    .unwrap();
    drop(s);
    let db = t.path().join("state.sqlite");
    std::fs::copy(snapshot, &db).unwrap();
    let s = Store::open(&db).unwrap();
    assert_eq!(s.status().unwrap()["recovery_hold"], true);
    drop(s);
    std::fs::remove_file(db.with_extension("epoch")).unwrap();
    assert_eq!(
        Store::open(&db).unwrap().status().unwrap()["recovery_hold"],
        true
    );
    assert!(Store::open(t.path().join("missing.sqlite")).is_err());
    assert!(!t.path().join("missing.sqlite").exists());
}
#[test]
fn s3_pagination_advances_past_malformed_and_revisits_late_objects() {
    let (t, mut s, mut storage) = setup();
    let mut objects = LocalObjects::new(t.path().join("objects")).unwrap();
    for i in 0..20 {
        objects
            .put_exact(
                &format!("{MAIL_PREFIX}/builder/000-{i:02}.json"),
                b"invalid",
            )
            .unwrap();
    }
    let e = Envelope::parse(SIMPLE).unwrap();
    objects.put_exact(&e.key().unwrap(), SIMPLE).unwrap();
    let mut remote = S3Remote(objects);
    assert_eq!(
        s.poll(&principal(), &mut remote, &mut storage, &consumers())
            .unwrap()["more"],
        true
    );
    assert_eq!(
        s.poll(&principal(), &mut remote, &mut storage, &consumers())
            .unwrap()["custody"],
        1
    );
    let mut late = e.clone();
    late.id = "00000000000000000000000001".into();
    remote
        .0
        .put_exact(&late.key().unwrap(), &late.bytes().unwrap())
        .unwrap();
    s.poll(&principal(), &mut remote, &mut storage, &consumers())
        .unwrap();
    s.poll(&principal(), &mut remote, &mut storage, &consumers())
        .unwrap();
    assert_eq!(s.inbox(&principal(), "builder").unwrap().len(), 2);
}
#[derive(Default)]
struct Session {
    generation: u64,
    acks: Vec<u16>,
    published: Vec<Vec<u8>>,
}
impl MqttSession for Session {
    fn publish_qos1(&mut self, _: &str, b: &[u8]) -> ar3_server_prototype::Result<()> {
        self.published.push(b.to_vec());
        Ok(())
    }
    fn ack_current(&mut self, p: u16) -> ar3_server_prototype::Result<()> {
        self.acks.push(p);
        Ok(())
    }
    fn generation(&self) -> u64 {
        self.generation
    }
}
#[test]
fn mqtt_contract_acks_only_committed_custody_on_current_connection() {
    let (_t, mut s, mut storage) = setup();
    let mut m = MqttAdapter {
        session: Session {
            generation: 2,
            ..Default::default()
        },
        topic: "synthetic-topic".into(),
    };
    s.fail_next_commit();
    let e = Envelope::parse(SIMPLE).unwrap();
    assert!(
        m.received(1, 2, SIMPLE, |raw| s.ingest(
            "synthetic",
            &e.key()?,
            raw,
            &consumers(),
            &mut storage
        ))
        .is_err()
    );
    assert!(m.session.acks.is_empty());
    assert!(
        !m.received(1, 1, SIMPLE, |raw| s.ingest(
            "synthetic",
            &e.key()?,
            raw,
            &consumers(),
            &mut storage
        ))
        .unwrap()
    );
    assert!(m.session.acks.is_empty());
    assert!(
        m.received(1, 2, SIMPLE, |raw| s.ingest(
            "synthetic",
            &e.key()?,
            raw,
            &consumers(),
            &mut storage
        ))
        .unwrap()
    );
    assert_eq!(m.session.acks, vec![1]);
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "ready"
    );
}

async fn http(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", t);
    }
    if let Some(k) = key {
        req = req.header("idempotency-key", k);
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    let status = response.status();
    let raw = to_bytes(response.into_body(), MAX_JSON * 16).await.unwrap();
    (status, serde_json::from_slice(&raw).unwrap())
}
#[tokio::test]
async fn http_auth_spoofing_and_synthetic_end_to_end() {
    let (t, s, storage) = setup();
    let app = router(App::new(Service {
        store: s,
        remote: S3Remote(LocalObjects::new(t.path().join("objects")).unwrap()),
        storage,
    }));
    for path in [
        "/v1/capabilities",
        "/v1/inboxes/builder/messages",
        "/health/ready",
        "/not-an-endpoint",
    ] {
        assert_eq!(
            http(&app, "GET", path, None, None, json!({})).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        http(
            &app,
            "POST",
            "/v1/messages",
            Some("Bearer analyst-test"),
            Some("spoof"),
            json!({"recipient":"builder","content":"test","from":"administrator"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        http(
            &app,
            "GET",
            "/v1/inboxes/analyst/messages",
            Some("Bearer builder-test"),
            None,
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, accepted) = http(
        &app,
        "POST",
        "/v1/messages",
        Some("Bearer analyst-test"),
        Some("one"),
        json!({"recipient":"builder","content":"λ $ `\n","attachment_ids":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let op = accepted["operation_id"].as_str().unwrap();
    assert_eq!(
        http(
            &app,
            "POST",
            &format!("/v1/operations/{op}/publish"),
            Some("Bearer analyst-test"),
            Some("publish-one"),
            json!({})
        )
        .await
        .1["state"],
        "published"
    );
    assert_eq!(
        http(
            &app,
            "POST",
            "/v1/inboxes/builder/poll",
            Some("Bearer builder-test"),
            Some("poll"),
            json!({})
        )
        .await
        .1["custody"],
        1
    );
    let claims = http(
        &app,
        "POST",
        "/v1/inboxes/builder/claims",
        Some("Bearer builder-test"),
        Some("claim"),
        json!({"consumer_id":"builder-device","max_messages":1,"wait_seconds":0}),
    )
    .await
    .1;
    let c = &claims["claims"][0];
    assert_eq!(c["envelope"]["from"], "analyst");
    let result = http(
        &app,
        "POST",
        &format!(
            "/v1/deliveries/{}/disposition",
            c["delivery_id"].as_str().unwrap()
        ),
        Some("Bearer builder-test"),
        Some("ack"),
        serde_json::to_value(ack(c, "handled")).unwrap(),
    )
    .await;
    assert_eq!(result.0, StatusCode::OK);
    assert_eq!(result.1["outcome"], "handled");
}
#[test]
fn emits_python_conformance_artifacts_when_requested() {
    if let Ok(dir) = std::env::var("AR3_CONFORMANCE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir).unwrap();
        let (_t, mut s, mut storage) = setup();
        let p = Principal::synthetic("analyst");
        let data = include_bytes!("fixtures/wire/result.txt");
        let upload = s
            .upload(&p, "conformance-file", "result.txt", data)
            .unwrap();
        let accepted = s
            .accept(
                &p,
                "conformance-message",
                Send {
                    recipient: "builder".into(),
                    content: "Rust outbound λ $ `\n".into(),
                    attachment_ids: vec![upload["attachment_id"].as_str().unwrap().into()],
                },
            )
            .unwrap();
        let mut remote = Ambiguous {
            bytes: vec![],
            fail: false,
        };
        s.publish(
            "synthetic",
            accepted["message_id"].as_str().unwrap(),
            &mut remote,
            &mut storage,
        )
        .unwrap();
        std::fs::write(dir.join("attachment.json"), &remote.bytes[0]).unwrap();
        std::fs::write(dir.join("result.txt"), data).unwrap();
        let e = Envelope::parse(SIMPLE).unwrap();
        std::fs::write(dir.join("normal.json"), e.bytes().unwrap()).unwrap();
        for stage in [
            "inbox_write",
            "attachment_fetched",
            "attachment_failed",
            "deferred",
            "expired",
            "no_local_recipient",
        ] {
            std::fs::write(
                dir.join(format!("{stage}.json")),
                e.receipt(stage).bytes().unwrap(),
            )
            .unwrap();
        }
    }
}

#[tokio::test]
async fn concurrent_http_claims_and_reader_denials() {
    let (t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let app = router(App::new(Service {
        store: s,
        remote: S3Remote(LocalObjects::new(t.path().join("objects")).unwrap()),
        storage,
    }));
    let reader = Some("Bearer builder-reader-test");
    assert_eq!(
        http(
            &app,
            "GET",
            "/v1/inboxes/builder/messages",
            reader,
            None,
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        http(
            &app,
            "POST",
            "/v1/messages",
            reader,
            Some("send"),
            json!({"recipient":"analyst","content":"denied"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let request = json!({"consumer_id":"builder-device","max_messages":1,"wait_seconds":0});
    assert_eq!(
        http(
            &app,
            "POST",
            "/v1/inboxes/builder/claims",
            reader,
            Some("read-claim"),
            request.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (a, b) = tokio::join!(
        http(
            &app,
            "POST",
            "/v1/inboxes/builder/claims",
            Some("Bearer builder-test"),
            Some("race-one"),
            request.clone()
        ),
        http(
            &app,
            "POST",
            "/v1/inboxes/builder/claims",
            Some("Bearer builder-test"),
            Some("race-two"),
            request
        )
    );
    assert_eq!(
        a.1["claims"].as_array().unwrap().len() + b.1["claims"].as_array().unwrap().len(),
        1
    );
}
#[test]
fn marker_write_failure_pauses_current_process_after_durable_commit() {
    let (t, mut s, _storage) = setup();
    let marker = t.path().join("state.epoch");
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    assert_eq!(
        s.accept(&Principal::synthetic("analyst"), "one", send())
            .unwrap_err(),
        Error("durable_storage_unavailable")
    );
    assert_eq!(s.status().unwrap()["recovery_hold"], true);
    assert_eq!(
        s.accept(&Principal::synthetic("analyst"), "two", send())
            .unwrap_err(),
        Error("recovery_hold")
    );
    assert_eq!(s.outbox("synthetic").unwrap().len(), 1);
}
#[test]
fn current_fifth_lease_is_valid_and_exhaustion_retains_a_dead_letter() {
    let (_t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let mut c = Value::Null;
    for attempt in 0..5 {
        c = claim(&mut s, &format!("claim-{attempt}"), 100 + attempt * 63);
    }
    assert!(claim(&mut s, "sixth-too-early", 353).is_null());
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "leased"
    );
    assert!(claim(&mut s, "sixth-expired", 415).is_null());
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "dead_letter"
    );
    assert!(
        s.disposition(
            &principal(),
            c["delivery_id"].as_str().unwrap(),
            "late",
            ack(&c, "handled"),
            415
        )
        .is_err()
    );
}
#[test]
fn unicode_and_space_attachment_names_roundtrip_and_encoded_escape_fails() {
    let (t, _s, mut storage) = setup();
    let capability = storage
        .publish_upload(&id(), "result λ.txt", b"data")
        .unwrap();
    assert_eq!(storage.retrieve_capability(&capability).unwrap(), b"data");
    for bad in [
        capability.replace("result%20%CE%BB.txt", "%2Fetc%2Fpasswd"),
        capability.replace("result%20%CE%BB.txt", "%252e%252e"),
        "s3://synthetic/a8s-test-files/object/file".into(),
        "https://user@objects.example.invalid/a8s-test-files/object/file".into(),
    ] {
        assert!(storage.retrieve_capability(&bad).is_err());
    }
    let mut objects = LocalObjects::new(t.path().join("objects")).unwrap();
    assert!(objects.get_exact("../outside", 1024).is_err());
}
#[test]
fn durable_receipt_retries_original_id_after_ambiguous_publish() {
    let (t, mut s, mut storage) = setup();
    ingest(&mut s, &mut storage);
    let mid = s.outbox("synthetic").unwrap()[0]["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut remote = Ambiguous {
        bytes: vec![],
        fail: true,
    };
    assert!(
        s.publish("synthetic", &mid, &mut remote, &mut storage)
            .is_err()
    );
    drop(s);
    let mut s = Store::open(t.path().join("state.sqlite")).unwrap();
    s.publish("synthetic", &mid, &mut remote, &mut storage)
        .unwrap();
    assert_eq!(remote.bytes[0], remote.bytes[1]);
    assert_eq!(Envelope::parse(&remote.bytes[0]).unwrap().id, mid);
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["state"],
        "ready"
    );
}
#[test]
fn mqtt_does_not_ack_a_refused_callback() {
    let mut m = MqttAdapter {
        session: Session {
            generation: 1,
            ..Default::default()
        },
        topic: "test".into(),
    };
    assert!(!m.received(8, 1, SIMPLE, |_| Ok(false)).unwrap());
    assert!(m.session.acks.is_empty());
}
#[tokio::test]
async fn oversized_and_nested_input_fail_without_echoing_content() {
    let (t, s, storage) = setup();
    let app = router(App::new(Service {
        store: s,
        remote: S3Remote(LocalObjects::new(t.path().join("objects")).unwrap()),
        storage,
    }));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("authorization", "Bearer analyst-test")
        .header("idempotency-key", "oversized")
        .body(Body::from(vec![b'x'; MAX_JSON + 1]))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let raw = to_bytes(response.into_body(), MAX_JSON).await.unwrap();
    assert!(raw.len() < 512);
    let nested = format!("{}0{}", "[".repeat(150), "]".repeat(150));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("authorization", "Bearer analyst-test")
        .header("idempotency-key", "nested")
        .body(Body::from(nested))
        .unwrap();
    assert_eq!(
        app.oneshot(req).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn slow_response_holds_a_bounded_request_slot() {
    let (t, s, storage) = setup();
    let app = router(App::new(Service {
        store: s,
        remote: S3Remote(LocalObjects::new(t.path().join("objects")).unwrap()),
        storage,
    }));
    let mut responses = Vec::new();
    for _ in 0..32 {
        let r = Request::builder()
            .uri("/v1/capabilities")
            .header("authorization", "Bearer analyst-test")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(r).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        responses.push(response);
    }
    assert_eq!(
        http(
            &app,
            "GET",
            "/v1/capabilities",
            Some("Bearer analyst-test"),
            None,
            json!({})
        )
        .await
        .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    drop(responses);
    assert_eq!(
        http(
            &app,
            "GET",
            "/v1/capabilities",
            Some("Bearer analyst-test"),
            None,
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[test]
fn ambiguous_outbound_attachments_are_rejected_before_acceptance() {
    let (_t, mut s, _storage) = setup();
    let p = Principal::synthetic("analyst");
    let a = s.upload(&p, "same-a", "same.txt", b"one").unwrap()["attachment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let b = s.upload(&p, "same-b", "same.txt", b"two").unwrap()["attachment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for (key, ids) in [
        ("repeated-id", vec![a.clone(), a.clone()]),
        ("same-name", vec![a, b]),
    ] {
        assert_eq!(
            s.accept(
                &p,
                key,
                Send {
                    recipient: "builder".into(),
                    content: "ambiguous".into(),
                    attachment_ids: ids
                }
            )
            .unwrap_err(),
            Error("duplicate_attachment_filename")
        );
    }
    assert!(s.outbox("synthetic").unwrap().is_empty());
}

#[test]
fn recovery_keeps_control_evidence_and_missing_files_unclaimable() {
    let (t, mut s, mut storage) = setup();
    let receipt = include_bytes!("fixtures/wire/receipt_inbox_write.json");
    let deferred = include_bytes!("fixtures/wire/attachment.json");
    for raw in [receipt.as_slice(), deferred.as_slice()] {
        let e = Envelope::parse(raw).unwrap();
        assert!(
            s.ingest(
                "synthetic",
                &e.key().unwrap(),
                raw,
                &consumers(),
                &mut storage
            )
            .unwrap()
        );
    }
    s.backup(&t.path().join("snapshot.sqlite")).unwrap();
    let mut recovered = Store::restore(
        &t.path().join("snapshot.sqlite"),
        &t.path().join("recovered/state.sqlite"),
    )
    .unwrap();
    recovered.cutover().unwrap();
    for raw in [receipt.as_slice(), deferred.as_slice()] {
        recovered
            .reviewed_requeue("synthetic", &Envelope::parse(raw).unwrap().id)
            .unwrap();
    }
    assert_eq!(
        recovered.inbox(&principal(), "builder").unwrap()[0]["state"],
        "deferred"
    );
    let analyst = Principal::synthetic("analyst");
    assert_eq!(
        recovered.inbox(&analyst, "analyst").unwrap()[0]["state"],
        "evidence"
    );
    for p in [principal(), analyst] {
        assert_eq!(
            recovered
                .claim(
                    &p,
                    &p.seat,
                    "after-review",
                    Claim {
                        consumer_id: p.consumer.clone(),
                        max_messages: 8,
                        wait_seconds: 0
                    },
                    100
                )
                .unwrap()["claims"],
            json!([])
        );
    }
}

#[test]
fn canonical_expansion_is_quarantined_and_poll_reaches_following_message() {
    let (t, mut s, mut storage) = setup();
    let mut bad = Envelope::parse(SIMPLE).unwrap();
    bad.id = "01M3Y7RKG00000000000000001".into();
    let base = String::from_utf8(bad.bytes().unwrap()).unwrap();
    let raw = format!(
        "{},\"opaque\":[{}]}}",
        base.strip_suffix('}').unwrap(),
        vec!["1e2"; 60000].join(",")
    )
    .into_bytes();
    assert!(raw.len() < MAX_JSON);
    assert_eq!(
        Envelope::parse(&raw).unwrap().bytes().unwrap_err(),
        Error("body_too_large")
    );
    let mut valid = Envelope::parse(SIMPLE).unwrap();
    valid.id = "01M3Y7RKG00000000000000002".into();
    let mut remote = S3Remote(LocalObjects::new(t.path().join("objects")).unwrap());
    remote.0.put_exact(&bad.key().unwrap(), &raw).unwrap();
    remote
        .0
        .put_exact(&valid.key().unwrap(), &valid.bytes().unwrap())
        .unwrap();
    assert_eq!(
        s.poll(&principal(), &mut remote, &mut storage, &consumers())
            .unwrap()["custody"],
        1
    );
    assert_eq!(
        s.inbox(&principal(), "builder").unwrap()[0]["envelope"]["id"],
        valid.id
    );
    assert!(
        s.poll(&principal(), &mut remote, &mut storage, &consumers())
            .is_ok()
    );
}

#[test]
fn final_wire_size_failure_blocks_the_accepted_operation_durably() {
    let (t, mut s, mut storage) = setup();
    let p = Principal::synthetic("analyst");
    let upload = s
        .upload(&p, "upload", &"x".repeat(128), b"synthetic")
        .unwrap();
    let request = Send {
        recipient: "builder".into(),
        content: "\"".repeat(MAX_JSON / 2 - 100),
        attachment_ids: vec![upload["attachment_id"].as_str().unwrap().into()],
    };
    assert!(serde_json::to_vec(&request).unwrap().len() < MAX_JSON);
    let op = s.accept(&p, "near-bound", request).unwrap();
    let mut remote = Ambiguous {
        bytes: vec![],
        fail: false,
    };
    assert_eq!(
        s.publish_operation(
            &p,
            op["operation_id"].as_str().unwrap(),
            &mut remote,
            &mut storage
        )
        .unwrap_err(),
        Error("body_too_large")
    );
    assert!(remote.bytes.is_empty());
    drop(s);
    let mut reopened = Store::open(t.path().join("state.sqlite")).unwrap();
    let status = reopened
        .operation_status(&p, op["operation_id"].as_str().unwrap())
        .unwrap();
    assert_eq!(status["state"], "blocked");
    assert_eq!(status["reason"], "wire_body_too_large");
    assert_eq!(status["wire_finalized"], false);
    assert_eq!(
        reopened
            .publish_operation(
                &p,
                op["operation_id"].as_str().unwrap(),
                &mut remote,
                &mut storage
            )
            .unwrap_err(),
        Error("body_too_large")
    );
    assert!(remote.bytes.is_empty());
}

#[tokio::test]
async fn inbound_file_http_decodes_once_and_preserves_utf8_names() {
    let (t, mut s, mut storage) = setup();
    let name = "result λ.txt";
    let data = b"exact inbound bytes";
    let reference = storage.publish_upload(&id(), name, data).unwrap();
    let mut e = Envelope::parse(SIMPLE).unwrap();
    e.files = vec![FileRef {
        filename: name.into(),
        storage: vec![reference],
        extensions: Default::default(),
    }];
    assert!(
        s.ingest(
            "synthetic",
            &e.key().unwrap(),
            &e.bytes().unwrap(),
            &consumers(),
            &mut storage
        )
        .unwrap()
    );
    let app = router(App::new(Service {
        store: s,
        remote: S3Remote(LocalObjects::new(t.path().join("objects")).unwrap()),
        storage,
    }));
    for (encoded, status) in [
        ("result%20%CE%BB.txt", StatusCode::OK),
        ("%2Fetc", StatusCode::BAD_REQUEST),
        ("%5Cetc", StatusCode::BAD_REQUEST),
        ("%252e%252e", StatusCode::BAD_REQUEST),
        ("%2e%2e", StatusCode::BAD_REQUEST),
        ("%FF", StatusCode::BAD_REQUEST),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/messages/{}/files/{encoded}", e.id))
                    .header("authorization", "Bearer builder-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let bytes = to_bytes(response.into_body(), MAX_JSON).await.unwrap();
        if status == StatusCode::OK {
            assert_eq!(bytes.as_ref(), data);
        }
    }
}
#[test]
fn duplicate_remote_names_are_quarantined_without_wedging_the_page() {
    let (t, mut s, mut storage) = setup();
    let capability = storage
        .publish_upload(&id(), "same.txt", b"content")
        .unwrap();
    let mut bad = Envelope::parse(SIMPLE).unwrap();
    bad.files = vec![
        FileRef {
            filename: "same.txt".into(),
            storage: vec![capability],
            extensions: Default::default()
        };
        2
    ];
    let mut valid = Envelope::parse(SIMPLE).unwrap();
    valid.id = "01M3Y7RKG00000000000000002".into();
    let mut objects = LocalObjects::new(t.path().join("objects")).unwrap();
    objects
        .put_exact(&bad.key().unwrap(), &bad.bytes().unwrap())
        .unwrap();
    objects
        .put_exact(&valid.key().unwrap(), &valid.bytes().unwrap())
        .unwrap();
    let mut remote = S3Remote(objects);
    let result = s
        .poll(&principal(), &mut remote, &mut storage, &consumers())
        .unwrap();
    assert_eq!(result["custody"], 1);
    let inbox = s.inbox(&principal(), "builder").unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0]["envelope"]["id"], valid.id);
    assert_eq!(
        s.poll(&principal(), &mut remote, &mut storage, &consumers())
            .unwrap()["custody"],
        1
    );
}
