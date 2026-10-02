use crate::{
    Error, Result,
    model::*,
    provider::{AttachmentStorage, Remote, atomic_new, bounded_read, private_dir},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

const SCHEMA: &str = r#"
CREATE TABLE meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), schema INTEGER NOT NULL, epoch TEXT NOT NULL, node TEXT NOT NULL, sequence INTEGER NOT NULL, hold INTEGER NOT NULL, joined TEXT NOT NULL);
CREATE TABLE messages (tenant TEXT NOT NULL, id TEXT NOT NULL, kind TEXT NOT NULL, seat TEXT NOT NULL, intent TEXT NOT NULL, envelope BLOB, state TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', object_key TEXT NOT NULL DEFAULT '', PRIMARY KEY(tenant,id));
CREATE TABLE operations (tenant TEXT NOT NULL, principal TEXT NOT NULL, kind TEXT NOT NULL, idem TEXT NOT NULL, intent TEXT NOT NULL, response TEXT NOT NULL, id TEXT NOT NULL, message_id TEXT, PRIMARY KEY(tenant,principal,kind,idem), UNIQUE(tenant,id));
CREATE TABLE attachments (tenant TEXT NOT NULL, id TEXT NOT NULL, owner TEXT NOT NULL, filename TEXT NOT NULL, bytes BLOB NOT NULL, reference TEXT, PRIMARY KEY(tenant,id));
CREATE TABLE consumed (tenant TEXT NOT NULL, id TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(tenant,id), FOREIGN KEY(tenant,id) REFERENCES messages(tenant,id));
CREATE TABLE deliveries (tenant TEXT NOT NULL, id TEXT NOT NULL, message_id TEXT NOT NULL, mailbox TEXT NOT NULL, consumer TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0, proof_hash TEXT NOT NULL DEFAULT '', expires INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, epoch TEXT NOT NULL, outcome TEXT NOT NULL DEFAULT '', handling_reference TEXT NOT NULL DEFAULT '', dispositions TEXT NOT NULL DEFAULT '[]', PRIMARY KEY(tenant,id), UNIQUE(tenant,message_id,mailbox,consumer), FOREIGN KEY(tenant,message_id) REFERENCES messages(tenant,id));
CREATE TABLE inbound_files (tenant TEXT NOT NULL, message_id TEXT NOT NULL, filename TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(tenant,message_id,filename), FOREIGN KEY(tenant,message_id) REFERENCES messages(tenant,id));
CREATE TABLE diagnostics (tenant TEXT NOT NULL, object_key TEXT NOT NULL, digest TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(tenant,object_key,digest));
CREATE TABLE cursors (tenant TEXT NOT NULL, mailbox TEXT NOT NULL, cursor TEXT, PRIMARY KEY(tenant,mailbox));
CREATE INDEX ready_deliveries ON deliveries(tenant,mailbox,consumer,state,expires);
"#;

#[derive(Serialize, Deserialize)]
struct Marker {
    epoch: String,
    sequence: i64,
}
#[derive(Serialize, Deserialize)]
struct Intent {
    request: Send,
    sender: String,
    date: String,
}
struct LeaseRow {
    mailbox: String,
    consumer: String,
    state: String,
    generation: i64,
    proof: String,
    expires: i64,
    lease_epoch: String,
    outcome: String,
    reference: String,
    dispositions: String,
}

// Completion uses existing delivery storage, including idempotency. Each issued
// lease has a reserved slot; reviewed repairs cannot discard historical keys.
#[derive(Serialize, Deserialize)]
struct Completed {
    principal: String,
    key: String,
    intent: String,
    response: Value,
}

pub struct Store {
    conn: Connection,
    path: PathBuf,
    fault_before_commit: bool,
    failed_marker: std::cell::Cell<bool>,
}

impl Store {
    fn connect(path: &Path) -> Result<Connection> {
        // DELETE journaling and one exclusive connection keep this prototype single-writer.
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_millis(100))?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=DELETE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=FULL; PRAGMA max_page_count=8192;")?;
        conn.execute_batch("BEGIN EXCLUSIVE; COMMIT;")?;
        if rusqlite::version_number() < 3_051_003 {
            return Err(Error("unsupported_sqlite"));
        }
        Ok(conn)
    }
    pub fn init(path: impl AsRef<Path>, joined: &str) -> Result<Self> {
        let path = path.as_ref();
        if !valid_id(joined) || path.exists() {
            return Err(Error("invalid_input"));
        }
        private_dir(path.parent().ok_or(Error("invalid_input"))?)?;
        let conn = Self::connect(path)?;
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT INTO meta VALUES(1,2,?1,?2,0,0,?3)",
            params![id(), id(), joined],
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        let store = Self {
            conn,
            path: path.to_path_buf(),
            fault_before_commit: false,
            failed_marker: std::cell::Cell::new(false),
        };
        store.marker()?;
        Ok(store)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(Error("recovery_hold"));
        }
        let conn = Self::connect(path)?;
        let schema: i64 = conn.query_row("SELECT schema FROM meta WHERE singleton=1", [], |r| {
            r.get(0)
        })?;
        if schema != 2 {
            return Err(Error("unsupported_schema"));
        }
        let check: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        if check != "ok" {
            return Err(Error("recovery_hold"));
        }
        let mut store = Self {
            conn,
            path: path.to_path_buf(),
            fault_before_commit: false,
            failed_marker: std::cell::Cell::new(false),
        };
        let marker = bounded_read(&store.path.with_extension("epoch"), 8192)
            .ok()
            .and_then(|b| serde_json::from_slice::<Marker>(&b).ok());
        let (epoch, seq): (String, i64) =
            store
                .conn
                .query_row("SELECT epoch,sequence FROM meta", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
        if !marker.is_some_and(|m| m.epoch == epoch && m.sequence == seq) {
            store.enter_hold()?;
        }
        Ok(store)
    }
    fn marker(&self) -> Result<()> {
        let m = self
            .conn
            .query_row("SELECT epoch,sequence FROM meta", [], |r| {
                Ok(Marker {
                    epoch: r.get(0)?,
                    sequence: r.get(1)?,
                })
            })?;
        atomic_new(&self.path.with_extension("epoch"), &serde_json::to_vec(&m)?).map_err(|_| {
            self.failed_marker.set(true);
            Error("durable_storage_unavailable")
        })
    }
    pub fn fail_next_commit(&mut self) {
        self.fault_before_commit = true;
    }
    pub fn status(&self) -> Result<Value> {
        Ok(self.conn.query_row("SELECT epoch,node,sequence,hold,joined FROM meta",[],|r|Ok(json!({"epoch":r.get::<_,String>(0)?,"node":r.get::<_,String>(1)?,"sequence":r.get::<_,i64>(2)?,"recovery_hold":r.get::<_,bool>(3)? || self.failed_marker.get(),"joined":r.get::<_,String>(4)?,"sqlite":rusqlite::version()})))?)
    }
    fn active(&self) -> Result<()> {
        if self.failed_marker.get() {
            return Err(Error("recovery_hold"));
        }
        if self
            .conn
            .query_row("SELECT hold FROM meta", [], |r| r.get::<_, bool>(0))?
        {
            Err(Error("recovery_hold"))
        } else {
            Ok(())
        }
    }
    pub fn enter_hold(&mut self) -> Result<()> {
        let epoch = id();
        let tx = self.conn.transaction()?;
        tx.execute("UPDATE meta SET hold=1,epoch=?1", [&epoch])?;
        tx.execute(
            "UPDATE deliveries SET proof_hash='',epoch=?1 WHERE state!='handled'",
            [&epoch],
        )?;
        finish(tx, std::mem::take(&mut self.fault_before_commit))?;
        self.marker()
    }
    pub fn cutover(&mut self) -> Result<()> {
        let epoch = id();
        let joined = id();
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE meta SET hold=0,epoch=?1,joined=?2",
            params![epoch, joined],
        )?;
        tx.execute(
            "UPDATE deliveries SET state=CASE WHEN state IN ('evidence','deferred') THEN state ELSE 'uncertain' END,proof_hash='',epoch=?1 WHERE state!='handled'",
            [&epoch],
        )?;
        tx.execute("UPDATE messages SET state='blocked',reason='recovery_review_required' WHERE kind!='inbound' AND state!='published'",[])?;
        finish(tx, false)?;
        self.marker()
    }
    pub fn backup(&self, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(Error("invalid_input"));
        }
        let mut target = Connection::open(destination)?;
        let backup = rusqlite::backup::Backup::new(&self.conn, &mut target)?;
        backup.run_to_completion(64, Duration::from_millis(10), None)?;
        drop(backup);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(destination, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
    pub fn restore(snapshot: &Path, destination: &Path) -> Result<Self> {
        if destination.exists() {
            return Err(Error("invalid_input"));
        }
        private_dir(destination.parent().ok_or(Error("invalid_input"))?)?;
        let source =
            Connection::open_with_flags(snapshot, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut target = Connection::open(destination)?;
        rusqlite::backup::Backup::new(&source, &mut target)?.run_to_completion(
            64,
            Duration::from_millis(10),
            None,
        )?;
        drop(target);
        drop(source);
        let mut restored = Self::open(destination)?;
        restored.enter_hold()?;
        Ok(restored)
    }
    pub fn reviewed_requeue(&mut self, tenant: &str, message_id: &str) -> Result<()> {
        self.active()?;
        if !valid_id(message_id) {
            return Err(Error("invalid_input"));
        }
        let epoch: String = self
            .conn
            .query_row("SELECT epoch FROM meta", [], |r| r.get(0))?;
        let tx = self.conn.transaction()?;
        let raw: Option<Vec<u8>> = tx
            .query_row(
                "SELECT envelope FROM messages WHERE tenant=?1 AND id=?2 AND EXISTS(SELECT 1 FROM deliveries WHERE tenant=?1 AND message_id=?2)",
                params![tenant, message_id],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        if let Some(raw) = raw {
            let envelope = Envelope::parse(&raw)?;
            let mut state = if envelope.extensions.contains_key("a8s_control") {
                "evidence"
            } else {
                "ready"
            };
            if state == "ready" {
                for f in envelope.files {
                    let present: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM inbound_files WHERE tenant=?1 AND message_id=?2 AND filename=?3)",params![tenant,message_id,f.filename],|r|r.get(0))?;
                    if !present {
                        state = "deferred";
                        break;
                    }
                }
            }
            tx.execute("UPDATE deliveries SET state=?1,attempts=0,expires=0,proof_hash='',epoch=?2 WHERE tenant=?3 AND message_id=?4 AND state!='handled' AND json_array_length(dispositions)<32",params![state,epoch,tenant,message_id])?;
            quota(&tx, 0, 0)?;
        }
        tx.execute("UPDATE messages SET state='accepted',reason='' WHERE tenant=?1 AND id=?2 AND state='blocked' AND kind!='inbound'",params![tenant,message_id])?;
        finish(tx, false)?;
        self.marker()
    }

    pub fn accept(&mut self, p: &Principal, key: &str, request: Send) -> Result<Value> {
        self.active()?;
        idem(key)?;
        p.authorize_send(&request.recipient)?;
        if request.content.len() > MAX_JSON / 2 || request.attachment_ids.len() > 16 {
            return Err(Error("invalid_input"));
        }
        let logical = serde_json::to_string(&request)?;
        if let Some(cached) = cached(&self.conn, p, "send", key, &logical)? {
            return Ok(cached);
        }
        let mut names = std::collections::BTreeSet::new();
        for attachment in &request.attachment_ids {
            let owned: Option<(String, String)> = self
                .conn
                .query_row(
                    "SELECT owner,filename FROM attachments WHERE tenant=?1 AND id=?2",
                    params![p.tenant, attachment],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (owner, name) = owned.ok_or(Error("forbidden"))?;
            if owner != p.id {
                return Err(Error("forbidden"));
            }
            if !names.insert(name) {
                return Err(Error("duplicate_attachment_filename"));
            }
        }
        let mid = id();
        let op = id();
        let intent = Intent {
            request,
            sender: p.seat.clone(),
            date: date(),
        };
        let response = json!({"operation_id":op,"message_id":mid,"state":"accepted","status_url":format!("/v1/operations/{op}")});
        let tx = self.conn.transaction()?;
        quota(&tx, 2, 0)?;
        tx.execute("INSERT INTO messages(tenant,id,kind,seat,intent,state) VALUES(?1,?2,'outbound',?3,?4,'accepted')",params![p.tenant,mid,p.seat,serde_json::to_string(&intent)?])?;
        operation(&tx, p, "send", key, &logical, &response, Some(&mid))?;
        finish(tx, std::mem::take(&mut self.fault_before_commit))?;
        self.marker()?;
        Ok(response)
    }
    pub fn upload(&mut self, p: &Principal, key: &str, name: &str, bytes: &[u8]) -> Result<Value> {
        if !p.can_upload {
            return Err(Error("forbidden"));
        }
        self.active()?;
        idem(key)?;
        filename(name)?;
        if bytes.len() > MAX_ATTACHMENT {
            return Err(Error("body_too_large"));
        }
        let logical = format!("{name}:{}", digest(bytes));
        if let Some(old) = cached(&self.conn, p, "upload", key, &logical)? {
            return Ok(old);
        }
        let aid = id();
        let response =
            json!({"attachment_id":aid,"filename":name,"bytes":bytes.len(),"sha256":digest(bytes)});
        let tx = self.conn.transaction()?;
        quota(&tx, 2, 0)?;
        tx.execute(
            "INSERT INTO attachments VALUES(?1,?2,?3,?4,?5,NULL)",
            params![p.tenant, aid, p.id, name, bytes],
        )?;
        operation(&tx, p, "upload", key, &logical, &response, None)?;
        finish(tx, false)?;
        self.marker()?;
        Ok(response)
    }
    pub fn attachment(&self, p: &Principal, aid: &str) -> Result<(String, Vec<u8>)> {
        let own: Option<(String, Vec<u8>)> = self
            .conn
            .query_row(
                "SELECT filename,bytes FROM attachments WHERE tenant=?1 AND id=?2 AND owner=?3",
                params![p.tenant, aid, p.id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        own.ok_or(Error("not_found"))
    }
    pub fn inbound_attachment(&self, p: &Principal, mid: &str, name: &str) -> Result<Vec<u8>> {
        p.authorize_inbox(&p.seat, None)?;
        self.conn.query_row("SELECT f.bytes FROM inbound_files f JOIN deliveries d ON f.tenant=d.tenant AND f.message_id=d.message_id WHERE f.tenant=?1 AND f.message_id=?2 AND f.filename=?3 AND d.mailbox=?4 AND d.consumer=?5",params![p.tenant,mid,name,p.seat,p.consumer],|r|r.get(0)).optional()?.ok_or(Error("not_found"))
    }
    pub fn operation_status(&self, p: &Principal, op: &str) -> Result<Value> {
        self.conn.query_row("SELECT m.id,m.state,m.reason,m.envelope IS NOT NULL FROM operations o JOIN messages m ON m.tenant=o.tenant AND m.id=o.message_id WHERE o.tenant=?1 AND o.principal=?2 AND o.id=?3",params![p.tenant,p.id,op],|r|Ok(json!({"operation_id":op,"message_id":r.get::<_,String>(0)?,"state":r.get::<_,String>(1)?,"reason":r.get::<_,String>(2)?,"wire_finalized":r.get::<_,bool>(3)?}))).optional()?.ok_or(Error("not_found"))
    }
    pub fn publish_operation(
        &mut self,
        p: &Principal,
        op: &str,
        remote: &mut dyn Remote,
        storage: &mut dyn AttachmentStorage,
    ) -> Result<Value> {
        let mid: String = self.operation_status(p, op)?["message_id"]
            .as_str()
            .ok_or(Error("not_found"))?
            .into();
        let intent: String = self.conn.query_row(
            "SELECT intent FROM messages WHERE tenant=?1 AND id=?2",
            params![p.tenant, mid],
            |r| r.get(0),
        )?;
        let intent: Intent = serde_json::from_str(&intent)?;
        if intent.sender != p.seat {
            return Err(Error("forbidden"));
        }
        p.authorize_send(&intent.request.recipient)?;
        self.publish(&p.tenant, &mid, remote, storage)?;
        self.operation_status(p, op)
    }
    pub fn publish(
        &mut self,
        tenant: &str,
        mid: &str,
        remote: &mut dyn Remote,
        storage: &mut dyn AttachmentStorage,
    ) -> Result<()> {
        self.active()?;
        let (kind,intent,wire,state,reason):(String,String,Option<Vec<u8>>,String,String)=self.conn.query_row("SELECT kind,intent,envelope,state,reason FROM messages WHERE tenant=?1 AND id=?2",params![tenant,mid],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?.ok_or(Error("not_found"))?;
        if kind == "inbound" {
            return Err(Error("forbidden"));
        }
        if state == "published" {
            return Ok(());
        }
        if reason == "recovery_review_required" {
            return Err(Error("recovery_hold"));
        }
        let bytes = if let Some(wire) = wire {
            wire
        } else {
            let logical: Intent = serde_json::from_str(&intent)?;
            let mut files = Vec::new();
            for aid in logical.request.attachment_ids {
                let (name,data,reference):(String,Vec<u8>,Option<String>)=self.conn.query_row("SELECT filename,bytes,reference FROM attachments WHERE tenant=?1 AND id=?2",params![tenant,aid],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                let reference = match reference {
                    Some(r) => r,
                    None => {
                        let r = match storage.publish_upload(&aid, &name, &data) {
                            Ok(r) => r,
                            Err(_) => {
                                self.block(tenant, mid, "attachment_provider_unavailable")?;
                                return Err(Error("provider_unavailable"));
                            }
                        };
                        self.conn.execute(
                            "UPDATE attachments SET reference=?1 WHERE tenant=?2 AND id=?3",
                            params![r, tenant, aid],
                        )?;
                        r
                    }
                };
                files.push(FileRef {
                    filename: name,
                    storage: vec![reference],
                    extensions: BTreeMap::new(),
                });
            }
            let e = Envelope {
                id: mid.into(),
                date: logical.date,
                sender: logical.sender,
                to: logical.request.recipient,
                content: logical.request.content,
                files,
                extensions: BTreeMap::new(),
            };
            let bytes = match e.validate().and_then(|()| e.bytes()) {
                Ok(bytes) => bytes,
                Err(err) => {
                    self.block(
                        tenant,
                        mid,
                        if err == Error("body_too_large") {
                            "wire_body_too_large"
                        } else {
                            "invalid_wire"
                        },
                    )?;
                    return Err(err);
                }
            };
            let tx = self.conn.transaction()?;
            tx.execute("UPDATE messages SET envelope=?1,object_key=?2,state='publishing',reason='' WHERE tenant=?3 AND id=?4 AND envelope IS NULL",params![bytes,e.key()?,tenant,mid])?;
            finish(tx, false)?;
            self.marker()?;
            bytes
        };
        let e = Envelope::parse(&bytes)?;
        // No database transaction covers the provider side effect.
        if remote.publish(&e.key()?, &bytes).is_err() {
            self.block(tenant, mid, "provider_result_unknown")?;
            return Err(Error("provider_unavailable"));
        }
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE messages SET state='published',reason='' WHERE tenant=?1 AND id=?2",
            params![tenant, mid],
        )?;
        finish(tx, std::mem::take(&mut self.fault_before_commit))?;
        self.marker()
    }
    fn block(&mut self, tenant: &str, mid: &str, reason: &str) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE messages SET state='blocked',reason=?1 WHERE tenant=?2 AND id=?3",
            params![reason, tenant, mid],
        )?;
        finish(tx, false)?;
        self.marker()
    }
    pub fn outbox(&self, tenant: &str) -> Result<Vec<Value>> {
        let mut stmt=self.conn.prepare("SELECT id,kind,state,reason FROM messages WHERE tenant=?1 AND kind!='inbound' AND state!='published' ORDER BY id LIMIT 64")?;
        Ok(stmt.query_map([tenant],|r|Ok(json!({"message_id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"state":r.get::<_,String>(2)?,"reason":r.get::<_,String>(3)?})))?.collect::<std::result::Result<Vec<_>,_>>()?)
    }

    pub fn ingest(
        &mut self,
        tenant: &str,
        key: &str,
        raw: &[u8],
        consumers: &[Principal],
        storage: &mut dyn AttachmentStorage,
    ) -> Result<bool> {
        self.active()?;
        if consumers.len() > 16 {
            return Err(Error("invalid_input"));
        }
        let e = match Envelope::parse(raw) {
            Ok(e) => e,
            Err(err) => {
                self.diagnostic(tenant, key, &digest(raw), err.0)?;
                return Ok(false);
            }
        };
        if key != e.key()? {
            self.diagnostic(tenant, key, &digest(raw), "key_id_mismatch")?;
            return Ok(false);
        }
        let bytes = match e.bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                self.diagnostic(tenant, key, &digest(raw), err.0)?;
                return Ok(false);
            }
        };
        let hash = digest(&bytes);
        let old: Option<String> = self
            .conn
            .query_row(
                "SELECT digest FROM consumed WHERE tenant=?1 AND id=?2",
                params![tenant, e.id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            if old == hash {
                return Ok(true);
            }
            self.diagnostic(tenant, key, &hash, "message_conflict")?;
            return Ok(false);
        }
        let existing: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT envelope FROM messages WHERE tenant=?1 AND id=?2",
                params![tenant, e.id],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        if existing.as_ref().is_some_and(|b| digest(b) != hash) {
            self.diagnostic(tenant, key, &hash, "message_conflict")?;
            return Ok(false);
        }
        let joined: String = self
            .conn
            .query_row("SELECT joined FROM meta", [], |r| r.get(0))?;
        if e.id < joined {
            self.diagnostic(tenant, key, &hash, "before_cutover_uncertain_history")?;
            return Ok(false);
        }
        let control = e.extensions.contains_key("a8s_control");
        let recipients: Vec<_> = consumers
            .iter()
            .filter(|p| {
                p.tenant == tenant
                    && (e.to.eq_ignore_ascii_case(&p.seat)
                        || (e.to == "team:builder" && p.seat == "builder")
                        || (control && e.mailbox().is_ok_and(|m| m == p.seat)))
            })
            .collect();
        if recipients.is_empty() {
            self.diagnostic(tenant, key, &hash, "unowned_recipient")?;
            return Ok(false);
        }
        let mut fetched = Vec::new();
        let mut deferred = false;
        for f in &e.files {
            let mut found = None;
            for reference in &f.storage {
                if let Ok(b) = storage.retrieve_capability(reference) {
                    found = Some(b);
                    break;
                }
            }
            match found {
                Some(b) => fetched.push((f.filename.clone(), b)),
                None => deferred = true,
            }
        }
        let epoch: String = self
            .conn
            .query_row("SELECT epoch FROM meta", [], |r| r.get(0))?;
        let receipt = if !control {
            Some(e.receipt(if deferred { "deferred" } else { "inbox_write" }))
        } else {
            None
        };
        let tx = self.conn.transaction()?;
        quota(
            &tx,
            i64::from(existing.is_none()) + recipients.len() as i64 + i64::from(receipt.is_some()),
            if control {
                0
            } else {
                5 * recipients.len() as i64
            },
        )?;
        if existing.is_none() {
            tx.execute("INSERT INTO messages(tenant,id,kind,seat,intent,envelope,state,object_key) VALUES(?1,?2,'inbound','', '',?3,'custody',?4)",params![tenant,e.id,bytes,key])?;
        }
        tx.execute(
            "INSERT INTO consumed VALUES(?1,?2,?3)",
            params![tenant, e.id, hash],
        )?;
        for p in recipients {
            tx.execute("INSERT INTO deliveries(tenant,id,message_id,mailbox,consumer,state,epoch) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![tenant,id(),e.id,p.seat,p.consumer,if control{"evidence"}else if deferred{"deferred"}else{"ready"},epoch])?;
        }
        for (name, b) in fetched {
            tx.execute(
                "INSERT INTO inbound_files VALUES(?1,?2,?3,?4)",
                params![tenant, e.id, name, b],
            )?;
        }
        if let Some(r) = receipt {
            tx.execute("INSERT INTO messages(tenant,id,kind,seat,intent,envelope,state,object_key) VALUES(?1,?2,'receipt','_a8s','',?3,'accepted',?4)",params![tenant,r.id,r.bytes()?,r.key()?])?;
        }
        finish(tx, std::mem::take(&mut self.fault_before_commit))?;
        self.marker()?;
        Ok(true)
    }
    fn diagnostic(&mut self, tenant: &str, key: &str, hash: &str, reason: &str) -> Result<()> {
        if key.len() > 512 {
            return Err(Error("invalid_input"));
        }
        let tx = self.conn.transaction()?;
        let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM diagnostics WHERE tenant=?1 AND object_key=?2 AND digest=?3)",params![tenant,key,hash],|r|r.get(0))?;
        if exists {
            return Ok(());
        }
        quota(&tx, 1, 0)?;
        tx.execute(
            "INSERT OR IGNORE INTO diagnostics VALUES(?1,?2,?3,?4)",
            params![tenant, key, hash, reason],
        )?;
        finish(tx, false)?;
        self.marker()
    }
    pub fn poll_with_key(
        &mut self,
        p: &Principal,
        key: &str,
        remote: &mut dyn Remote,
        storage: &mut dyn AttachmentStorage,
        consumers: &[Principal],
    ) -> Result<Value> {
        self.active()?;
        idem(key)?;
        if let Some(old) = cached(&self.conn, p, "poll", key, "{}")? {
            return Ok(old);
        }
        let response = self.poll(p, remote, storage, consumers)?;
        let tx = self.conn.transaction()?;
        quota(&tx, 1, 0)?;
        operation(&tx, p, "poll", key, "{}", &response, None)?;
        finish(tx, false)?;
        self.marker()?;
        Ok(response)
    }
    pub fn poll(
        &mut self,
        p: &Principal,
        remote: &mut dyn Remote,
        storage: &mut dyn AttachmentStorage,
        consumers: &[Principal],
    ) -> Result<Value> {
        let mailboxes = if p.seat == "builder" {
            vec!["builder", "team"]
        } else {
            vec![p.seat.as_str()]
        };
        let mut custody = 0;
        let mut objects = 0;
        let mut more = false;
        for mailbox in mailboxes {
            let result = self.poll_mailbox(p, mailbox, remote, storage, consumers)?;
            custody += result["custody"].as_u64().unwrap_or(0);
            objects += result["objects"].as_u64().unwrap_or(0);
            more |= result["more"].as_bool().unwrap_or(false);
        }
        Ok(json!({"custody":custody,"objects":objects,"more":more}))
    }
    fn poll_mailbox(
        &mut self,
        p: &Principal,
        mailbox: &str,
        remote: &mut dyn Remote,
        storage: &mut dyn AttachmentStorage,
        consumers: &[Principal],
    ) -> Result<Value> {
        self.active()?;
        let cursor: Option<String> = self
            .conn
            .query_row(
                "SELECT cursor FROM cursors WHERE tenant=?1 AND mailbox=?2",
                params![p.tenant, mailbox],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let page = remote.poll(mailbox, cursor.as_deref(), 16)?;
        let mut accepted = 0;
        for (key, bytes) in &page.objects {
            if self.ingest(&p.tenant, key, bytes, consumers, storage)? {
                accepted += 1;
            }
        }
        let tx = self.conn.transaction()?;
        tx.execute("INSERT INTO cursors VALUES(?1,?2,?3) ON CONFLICT(tenant,mailbox) DO UPDATE SET cursor=excluded.cursor",params![p.tenant,mailbox,page.next])?;
        finish(tx, false)?;
        self.marker()?;
        Ok(json!({"custody":accepted,"objects":page.objects.len(),"more":page.next.is_some()}))
    }
    pub fn inbox(&self, p: &Principal, mailbox: &str) -> Result<Vec<Value>> {
        p.authorize_inbox(mailbox, None)?;
        let mut q=self.conn.prepare("SELECT d.id,d.state,d.generation,d.attempts,m.envelope FROM deliveries d JOIN messages m ON m.tenant=d.tenant AND m.id=d.message_id WHERE d.tenant=?1 AND d.mailbox=?2 AND d.consumer=?3 ORDER BY d.message_id LIMIT 16")?;
        let rows = q
            .query_map(params![p.tenant, mailbox, p.consumer], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Vec<u8>>(4)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter().map(|(delivery,state,generation,attempts,raw)|Ok(json!({"delivery_id":delivery,"state":state,"lease_generation":generation,"attempts":attempts,"provenance":"network_asserted","envelope":serde_json::from_slice::<Value>(&raw)?}))).collect()
    }
    pub fn claim(
        &mut self,
        p: &Principal,
        mailbox: &str,
        key: &str,
        request: Claim,
        time: i64,
    ) -> Result<Value> {
        self.active()?;
        idem(key)?;
        p.authorize_inbox(mailbox, Some(&request.consumer_id))?;
        if !(1..=8).contains(&request.max_messages) || request.wait_seconds != 0 {
            return Err(Error("invalid_input"));
        }
        let epoch: String = self
            .conn
            .query_row("SELECT epoch FROM meta", [], |r| r.get(0))?;
        let kind = format!("claim:{epoch}");
        let logical = serde_json::to_string(&request)?;
        if let Some(old) = cached(&self.conn, p, &kind, key, &logical)? {
            return Ok(old);
        }
        let tx = self.conn.transaction()?;
        let exhausted=tx.execute("UPDATE deliveries SET state='dead_letter',proof_hash='' WHERE tenant=?1 AND mailbox=?2 AND consumer=?3 AND (attempts>=5 OR json_array_length(dispositions)>=32) AND ((state='leased' AND expires<=?4) OR state='ready')",params![p.tenant,mailbox,p.consumer,time])?;
        let rows = {
            let mut q=tx.prepare("SELECT d.id,d.generation,d.attempts,m.envelope FROM deliveries d JOIN messages m ON m.tenant=d.tenant AND m.id=d.message_id WHERE d.tenant=?1 AND d.mailbox=?2 AND d.consumer=?3 AND d.epoch=?4 AND d.attempts<5 AND json_array_length(d.dispositions)<32 AND ((d.state='ready' AND d.expires<=?5) OR (d.state='leased' AND d.expires+2<=?5)) ORDER BY d.message_id LIMIT ?6")?;
            q.query_map(
                params![
                    p.tenant,
                    mailbox,
                    p.consumer,
                    epoch,
                    time,
                    request.max_messages as i64
                ],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut claims = Vec::new();
        for (delivery, generation, attempts, raw) in rows {
            let proof = format!("{}{}", id(), id());
            tx.execute("UPDATE deliveries SET state='leased',generation=generation+1,proof_hash=?1,expires=?2,attempts=attempts+1 WHERE tenant=?3 AND id=?4",params![digest(proof.as_bytes()),time+60,p.tenant,delivery])?;
            claims.push(json!({"delivery_id":delivery,"lease_generation":generation+1,"lease_proof":proof,"expires":time+60,"attempts":attempts+1,"epoch":epoch,"envelope":serde_json::from_slice::<Value>(&raw)?}));
        }
        let response = json!({"claims":claims});
        if claims.is_empty() {
            if let Err(err) = quota(&tx, 1, 0) {
                // Exhaustion changes existing rows; rejection of a new cached
                // empty request must not roll that terminal maintenance back.
                if exhausted > 0 && err == Error("quota") {
                    finish(tx, std::mem::take(&mut self.fault_before_commit))?;
                    self.marker()?;
                }
                return Err(err);
            }
        } else {
            completion_capacity(&tx, 1)?;
        }
        operation(&tx, p, &kind, key, &logical, &response, None)?;
        finish(tx, std::mem::take(&mut self.fault_before_commit))?;
        self.marker()?;
        Ok(response)
    }
    pub fn disposition(
        &mut self,
        p: &Principal,
        delivery: &str,
        key: &str,
        request: Disposition,
        time: i64,
    ) -> Result<Value> {
        self.active()?;
        idem(key)?;
        if !["handled", "blocked", "retry"].contains(&request.outcome.as_str())
            || request.handling_reference.is_empty()
            || request.handling_reference.len() > 256
            || request.lease_proof.len() > 128
        {
            return Err(Error("invalid_input"));
        }
        let epoch: String = self
            .conn
            .query_row("SELECT epoch FROM meta", [], |r| r.get(0))?;
        let row:Option<LeaseRow>=self.conn.query_row("SELECT mailbox,consumer,state,generation,proof_hash,expires,epoch,outcome,handling_reference,dispositions FROM deliveries WHERE tenant=?1 AND id=?2",params![p.tenant,delivery],|r|Ok(LeaseRow{mailbox:r.get(0)?,consumer:r.get(1)?,state:r.get(2)?,generation:r.get(3)?,proof:r.get(4)?,expires:r.get(5)?,lease_epoch:r.get(6)?,outcome:r.get(7)?,reference:r.get(8)?,dispositions:r.get(9)?})).optional()?;
        let LeaseRow {
            mailbox,
            consumer,
            state,
            generation,
            proof,
            expires,
            lease_epoch,
            outcome,
            reference,
            dispositions,
        } = row.ok_or(Error("not_found"))?;
        p.authorize_inbox(&mailbox, Some(&consumer))?;
        if lease_epoch != epoch
            || generation != request.lease_generation
            || proof != digest(request.lease_proof.as_bytes())
        {
            return Err(Error("stale_lease"));
        }
        let logical = serde_json::to_string(&request)?;
        let mut history: Vec<Completed> = serde_json::from_str(&dispositions)?;
        let hash = digest(logical.as_bytes());
        if let Some(old) = history.iter().find(|c| c.principal == p.id && c.key == key) {
            if old.intent != hash {
                return Err(Error("idempotency_conflict"));
            }
            return Ok(old.response.clone());
        }
        if state != "leased" || expires <= time {
            if outcome == request.outcome
                && reference == request.handling_reference
                && (state == "handled" || state == "blocked")
            {
                return Ok(json!({"delivery_id":delivery,"outcome":outcome}));
            }
            return Err(Error("stale_lease"));
        }
        let response = json!({"delivery_id":delivery,"outcome":request.outcome});
        if history.len() >= 32 {
            return Err(Error("quota"));
        }
        history.push(Completed {
            principal: p.id.clone(),
            key: key.into(),
            intent: hash,
            response: response.clone(),
        });
        let tx = self.conn.transaction()?;
        tx.execute("UPDATE deliveries SET state=?1,outcome=?2,handling_reference=?3,expires=?4,dispositions=?5 WHERE tenant=?6 AND id=?7",params![if request.outcome=="retry"{"ready"}else{&request.outcome},request.outcome,request.handling_reference,if request.outcome=="retry"{time+5}else{expires},serde_json::to_string(&history)?,p.tenant,delivery])?;
        finish(tx, std::mem::take(&mut self.fault_before_commit))?;
        self.marker()?;
        Ok(response)
    }
}

fn finish(tx: Transaction<'_>, fault: bool) -> Result<()> {
    tx.execute("UPDATE meta SET sequence=sequence+1", [])?;
    if fault {
        return Err(Error("durable_storage_unavailable"));
    }
    tx.commit()?;
    Ok(())
}
fn idem(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 128 || !key.bytes().all(|b| b.is_ascii_graphic()) {
        Err(Error("invalid_input"))
    } else {
        Ok(())
    }
}
fn accounted(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("SELECT (SELECT count(*) FROM messages)+(SELECT count(*) FROM operations)+(SELECT count(*) FROM attachments)+(SELECT count(*) FROM diagnostics)+(SELECT count(*) FROM deliveries)+(SELECT coalesce(sum(max(0,5-attempts)),0) FROM deliveries WHERE state IN ('ready','deferred','leased') AND json_array_length(dispositions)<32)",[],|r|r.get(0))?)
}
fn completion_capacity(conn: &Connection, rows: i64) -> Result<()> {
    if accounted(conn)? + rows > MAX_RECORDS {
        Err(Error("quota"))
    } else {
        Ok(())
    }
}
fn quota(conn: &Connection, rows: i64, claim_debt: i64) -> Result<()> {
    // Reserve each accepted delivery's remaining bounded claim attempts. New
    // admissions and empty claims cannot consume capacity needed to drain it.
    if accounted(conn)? + rows + claim_debt + 64 > MAX_RECORDS {
        Err(Error("quota"))
    } else {
        Ok(())
    }
}
fn cached(
    conn: &Connection,
    p: &Principal,
    kind: &str,
    key: &str,
    intent: &str,
) -> Result<Option<Value>> {
    let row:Option<(String,String)>=conn.query_row("SELECT intent,response FROM operations WHERE tenant=?1 AND principal=?2 AND kind=?3 AND idem=?4",params![p.tenant,p.id,kind,key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    match row {
        None => Ok(None),
        Some((old, response)) if old == intent => Ok(Some(serde_json::from_str(&response)?)),
        _ => Err(Error("idempotency_conflict")),
    }
}
fn operation(
    tx: &Transaction<'_>,
    p: &Principal,
    kind: &str,
    key: &str,
    intent: &str,
    response: &Value,
    mid: Option<&str>,
) -> Result<()> {
    let op = response
        .get("operation_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(id);
    tx.execute(
        "INSERT INTO operations VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            p.tenant,
            p.id,
            kind,
            key,
            intent,
            serde_json::to_string(response)?,
            op,
            mid
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    #[test]
    fn quota_does_not_truncate_custody_or_handled_history() {
        let t = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(t.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut s =
            Store::init(t.path().join("state.sqlite"), "00000000000000000000000000").unwrap();
        let tx = s.conn.transaction().unwrap();
        for n in 0..MAX_RECORDS {
            tx.execute(
                "INSERT INTO diagnostics VALUES('synthetic',?1,'digest','synthetic')",
                [n.to_string()],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        assert_eq!(
            s.accept(
                &Principal::synthetic("analyst"),
                "over",
                Send {
                    recipient: "builder".into(),
                    content: "data".into(),
                    attachment_ids: vec![]
                }
            )
            .unwrap_err(),
            Error("quota")
        );
        assert_eq!(
            s.conn
                .query_row("SELECT count(*) FROM diagnostics", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            MAX_RECORDS
        );
    }
    #[test]
    fn issued_leases_finish_at_the_record_cap_without_new_rows() {
        use crate::provider::LocalStorage;
        let t = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(t.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut s =
            Store::init(t.path().join("state.sqlite"), "00000000000000000000000000").unwrap();
        let mut storage = LocalStorage::new(t.path().join("objects")).unwrap();
        let p = Principal::synthetic("builder");
        for _ in 0..3 {
            let mut e =
                Envelope::parse(include_bytes!("../tests/fixtures/wire/simple.json")).unwrap();
            e.id = id();
            assert!(
                s.ingest(
                    "synthetic",
                    &e.key().unwrap(),
                    &e.bytes().unwrap(),
                    std::slice::from_ref(&p),
                    &mut storage
                )
                .unwrap()
            );
        }
        let claims = s
            .claim(
                &p,
                "builder",
                "three",
                Claim {
                    consumer_id: p.consumer.clone(),
                    max_messages: 3,
                    wait_seconds: 0,
                },
                100,
            )
            .unwrap();
        let count:i64=s.conn.query_row("SELECT (SELECT count(*) FROM messages)+(SELECT count(*) FROM operations)+(SELECT count(*) FROM attachments)+(SELECT count(*) FROM diagnostics)+(SELECT count(*) FROM deliveries)",[],|r|r.get(0)).unwrap();
        let tx = s.conn.transaction().unwrap();
        for n in count..MAX_RECORDS {
            tx.execute(
                "INSERT INTO diagnostics VALUES('synthetic',?1,'digest','synthetic')",
                [n.to_string()],
            )
            .unwrap();
        }
        finish(tx, false).unwrap();
        s.marker().unwrap();
        for (c, outcome) in claims["claims"]
            .as_array()
            .unwrap()
            .iter()
            .zip(["handled", "blocked", "retry"])
        {
            let request = Disposition {
                lease_generation: c["lease_generation"].as_i64().unwrap(),
                lease_proof: c["lease_proof"].as_str().unwrap().into(),
                outcome: outcome.into(),
                handling_reference: "at-cap".into(),
            };
            let delivery = c["delivery_id"].as_str().unwrap();
            assert_eq!(
                s.disposition(&p, delivery, outcome, request.clone(), 101)
                    .unwrap()["outcome"],
                outcome
            );
            assert_eq!(
                s.disposition(&p, delivery, outcome, request, 102).unwrap()["outcome"],
                outcome
            );
        }
        let after:i64=s.conn.query_row("SELECT (SELECT count(*) FROM messages)+(SELECT count(*) FROM operations)+(SELECT count(*) FROM attachments)+(SELECT count(*) FROM diagnostics)+(SELECT count(*) FROM deliveries)",[],|r|r.get(0)).unwrap();
        assert_eq!(after, MAX_RECORDS);
        assert_eq!(
            s.accept(
                &p,
                "new",
                Send {
                    recipient: "analyst".into(),
                    content: "denied".into(),
                    attachment_ids: vec![]
                }
            )
            .unwrap_err(),
            Error("quota")
        );
    }

    #[test]
    fn saturated_admission_reserves_claims_for_existing_ready_work() {
        use crate::provider::LocalStorage;
        let t = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(t.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut s =
            Store::init(t.path().join("state.sqlite"), "00000000000000000000000000").unwrap();
        let mut storage = LocalStorage::new(t.path().join("objects")).unwrap();
        let p = Principal::synthetic("builder");
        for _ in 0..3 {
            let mut e =
                Envelope::parse(include_bytes!("../tests/fixtures/wire/simple.json")).unwrap();
            e.id = id();
            assert!(
                s.ingest(
                    "synthetic",
                    &e.key().unwrap(),
                    &e.bytes().unwrap(),
                    std::slice::from_ref(&p),
                    &mut storage
                )
                .unwrap()
            );
        }
        let start = accounted(&s.conn).unwrap();
        let tx = s.conn.transaction().unwrap();
        for n in start..MAX_RECORDS - 64 {
            tx.execute(
                "INSERT INTO diagnostics VALUES('synthetic',?1,'digest','synthetic')",
                [n.to_string()],
            )
            .unwrap();
        }
        finish(tx, false).unwrap();
        s.marker().unwrap();
        assert_eq!(accounted(&s.conn).unwrap(), MAX_RECORDS - 64);
        let analyst = Principal::synthetic("analyst");
        assert_eq!(
            s.claim(
                &analyst,
                "analyst",
                "empty",
                Claim {
                    consumer_id: analyst.consumer.clone(),
                    max_messages: 8,
                    wait_seconds: 0
                },
                100
            )
            .unwrap_err(),
            Error("quota")
        );
        assert_eq!(
            s.accept(
                &p,
                "new",
                Send {
                    recipient: "analyst".into(),
                    content: "denied".into(),
                    attachment_ids: vec![]
                }
            )
            .unwrap_err(),
            Error("quota")
        );
        for n in 0..3 {
            let batch = s
                .claim(
                    &p,
                    "builder",
                    &format!("existing-{n}"),
                    Claim {
                        consumer_id: p.consumer.clone(),
                        max_messages: 1,
                        wait_seconds: 0,
                    },
                    100,
                )
                .unwrap();
            let c = &batch["claims"][0];
            assert!(!c.is_null());
            let request = Disposition {
                lease_generation: c["lease_generation"].as_i64().unwrap(),
                lease_proof: c["lease_proof"].as_str().unwrap().into(),
                outcome: "handled".into(),
                handling_reference: "drained-at-capacity".into(),
            };
            s.disposition(
                &p,
                c["delivery_id"].as_str().unwrap(),
                "handled",
                request,
                101,
            )
            .unwrap();
            assert!(accounted(&s.conn).unwrap() <= MAX_RECORDS);
        }
        assert!(
            s.inbox(&p, "builder")
                .unwrap()
                .iter()
                .all(|d| d["state"] == "handled")
        );
    }

    #[test]
    fn schema_one_state_is_rejected_without_automatic_migration() {
        let t = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(t.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let path = t.path().join("state.sqlite");
        let s = Store::init(&path, "00000000000000000000000000").unwrap();
        s.conn.execute("UPDATE meta SET schema=1", []).unwrap();
        drop(s);
        assert!(matches!(
            Store::open(&path),
            Err(Error("unsupported_schema"))
        ));
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT schema FROM meta", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn saturated_empty_claim_keeps_retry_exhaustion_durable_without_new_rows() {
        use crate::provider::LocalStorage;
        let t = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(t.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let path = t.path().join("state.sqlite");
        let mut s = Store::init(&path, "00000000000000000000000000").unwrap();
        let mut storage = LocalStorage::new(t.path().join("objects")).unwrap();
        let p = Principal::synthetic("builder");
        let e = Envelope::parse(include_bytes!("../tests/fixtures/wire/simple.json")).unwrap();
        assert!(
            s.ingest(
                "synthetic",
                &e.key().unwrap(),
                &e.bytes().unwrap(),
                std::slice::from_ref(&p),
                &mut storage
            )
            .unwrap()
        );
        let start = accounted(&s.conn).unwrap();
        let tx = s.conn.transaction().unwrap();
        for n in start..MAX_RECORDS - 64 {
            tx.execute(
                "INSERT INTO diagnostics VALUES('synthetic',?1,'digest','synthetic')",
                [n.to_string()],
            )
            .unwrap();
        }
        finish(tx, false).unwrap();
        s.marker().unwrap();
        let request = Claim {
            consumer_id: p.consumer.clone(),
            max_messages: 1,
            wait_seconds: 0,
        };
        let mut fifth = json!({});
        for attempt in 0..5 {
            let result = s
                .claim(
                    &p,
                    "builder",
                    &format!("claim-{attempt}"),
                    request.clone(),
                    100 + 62 * attempt,
                )
                .unwrap();
            fifth = result["claims"][0].clone();
            assert_eq!(fifth["attempts"], attempt + 1);
        }
        assert_eq!(accounted(&s.conn).unwrap(), MAX_RECORDS - 64);
        let before: i64 = s
            .conn
            .query_row("SELECT count(*) FROM operations", [], |r| r.get(0))
            .unwrap();
        s.fail_next_commit();
        assert_eq!(
            s.claim(&p, "builder", "exhausted", request.clone(), 410)
                .unwrap_err(),
            Error("durable_storage_unavailable")
        );
        assert_eq!(s.inbox(&p, "builder").unwrap()[0]["state"], "leased");
        assert_eq!(
            s.claim(&p, "builder", "exhausted", request.clone(), 410)
                .unwrap_err(),
            Error("quota")
        );
        assert_eq!(s.inbox(&p, "builder").unwrap()[0]["state"], "dead_letter");
        assert_eq!(accounted(&s.conn).unwrap(), MAX_RECORDS - 64);
        assert_eq!(
            s.conn
                .query_row("SELECT count(*) FROM operations", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            before
        );
        drop(s);
        let mut s = Store::open(&path).unwrap();
        assert_eq!(s.inbox(&p, "builder").unwrap()[0]["state"], "dead_letter");
        assert_eq!(
            s.claim(&p, "builder", "exhausted", request, 411)
                .unwrap_err(),
            Error("quota")
        );
        assert_eq!(
            s.disposition(
                &p,
                fifth["delivery_id"].as_str().unwrap(),
                "late",
                Disposition {
                    lease_generation: 5,
                    lease_proof: fifth["lease_proof"].as_str().unwrap().into(),
                    outcome: "handled".into(),
                    handling_reference: "expired".into()
                },
                411
            )
            .unwrap_err(),
            Error("stale_lease")
        );
        assert_eq!(s.inbox(&p, "builder").unwrap()[0]["state"], "dead_letter");
    }
}
