use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MAX_JSON: usize = 256 * 1024;
pub const MAX_ATTACHMENT: usize = 1024 * 1024;
pub const MAX_RECORDS: i64 = 5000;
pub const MAIL_PREFIX: &str = "a8s-test-mail";
pub const STORAGE_PREFIX: &str = "a8s-test-files";

#[derive(Clone, Debug)]
pub struct Principal {
    pub tenant: String,
    pub id: String,
    pub seat: String,
    pub consumer: String,
    pub destinations: Vec<String>,
    pub can_claim: bool,
    pub can_upload: bool,
}
impl Principal {
    pub fn synthetic(seat: &str) -> Self {
        Self {
            tenant: "synthetic".into(),
            id: seat.into(),
            seat: seat.into(),
            consumer: format!("{seat}-device"),
            destinations: vec!["analyst".into(), "builder".into(), "team:builder".into()],
            can_claim: true,
            can_upload: true,
        }
    }
    pub fn authorize_send(&self, recipient: &str) -> Result<()> {
        if self.destinations.iter().any(|x| x == recipient) {
            Ok(())
        } else {
            Err(Error("forbidden"))
        }
    }
    pub fn authorize_inbox(&self, mailbox: &str, consumer: Option<&str>) -> Result<()> {
        if mailbox == self.seat
            && (consumer.is_none() || self.can_claim)
            && consumer.is_none_or(|x| x == self.consumer)
        {
            Ok(())
        } else {
            Err(Error("forbidden"))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FileRef {
    pub filename: String,
    pub storage: Vec<String>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub id: String,
    pub date: String,
    #[serde(rename = "from")]
    pub sender: String,
    pub to: String,
    pub content: String,
    pub files: Vec<FileRef>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Send {
    pub recipient: String,
    pub content: String,
    #[serde(default)]
    pub attachment_ids: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    pub consumer_id: String,
    pub max_messages: usize,
    #[serde(default)]
    pub wait_seconds: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Disposition {
    pub lease_generation: i64,
    pub lease_proof: String,
    pub outcome: String,
    pub handling_reference: String,
}

pub fn id() -> String {
    ulid::Ulid::new().to_string()
}
pub fn valid_id(raw: &str) -> bool {
    raw.len() == 26
        && raw.as_bytes()[0] <= b'7'
        && raw
            .bytes()
            .all(|x| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&x))
}
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
pub fn date() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
pub fn canonical_name(raw: &str) -> Result<String> {
    let name = raw.trim().to_ascii_lowercase();
    if name.is_empty()
        || name.len() > 64
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    {
        return Err(Error("invalid_input"));
    }
    Ok(name)
}
pub fn filename(raw: &str) -> Result<()> {
    if raw.is_empty()
        || raw.len() > 128
        || raw == "."
        || raw == ".."
        || raw.chars().any(|c| c.is_control() || "/\\%".contains(c))
    {
        Err(Error("invalid_input"))
    } else {
        Ok(())
    }
}
fn address(raw: &str) -> Result<()> {
    if raw.len() > 192 {
        return Err(Error("invalid_input"));
    }
    for part in raw.split(':') {
        canonical_name(part)?;
    }
    Ok(())
}
impl Envelope {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_JSON {
            return Err(Error("body_too_large"));
        }
        let e: Self = serde_json::from_slice(bytes)?;
        e.validate()?;
        Ok(e)
    }
    pub fn validate(&self) -> Result<()> {
        if !valid_id(&self.id)
            || chrono::DateTime::parse_from_rfc3339(&self.date).is_err()
            || !self.date.ends_with('Z')
            || self.content.len() > MAX_JSON / 2
            || self.files.len() > 16
        {
            return Err(Error("invalid_input"));
        }
        if let Some(c) = self.extensions.get("a8s_control") {
            let stages = [
                "inbox_write",
                "attachment_fetched",
                "attachment_failed",
                "deferred",
                "expired",
                "no_local_recipient",
            ];
            if self.sender != "_a8s"
                || self.to != "__a8s_receipt__"
                || !self.content.is_empty()
                || !self.files.is_empty()
                || c.get("type").and_then(Value::as_str) != Some("delivery_receipt")
                || c.get("version").and_then(Value::as_i64) != Some(2)
                || !c
                    .get("for_id")
                    .and_then(Value::as_str)
                    .is_some_and(valid_id)
                || !c
                    .get("stage")
                    .and_then(Value::as_str)
                    .is_some_and(|s| stages.contains(&s))
            {
                return Err(Error("invalid_input"));
            }
            address(
                c.get("sender")
                    .and_then(Value::as_str)
                    .ok_or(Error("invalid_input"))?,
            )?;
            let recipients = c
                .get("recipients")
                .and_then(Value::as_array)
                .ok_or(Error("invalid_input"))?;
            if recipients.is_empty() || recipients.len() > 16 {
                return Err(Error("invalid_input"));
            }
            for recipient in recipients {
                address(recipient.as_str().ok_or(Error("invalid_input"))?)?;
            }
        } else {
            address(&self.sender)?;
            address(&self.to)?;
        }
        let mut names = std::collections::BTreeSet::new();
        for f in &self.files {
            filename(&f.filename)?;
            if !names.insert(&f.filename) {
                return Err(Error("duplicate_attachment_filename"));
            }
            if f.storage.is_empty()
                || f.storage.len() > 4
                || f.storage.iter().any(|s| s.len() > 4096)
            {
                return Err(Error("invalid_input"));
            }
        }
        Ok(())
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        let b = serde_json::to_vec(self)?;
        if b.len() > MAX_JSON {
            return Err(Error("body_too_large"));
        }
        Ok(b)
    }
    pub fn mailbox(&self) -> Result<String> {
        let target = if let Some(c) = self.extensions.get("a8s_control") {
            c.get("sender")
                .and_then(Value::as_str)
                .ok_or(Error("invalid_input"))?
        } else {
            &self.to
        };
        canonical_name(target.split(':').next().ok_or(Error("invalid_input"))?)
    }
    pub fn key(&self) -> Result<String> {
        Ok(format!(
            "{MAIL_PREFIX}/{}/{}.json",
            self.mailbox()?,
            self.id
        ))
    }
    pub fn receipt(&self, stage: &str) -> Self {
        let control = serde_json::json!({"type":"delivery_receipt","version":2,"for_id":self.id,"sender":self.sender,"recipients":[self.to],"stage":stage,"files":[],"detail":""});
        Self {
            id: id(),
            date: date(),
            sender: "_a8s".into(),
            to: "__a8s_receipt__".into(),
            content: String::new(),
            files: vec![],
            extensions: BTreeMap::from([("a8s_control".into(), control)]),
        }
    }
}
pub fn disjoint(a: &str, b: &str) -> bool {
    let a = a.trim_matches('/');
    let b = b.trim_matches('/');
    !(a == b || a.starts_with(&format!("{b}/")) || b.starts_with(&format!("{a}/")))
}
