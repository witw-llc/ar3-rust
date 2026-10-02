use crate::{Error, Result, model::*};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct Page {
    pub objects: Vec<(String, Vec<u8>)>,
    pub next: Option<String>,
}
pub trait Remote: std::marker::Send {
    fn publish(&mut self, key: &str, bytes: &[u8]) -> Result<()>;
    fn poll(&mut self, mailbox: &str, cursor: Option<&str>, limit: usize) -> Result<Page>;
}
pub trait ObjectClient: std::marker::Send {
    fn put_exact(&mut self, key: &str, bytes: &[u8]) -> Result<()>;
    fn get_exact(&mut self, key: &str, limit: usize) -> Result<Vec<u8>>;
    fn list_mail(
        &mut self,
        prefix: &str,
        delimiter: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>)>;
}
pub struct S3Remote<C: ObjectClient>(pub C);
impl<C: ObjectClient> Remote for S3Remote<C> {
    fn publish(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        let e = Envelope::parse(bytes)?;
        if key != e.key()? {
            return Err(Error("invalid_input"));
        }
        self.0.put_exact(key, bytes)
    }
    fn poll(&mut self, mailbox: &str, cursor: Option<&str>, limit: usize) -> Result<Page> {
        if !(1..=64).contains(&limit) {
            return Err(Error("invalid_input"));
        }
        let prefix = format!("{MAIL_PREFIX}/{}/", canonical_name(mailbox)?);
        let (keys, next) = self.0.list_mail(&prefix, "/", cursor, limit)?;
        let mut objects = Vec::new();
        for key in keys {
            // Failed GETs are revisited on wrap, without starving later keys.
            if let Ok(bytes) = self.0.get_exact(&key, MAX_JSON) {
                objects.push((key, bytes));
            }
        }
        Ok(Page { objects, next })
    }
}

pub struct LocalObjects {
    root: PathBuf,
}
impl LocalObjects {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        private_dir(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }
    fn exact(&self, key: &str) -> Result<PathBuf> {
        if key.len() > 512
            || key.split('/').any(|s| {
                s.is_empty()
                    || s == "."
                    || s == ".."
                    || s.contains(['\\', '%'])
                    || s.chars().any(char::is_control)
            })
        {
            return Err(Error("invalid_input"));
        }
        let mut path = self.root.clone();
        for segment in key.split('/') {
            path.push(segment);
            if fs::symlink_metadata(&path).is_ok_and(|s| s.file_type().is_symlink()) {
                return Err(Error("forbidden"));
            }
        }
        Ok(path)
    }
}
impl ObjectClient for LocalObjects {
    fn put_exact(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        let dest = self.exact(key)?;
        if dest.exists() {
            if bounded_read(&dest, MAX_ATTACHMENT + 8192)? == bytes {
                return Ok(());
            }
            return Err(Error("message_conflict"));
        }
        private_dir(dest.parent().ok_or(Error("invalid_input"))?)?;
        atomic_new(&dest, bytes)
    }
    fn get_exact(&mut self, key: &str, limit: usize) -> Result<Vec<u8>> {
        bounded_read(&self.exact(key)?, limit)
    }
    fn list_mail(
        &mut self,
        prefix: &str,
        delimiter: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>)> {
        if delimiter != "/"
            || !prefix.starts_with(&format!("{MAIL_PREFIX}/"))
            || !(1..=64).contains(&limit)
        {
            return Err(Error("forbidden"));
        }
        let folder = self.exact(prefix.trim_end_matches('/'))?;
        if !folder.exists() {
            return Ok((vec![], None));
        }
        // Keep only the smallest page, rather than collecting the whole mailbox.
        let mut names = std::collections::BTreeSet::new();
        for entry in fs::read_dir(folder)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let key = format!("{prefix}{name}");
            if cursor.is_some_and(|c| key.as_str() <= c) {
                continue;
            }
            names.insert(key);
            if names.len() > limit + 1 {
                names.pop_last();
            }
        }
        let more = names.len() > limit;
        if more {
            names.pop_last();
        }
        let next = if more { names.last().cloned() } else { None };
        Ok((names.into_iter().collect(), next))
    }
}

pub trait AttachmentStorage: std::marker::Send {
    fn publish_upload(
        &mut self,
        attachment_id: &str,
        filename: &str,
        bytes: &[u8],
    ) -> Result<String>;
    fn retrieve_capability(&mut self, capability: &str) -> Result<Vec<u8>>;
}
#[derive(Serialize, Deserialize)]
struct CapabilityObject {
    capability: String,
    digest: String,
}
pub struct LocalStorage {
    objects: LocalObjects,
}
impl LocalStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            objects: LocalObjects::new(path)?,
        })
    }
}
impl AttachmentStorage for LocalStorage {
    fn publish_upload(&mut self, attachment_id: &str, name: &str, bytes: &[u8]) -> Result<String> {
        if !valid_id(attachment_id) || bytes.len() > MAX_ATTACHMENT {
            return Err(Error("invalid_input"));
        }
        filename(name)?;
        let key = format!("{STORAGE_PREFIX}/{attachment_id}/{name}");
        let registry = format!(".capabilities/{attachment_id}.json");
        if let Ok(raw) = self.objects.get_exact(&registry, 8192) {
            let old: CapabilityObject = serde_json::from_slice(&raw)?;
            if old.digest != digest(bytes) {
                return Err(Error("message_conflict"));
            }
            return Ok(old.capability);
        }
        self.objects.put_exact(&key, bytes)?;
        let mut url = url::Url::parse("https://objects.example.invalid/")
            .map_err(|_| Error("invalid_input"))?;
        url.set_path(&key);
        url.query_pairs_mut().append_pair("cap", &id());
        let capability = url.to_string();
        self.objects.put_exact(
            &registry,
            &serde_json::to_vec(&CapabilityObject {
                capability: capability.clone(),
                digest: digest(bytes),
            })?,
        )?;
        Ok(capability)
    }
    fn retrieve_capability(&mut self, capability: &str) -> Result<Vec<u8>> {
        let u = url::Url::parse(capability).map_err(|_| Error("forbidden"))?;
        if u.scheme() != "https"
            || u.host_str() != Some("objects.example.invalid")
            || u.port().is_some()
            || !u.username().is_empty()
            || u.password().is_some()
            || u.fragment().is_some()
        {
            return Err(Error("forbidden"));
        }
        let parts: Vec<_> = u.path().trim_start_matches('/').split('/').collect();
        if parts.len() != 3 || parts[0] != STORAGE_PREFIX || !valid_id(parts[1]) {
            return Err(Error("forbidden"));
        }
        let name = percent_encoding::percent_decode_str(parts[2])
            .decode_utf8()
            .map_err(|_| Error("forbidden"))?;
        filename(&name)?;
        let raw = self
            .objects
            .get_exact(&format!(".capabilities/{}.json", parts[1]), 8192)?;
        let registered: CapabilityObject = serde_json::from_slice(&raw)?;
        // The exact capability is the authority; prefix membership alone grants nothing.
        if digest(capability.as_bytes()) != digest(registered.capability.as_bytes()) {
            return Err(Error("forbidden"));
        }
        let bytes = self.objects.get_exact(
            &format!("{STORAGE_PREFIX}/{}/{}", parts[1], name),
            MAX_ATTACHMENT,
        )?;
        if digest(&bytes) != registered.digest {
            return Err(Error("provider_unavailable"));
        }
        Ok(bytes)
    }
}

pub trait MqttSession {
    fn publish_qos1(&mut self, topic: &str, bytes: &[u8]) -> Result<()>;
    fn ack_current(&mut self, packet: u16) -> Result<()>;
    fn generation(&self) -> u64;
}
pub struct MqttAdapter<S: MqttSession> {
    pub session: S,
    pub topic: String,
}
impl<S: MqttSession> MqttAdapter<S> {
    pub fn publish(&mut self, bytes: &[u8]) -> Result<()> {
        Envelope::parse(bytes)?;
        self.session.publish_qos1(&self.topic, bytes)
    }
    pub fn received(
        &mut self,
        packet: u16,
        generation: u64,
        bytes: &[u8],
        accept: impl FnOnce(&[u8]) -> Result<bool>,
    ) -> Result<bool> {
        if !accept(bytes)? {
            return Ok(false);
        }
        if generation != self.session.generation() {
            return Ok(false);
        }
        self.session.ack_current(packet)?;
        Ok(true)
    }
}

pub fn private_dir(path: &Path) -> Result<()> {
    let existed = path.exists();
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if existed {
            if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
                return Err(Error("unsafe_state_directory"));
            }
        } else {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}
pub fn bounded_read(path: &Path, limit: usize) -> Result<Vec<u8>> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(Error("forbidden"));
    }
    let f = File::open(path)?;
    let mut bytes = Vec::new();
    f.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(Error("body_too_large"));
    }
    Ok(bytes)
}
pub fn atomic_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", id()));
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    File::open(path.parent().ok_or(Error("invalid_input"))?)?.sync_all()?;
    Ok(())
}
