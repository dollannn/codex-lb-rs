use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionRerouteSignal {
    pub generation: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionConnectionSnapshot {
    pub connection_count: usize,
    pub account_ids: HashSet<Uuid>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionSignalResult {
    pub matching_connections: usize,
    pub signaled_connections: usize,
    pub account_ids: HashSet<Uuid>,
}

#[derive(Clone, Default)]
pub struct SessionConnectionRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    next_token: u64,
    connections: HashMap<u64, ConnectionEntry>,
    connections_by_key: HashMap<String, HashSet<u64>>,
    root_by_key: HashMap<String, String>,
    generation_by_root: HashMap<String, i64>,
}

struct ConnectionEntry {
    root_key_hash: String,
    aliases: HashSet<String>,
    account_id: Uuid,
    generation: i64,
    signal: watch::Sender<Option<SessionRerouteSignal>>,
}

pub struct SessionConnectionRegistration {
    token: u64,
    registry: SessionConnectionRegistry,
    reroute: watch::Receiver<Option<SessionRerouteSignal>>,
}

impl SessionConnectionRegistry {
    pub fn register(
        &self,
        root_key_hash: String,
        aliases: impl IntoIterator<Item = String>,
        account_id: Uuid,
        generation: i64,
    ) -> SessionConnectionRegistration {
        let mut inner = self.lock();
        let mut aliases = aliases.into_iter().collect::<HashSet<_>>();
        let canonical_root = inner
            .root_by_key
            .get(&root_key_hash)
            .cloned()
            .or_else(|| {
                aliases
                    .iter()
                    .filter_map(|key| inner.root_by_key.get(key).cloned())
                    .min()
            })
            .unwrap_or(root_key_hash);
        aliases.insert(canonical_root.clone());
        let current_generation = inner
            .generation_by_root
            .get(&canonical_root)
            .copied()
            .unwrap_or(generation);
        let initial_signal = (generation < current_generation).then_some(SessionRerouteSignal {
            generation: current_generation,
        });
        let (signal, reroute) = watch::channel(initial_signal);

        inner.next_token = inner.next_token.wrapping_add(1).max(1);
        let token = inner.next_token;
        for key in &aliases {
            inner
                .connections_by_key
                .entry(key.clone())
                .or_default()
                .insert(token);
            inner
                .root_by_key
                .entry(key.clone())
                .or_insert_with(|| canonical_root.clone());
        }
        inner.connections.insert(
            token,
            ConnectionEntry {
                root_key_hash: canonical_root,
                aliases,
                account_id,
                generation,
                signal,
            },
        );

        SessionConnectionRegistration {
            token,
            registry: self.clone(),
            reroute,
        }
    }

    pub fn snapshot(
        &self,
        root_key_hash: &str,
        aliases: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> SessionConnectionSnapshot {
        let inner = self.lock();
        let tokens = matching_tokens(&inner, root_key_hash, aliases);
        let account_ids = tokens
            .iter()
            .filter_map(|token| inner.connections.get(token).map(|entry| entry.account_id))
            .collect();
        SessionConnectionSnapshot {
            connection_count: tokens.len(),
            account_ids,
        }
    }

    pub fn signal_reroute(
        &self,
        root_key_hash: &str,
        aliases: impl IntoIterator<Item = impl AsRef<str>>,
        generation: i64,
    ) -> SessionSignalResult {
        let mut inner = self.lock();
        let aliases = aliases
            .into_iter()
            .map(|key| key.as_ref().to_string())
            .collect::<HashSet<_>>();
        let tokens = matching_tokens(&inner, root_key_hash, aliases.iter());
        let account_ids = tokens
            .iter()
            .filter_map(|token| inner.connections.get(token).map(|entry| entry.account_id))
            .collect::<HashSet<_>>();

        inner
            .generation_by_root
            .entry(root_key_hash.to_string())
            .and_modify(|current| *current = (*current).max(generation))
            .or_insert(generation);
        inner
            .root_by_key
            .insert(root_key_hash.to_string(), root_key_hash.to_string());
        for alias in &aliases {
            inner
                .root_by_key
                .insert(alias.clone(), root_key_hash.to_string());
        }

        let mut signaled_connections = 0;
        for token in &tokens {
            let Some(entry) = inner.connections.get_mut(token) else {
                continue;
            };
            entry.root_key_hash = root_key_hash.to_string();
            if entry.generation < generation {
                entry
                    .signal
                    .send_replace(Some(SessionRerouteSignal { generation }));
                signaled_connections += 1;
            }
        }
        for token in &tokens {
            inner
                .connections_by_key
                .entry(root_key_hash.to_string())
                .or_default()
                .insert(*token);
        }

        SessionSignalResult {
            matching_connections: tokens.len(),
            signaled_connections,
            account_ids,
        }
    }

    fn unregister(&self, token: u64) {
        let mut inner = self.lock();
        let Some(entry) = inner.connections.remove(&token) else {
            return;
        };
        let mut keys = entry.aliases;
        keys.insert(entry.root_key_hash);
        for key in keys {
            if let Some(tokens) = inner.connections_by_key.get_mut(&key) {
                tokens.remove(&token);
                if tokens.is_empty() {
                    inner.connections_by_key.remove(&key);
                }
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, RegistryInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl SessionConnectionRegistration {
    pub fn reroute_receiver(&self) -> watch::Receiver<Option<SessionRerouteSignal>> {
        self.reroute.clone()
    }
}

impl Drop for SessionConnectionRegistration {
    fn drop(&mut self) {
        self.registry.unregister(self.token);
    }
}

fn matching_tokens(
    inner: &RegistryInner,
    root_key_hash: &str,
    aliases: impl IntoIterator<Item = impl AsRef<str>>,
) -> HashSet<u64> {
    let mut keys = aliases
        .into_iter()
        .map(|key| key.as_ref().to_string())
        .collect::<HashSet<_>>();
    keys.insert(root_key_hash.to_string());
    let mapped_roots = keys
        .iter()
        .filter_map(|key| inner.root_by_key.get(key).cloned())
        .collect::<Vec<_>>();
    keys.extend(mapped_roots);
    keys.into_iter()
        .filter_map(|key| inner.connections_by_key.get(&key))
        .flatten()
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signals_every_matching_connection_once_and_cleans_up_by_token() {
        let registry = SessionConnectionRegistry::default();
        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();
        let first = registry.register(
            "root".to_string(),
            ["root".to_string(), "alias-a".to_string()],
            account_a,
            1,
        );
        let second =
            registry.register("alias-b".to_string(), ["alias-b".to_string()], account_b, 1);

        let result = registry.signal_reroute("root", ["alias-a", "alias-b"], 2);
        assert_eq!(result.matching_connections, 2);
        assert_eq!(result.signaled_connections, 2);
        assert_eq!(result.account_ids, HashSet::from([account_a, account_b]));
        assert_eq!(
            first
                .reroute_receiver()
                .borrow()
                .as_ref()
                .map(|item| item.generation),
            Some(2)
        );
        assert_eq!(
            second
                .reroute_receiver()
                .borrow()
                .as_ref()
                .map(|item| item.generation),
            Some(2)
        );

        drop(first);
        assert_eq!(
            registry
                .snapshot("root", ["alias-a", "alias-b"])
                .connection_count,
            1
        );
        drop(second);
        assert_eq!(
            registry
                .snapshot("root", ["alias-a", "alias-b"])
                .connection_count,
            0
        );
    }

    #[test]
    fn late_old_generation_registration_starts_signaled() {
        let registry = SessionConnectionRegistry::default();
        let result = registry.signal_reroute("root", ["alias"], 7);
        assert_eq!(result.matching_connections, 0);

        let registration = registry.register(
            "alias".to_string(),
            ["alias".to_string()],
            Uuid::new_v4(),
            6,
        );
        assert_eq!(
            registration
                .reroute_receiver()
                .borrow()
                .as_ref()
                .map(|item| item.generation),
            Some(7)
        );
    }
}
