use std::{
    collections::{BTreeMap, VecDeque},
    hash::Hasher,
    sync::Mutex,
};

use elements::OutPoint;
use fxhash::FxHasher;

use crate::{error_panic, ScriptHash};

use super::{BlockMeta, SpentUtxo, Store, TxSeen, REORG_BUFFER_MAX_DEPTH};
use crate::V;

#[derive(Debug)]
pub struct MemoryStore {
    utxos: Mutex<BTreeMap<OutPoint, ScriptHash>>,
    history: Mutex<BTreeMap<ScriptHash, Vec<TxSeen>>>,
    reorg_data: Mutex<VecDeque<ReorgData>>,
}

#[derive(Debug, Default)]
struct ReorgData {
    height: crate::Height,
    spent: Vec<(OutPoint, ScriptHash)>,
    history: BTreeMap<ScriptHash, Vec<TxSeen>>,
    utxos_created: BTreeMap<OutPoint, ScriptHash>,
}

impl Store for MemoryStore {
    fn hash(&self, script: &[u8]) -> ScriptHash {
        let mut hasher = FxHasher::default();
        // TODO should be salted
        hasher.write(script);
        hasher.finish()
    }

    fn iter_hash_ts(&self) -> Box<dyn Iterator<Item = BlockMeta> + '_> {
        // it's not needed to preload
        Box::new(vec![].into_iter())
    }

    fn get_utxos(
        &self,
        outpoints: &[elements::OutPoint],
    ) -> anyhow::Result<Vec<Option<ScriptHash>>> {
        let mut result = Vec::with_capacity(outpoints.len());
        for outpoint in outpoints {
            result.push(self.utxos.lock().unwrap().get(outpoint).cloned());
        }
        Ok(result)
    }

    fn get_history(
        &self,
        scripts: &[crate::ScriptHash],
    ) -> anyhow::Result<Vec<Vec<super::TxSeen>>> {
        let mut result = Vec::with_capacity(scripts.len());
        for script in scripts {
            result.push(
                self.history
                    .lock()
                    .unwrap()
                    .get(script)
                    .cloned()
                    .unwrap_or(vec![]),
            );
        }
        Ok(result)
    }

    fn update(
        &self,
        block_meta: &BlockMeta,
        utxo_spent: Vec<SpentUtxo>,
        history_map: std::collections::BTreeMap<ScriptHash, Vec<TxSeen>>,
        utxo_created: std::collections::BTreeMap<elements::OutPoint, ScriptHash>,
    ) -> anyhow::Result<()> {
        let mut history_map = history_map;
        let only_outpoints: Vec<_> = utxo_spent.iter().map(|e| e.outpoint).collect();
        self.remove_utxos(&only_outpoints);

        for spent in utxo_spent.iter() {
            let el = history_map.entry(spent.script_hash).or_default();
            el.push(TxSeen::new(
                spent.txid,
                block_meta.height(),
                V::Vin(spent.vin),
            ));
        }

        self.update_history(&history_map);
        self.insert_utxos(&utxo_created);
        self.push_reorg_data(ReorgData {
            height: block_meta.height(),
            spent: utxo_spent
                .iter()
                .map(|spent| (spent.outpoint, spent.script_hash))
                .collect(),
            history: history_map,
            utxos_created: utxo_created,
        });
        Ok(())
    }

    fn reorg(&self) {
        let reorg_data = self.pop_reorg_data();
        let spent: BTreeMap<_, _> = reorg_data.spent.into_iter().collect();
        self.insert_utxos(&spent);
        self.remove_utxos_created(&reorg_data.utxos_created);
        self.remove_history_entries(&reorg_data.history);
    }

    fn ibd_finished(&self) {}

    fn put_hash_ts(&self, meta: &BlockMeta) -> anyhow::Result<()> {
        self.write_hash_ts(meta)
    }
}

impl MemoryStore {
    fn remove_utxos(&self, outpoints: &[OutPoint]) {
        let mut utxos = self.utxos.lock().unwrap();
        for outpoint in outpoints {
            if utxos.remove(outpoint).is_none() {
                log::warn!("missing utxo {outpoint} while applying block");
            }
        }
    }
    fn update_history(&self, add: &BTreeMap<ScriptHash, Vec<TxSeen>>) {
        let mut history = self.history.lock().unwrap();
        for (k, v) in add {
            history.entry(*k).or_default().extend(v.clone());
        }
    }
    fn insert_utxos(&self, adds: &BTreeMap<OutPoint, ScriptHash>) {
        self.utxos.lock().unwrap().extend(adds);
    }

    fn remove_utxos_created(&self, utxos_created: &BTreeMap<OutPoint, ScriptHash>) {
        let mut utxos = self.utxos.lock().unwrap();
        for outpoint in utxos_created.keys() {
            utxos.remove(outpoint);
        }
    }

    fn remove_history_entries(&self, to_remove: &BTreeMap<ScriptHash, Vec<TxSeen>>) {
        if to_remove.is_empty() {
            return;
        }
        let mut history = self.history.lock().unwrap();
        for (script_hash, entries_to_remove) in to_remove {
            if let Some(current_entries) = history.get_mut(script_hash) {
                for entry_to_remove in entries_to_remove {
                    current_entries.retain(|entry| {
                        !(entry.txid == entry_to_remove.txid
                            && entry.height == entry_to_remove.height
                            && entry.v == entry_to_remove.v)
                    });
                }
                if current_entries.is_empty() {
                    history.remove(script_hash);
                }
            }
        }
    }

    fn push_reorg_data(&self, data: ReorgData) {
        let mut reorg_data = self.reorg_data.lock().unwrap();
        reorg_data.push_back(data);
        if reorg_data.len() > REORG_BUFFER_MAX_DEPTH {
            let dropped = reorg_data.pop_front().expect("len > max depth");
            log::warn!(
                "reorg buffer depth exceeded ({}); dropping undo data for height {}",
                REORG_BUFFER_MAX_DEPTH,
                dropped.height
            );
        }
    }

    fn pop_reorg_data(&self) -> ReorgData {
        let mut reorg_data = self.reorg_data.lock().unwrap();
        reorg_data.pop_back().unwrap_or_else(|| {
            error_panic!(
                "reorg depth exceeded in-memory buffer ({}); reindex required",
                REORG_BUFFER_MAX_DEPTH
            )
        })
    }

    pub(crate) fn new() -> Self {
        Self {
            utxos: Mutex::new(BTreeMap::new()),
            history: Mutex::new(BTreeMap::new()),
            reorg_data: Mutex::new(VecDeque::new()),
        }
    }

    fn write_hash_ts(&self, _meta: &BlockMeta) -> anyhow::Result<()> {
        Ok(())
    }
}
