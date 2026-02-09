use std::sync::Arc;

use crate::{
    fetch::Client,
    server::Error,
    server::State,
    store::{BlockMeta, Store},
    Family,
};

const MAX_HASH_GAP_REPAIR: u32 = 1000;

pub async fn headers(
    state: Arc<State>,
    client: Option<&Client>,
    family: Family,
) -> Result<(), Error> {
    let mut blocks_hash_ts = state.blocks_hash_ts.lock().await;
    let mut i = 0usize;
    let metas: Vec<BlockMeta> = state.store.iter_hash_ts().collect();
    for meta in metas {
        if i as u32 != meta.height() {
            let gap_start = i as u32;
            let gap_end = meta.height();
            let gap_len = gap_end.saturating_sub(gap_start);

            if gap_len == 0 {
                return Err(Error::DBCorrupted(format!(
                    "hashes CF out-of-order entry at height {}, reindex required",
                    meta.height()
                )));
            }

            let client = client.ok_or_else(|| {
                Error::DBCorrupted(format!(
                    "hashes CF gap detected: expected height {}, found {}. \
DB is inconsistent; reindex required",
                    i,
                    meta.height()
                ))
            })?;

            if gap_len > MAX_HASH_GAP_REPAIR {
                return Err(Error::DBCorrupted(format!(
                    "hashes CF gap too large to repair ({} blocks from {} to {}), reindex required",
                    gap_len,
                    gap_start,
                    gap_end - 1
                )));
            }

            log::warn!(
                "hashes CF gap detected ({} blocks from {} to {}), attempting repair",
                gap_len,
                gap_start,
                gap_end - 1
            );

            for height in gap_start..gap_end {
                let hash = client
                    .block_hash(height)
                    .await
                    .map_err(|e| Error::DBCorrupted(format!("failed to fetch block hash: {e}")))?
                    .ok_or_else(|| {
                        Error::DBCorrupted(format!(
                            "missing block hash at height {height} while repairing hashes CF"
                        ))
                    })?;
                let header = client
                    .block_header(hash, family)
                    .await
                    .map_err(|e| {
                        Error::DBCorrupted(format!(
                            "failed to fetch block header for {hash}: {e}"
                        ))
                    })?;
                let repaired = BlockMeta::new(height, hash, header.time());
                state
                    .store
                    .put_hash_ts(&repaired)
                    .map_err(|e| Error::DBCorrupted(format!("failed to write hash meta: {e}")))?;
                blocks_hash_ts.push((repaired.hash(), repaired.timestamp()));
            }
            i = gap_end as usize;
        }
        blocks_hash_ts.push((meta.hash(), meta.timestamp()));
        i += 1;
    }
    log::info!("{i} block meta preloaded");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::headers;
    use crate::server::Error;
    use crate::store::{db::DBStore, AnyStore, BlockMeta, Store};
    use age::x25519::Identity;
    use bitcoin::NetworkKind;
    use elements::{hashes::Hash, BlockHash};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::State;

    #[tokio::test]
    async fn test_preload_detects_hash_gap() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let db = DBStore::open(tempdir.path(), 64, false).unwrap();

        let block0 = BlockMeta::new(0, BlockHash::all_zeros(), 0);
        db.update(&block0, vec![], BTreeMap::new(), BTreeMap::new())
            .unwrap();

        let block2 = BlockMeta::new(2, BlockHash::all_zeros(), 0);
        db.update(&block2, vec![], BTreeMap::new(), BTreeMap::new())
            .unwrap();

        let state = Arc::new(State::new(
            AnyStore::Db(db),
            Identity::generate(),
            bitcoin::PrivateKey::generate(NetworkKind::Test),
            100,
            5,
            1000,
        )
        .unwrap());

        let result = headers(state, None, crate::Family::Elements).await;
        assert!(matches!(result, Err(Error::DBCorrupted(_))));
    }
}
