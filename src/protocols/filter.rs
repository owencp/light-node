use super::{ChainStore, HeaderProviderWrapper, HeaderVerifier,Peers};
use crate::store::Store;
use crate::{types::SyncShared, BAD_MESSAGE_BAN_TIME};
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{core::{BlockNumber, HeaderView},packed, prelude::*};
use crossbeam_channel::Receiver;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use crossbeam_channel::{unbounded,Sender,Receiver};
use ckb_hash::blake2b_256;

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const GCS_FILTER_MSG_TOKEN: u64 = 0;
//const SEND_GET_GCS_FILTER_HASHES_TOKEN: u64 = 1;
//const SEND_GET_GCS_CHECKPOINT_TOKEN: u64 = 2;
const CONTROL_RECEIVER_TOKEN: u64 = 1;

const MAX_PEER_SIZE: usize = 7;
const MAX_FILTER_RANGE_SIZE: usize = 2000;
const MIN_CHECK_POINT_INTERVAL: u32 = 200_000;

struct CheckPoints{
    flag:bool,
    stop_hash:packed::Byte32,
    check_points:Box<packed::Byte32Vec>
}

impl CheckPoints {
    pub fn new(cps:packed::GcsFilterCheckPoint, flag:bool) -> Self {
        Self{
            flag,
            stop_hash:cps.stop_hash,
            check_points:Box::new(cps.filter_hashes),
        }
    }
}


pub struct FilterProtocol<S> {
    store: ChainStore<S>,
    consensus: Consensus,
    control_receiver: Receiver<ControlMessage>,
    //pending_get_filtered_blocks: Arc<RwLock<HashSet<packed::Byte32>>>,
    peers: Peers,
    filter_hashes: Arc<RwLock<HashMap<PeerIndex, packed::GcsFilterHashes>>>,
    check_points: Arc<RwLock<HashMap<PeerIndex,CheckPoints>>>,
    inner_sender: Sender<GcsMessage>,
    inner_receiver: Receiver<GcsMessage>,
}

impl<S> FilterProtocol<S> {
    pub fn new(
        store: ChainStore<S>,
        consensus: Consensus,
        control_receiver: Receiver<ControlMessage>,
        peers:Peers,
    ) -> Self {
        let (inner_sender, inner_receiver) = unbounded();
        Self {
            store,
            consensus,
            control_receiver,
            //pending_get_filtered_blocks: Arc::new(HashMap::new()),
            peers,
            filter_hashes: Arc::new(HashMap::new()),
            check_points: Arc::new(HashMap::new()),
            inner_sender,
            inner_receiver,
        }
    }
    
    //insert filter hashes
    fn insert_hashes(&mut self, peer:PeerIndex, _hashes: packed::GcsFilterHashes){
        let stop_hash = _hashes.stop_hash().clone();
        let parent_hash = _hashes.parent_hash().clone();
        let hash_len = _hashes.len();
        let parent_num = self.store.get_header(parent_hash.clone())?.number();
        let stop_num = self.store.get_header(stop_hash.clone())?.number();
        
        
        //check parent_hash
        if stop_num >= parent_num && stop_num - parent_num == len  {
            self.filter_hashes.write().unwrap().insert(peer.clone(), _hashes);
            info!("insert hashes from peer: {}, stop_hash is {}, parent_hash is {}, hash len is {}.",
                    peer,
                    stop_hash,
                    parent_hash,
                    len
            );
            //send msg 
            self.inner_sender.send(GcsMessage::GetFilters(peer.clone()));
        }
    }
    
    //insert checkpoints
    //check_points: Arc<RwLock<HashMap<PeerIndex,Box<packed::GcsFilterCheckPoint>>>>
    fn insert_checkpoints(&mut self, peer:PeerIndex, _check_points: packed::GcsFilterCheckPoint){
        let stop_hash = _check_points.stop_hash();
        let last_filter_hash = _check_points.filter_hashes()[_check_points.filter_hashes().len() - 1].clone();
        let _times:u32 = 1;

        for checkpoints in self.check_points.read().unwrap().values() {
            //if the flag is set to true, return without process
            if checkpoints.flag == true {
                return;
            }
            if checkpoints.stop_hash == stop_hash {
                let len = checkpoints.filter_hashes().len();
                if checkpoints.filter_hashes()[len -1] == last_filter_hash {
                    _times += 1;
                }
            }
        }
        
        let valid_peers = self.peers.valid_peer_num();
        let data_len = self.check_points.read().unwrap().len();
        if _times == valid_peers || _times > 1 && _times > data_len + 1 / 2 {
            self.check_points.write().unwrap().clear();
            self.check_points.write().unwrap().insert(peer.clone(), CheckPoints::new(_check_points, true));
            self.inner_sender.send(GcsMessage::GetFilterHashes(peer.clone()));
        }else{
            self.check_points.write().unwrap().insert(peer.clone(), CheckPoints::new(_check_points,false));
        }
    }
}

pub enum ControlMessage {
    SendTransaction(packed::Transaction),
}

pub enum GcsMessage {
    GetCheckPoint,
    GetFilterHashes(PeerIndex),
    GetFilters(PeerIndex),
}

impl<S: Store + Send + Sync> CKBProtocolHandler for FilterProtocol<S> {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        nc.set_notify(Duration::from_millis(10), GCS_FILTER_MSG_TOKEN)
            .expect("set_notify should be ok");
        nc.set_notify(Duration::from_millis(100), CONTROL_RECEIVER_TOKEN)
            .expect("set_notify should be ok");
    }

    fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        match token {
            GCS_FILTER_MSG_TOKEN => {
                if let Ok(msg) = self.inner_receiver.try_recv() { 
                    match msg {
                        GcsMessage::GetCheckPoint => {
                            let interval = MIN_CHECK_POINT_INTERVAL as usize;
                            let stop_num = self.store.tip()
                                .expect("store should be OK")
                                .expect("tip stored").number();
                            if stop_num == 0 {
                                //resend GetCheckPoint
                                self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();
                                return;
                            }
                            let stop_num = self.store.tip()
                                .expect("store should be OK")
                                .expect("tip stored").hash();
                            let message = packed::GcsFilterMessage::new_builder()
                                .set(
                                    packed::GetGcsFilterCheckPoint::new_builder()
                                        .stop_hash(stop_hash.clone())
                                        .interval(interval.pack())
                                        .build(),
                                )
                                .build();

                            let peers: Vec<PeerIndex> = self
                                .peers
                                .read()
                                .unwrap()
                                .iter()
                                .filter_map(|(peer, peer_state)| {
                                    if peer_state.state == true {
                                        Some(*peer)
                                    } else {
                                        None
                                    } 
                                })
                                .collect();
 
                            peers
                                .into_iter()
                                .for_each(|peer| {
                                    if let Err(err) = nc.send_message_to(peer, message.as_bytes()){
                                        debug!("GcsFilterProtocol send GetGcsFilterCheckPoint error: {:?}", err);
                                    }
                                });
                        }
                        GcsMessage::GetFilterHashes(peer) => {
                            //get the last filter info 
                            let start_block_hash = self.store.get_lastest_hash()?.expect("stored lastest filter");
                            let start_block_num = self.store.get_header(start_block_hash)?.number();
                            //get stop hash from checkpoint
                            let stop_hash = self.check_points.read().unwrap()
                                .get(peer.clone()).unwrap().stop_hash.clone();
                            let message = packed::GcsFilterMessage::new_builder()
                                .set(
                                    packed::GetGcsFilterHashes::new_builder()
                                        .start_number(start_block_num.pack())
                                        .stop_hash(stop_hash.clone())
                                        .build(),
                                )
                                .build();

                            let peers: Vec<PeerIndex> = self
                                .peers
                                .read()
                                .unwrap()
                                .iter()
                                .filter_map(|(peer, peer_state)| {
                                    if peer_state.state == true {
                                        Some(*peer)
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            peers
                                .into_iter()
                                .for_each(|peer| {
                                    if let Err(err) = nc.send_message_to(peer, message.as_bytes()){
                                        debug!("GcsFilterProtocol send GetGcsFilterhash error: {:?}", err);
                                    }
                                });
                            //send GcsMessage::GetCheckPoint
                            self.check_points.write().unwrap().clear();
                            self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();
                             
                        } 
                        GcsMessage::GetFilters(peer) => {
                            //send get filters
                            //get the last filter info
                            let start_block_hash = self.store.get_lastest_hash()?.expect("stored lastest filter");
                            let start_block_num = self.store.get_header(start_block_hash)?.number();
                            //get stop hash from filterhashes
                            let stop_hash = self
                                .filter_hashes.read().unwrap()
                                .get(peer.clone()).unwrap()
                                .stop_hash.clone();
 
                            let message = packed::GcsFilterMessage::new_builder()
                                .set(
                                    packed::GetGcsFilters::new_builder()
                                        .start_number(start_block_num.pack())
                                        .stop_hash(stop_hash.clone())
                                        .build(),
                                ).build();
                            //send message to peer
                            if let Err(err) = nc.send_message_to(peer.clone(), message.as_bytes()){
                                debug!("GcsFilterProtocol send peer {} GetGcsFilters error: {:?}", err, peer);
                            }
                        }
                        _ => unreachable!(),
                    }
            }    
            CONTROL_RECEIVER_TOKEN => {
                if let Ok(msg) = self.control_receiver.try_recv() {
                    match msg {
                        ControlMessage::SendTransaction(transaction) => {
                            if let Some((peer, _)) = self.peer_filter_hash_seed {
                                let message = packed::GcsFilterMessage::new_builder()
                                    .set(
                                        packed::SendTransaction::new_builder()
                                            .transaction(transaction)
                                            .build(),
                                    )
                                    .build();
                                for (peer, _) in &self.peers {
                                    if let Ok(ret) = nc.send_message_to(peer, message.as_bytes()){
                                        return;
                                    }
                                    debug!("GcsFilterProtocol send SendTransaction error: {:?}", ret);
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn connected(
        &mut self,
        _nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        _version: &str,
    ) {
        //not insert peers
        //at the first peer connected, send GetCheckPoint to starting
        if self.peers.peers_num() == 1 {
            self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();
        }
    }

    fn received(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex, data: Bytes) {
        let message = match packed::GcsFilterMessage::from_slice(&data) {
            Ok(msg) => msg.to_enum(),
            _ => {
                info!("peer {} sends us a malformed Gcsfilter message", peer);
                nc.ban_peer(
                    peer,
                    BAD_MESSAGE_BAN_TIME,
                    String::from("send us a malformed Gcsfilter message"),
                );
                return;
            }
        };

        match message.as_reader() {
            packed::GcsFilterMessageUnionReader::GcsFilter(reader) => {
                //GcsFilter
                let gcs_filter = reader.to_entity();
                info!("received GcsFilter from peer: {}, block_hash is {}, filter is {}.",
                    peer, 
                    gcs_filter.block_hash(),
                    gcs_filter.filter()
                );
                 
                //check and insert to store
                let block_num = self.store.get_header(gcs_filter.block_hash())?.number();
                let filter = gcs_filter.filter().unpack();
                let filter_hash = blake2b_256(filter.clone()).pack();
                
                /*
                get the stop num from filter_hashes, and get the index of filter_hash,
                then compare it to hash of this filter
                */
                let compare_result = 
                    match self.filter_hashes.read().unwrap().get(peer.clone()) {
                        Some(value) => {
                            let stop_num = self.store.get_header(value.stop_hash)?.number();
                            if filter_hash == value.filter_hashes[value.len() - stop_num + block_num -1] {
                                true
                            }
                        },
                        None => false,
                    }
                 
                //check if the lastest filter's blockhash is this one's parent
                //get the lastest filter's blockhash
                if compare_result == true {
                    let lastest_hash = self.store.get_lastest_hash()?;
                    let this_one_parent_hash = self.store.get_header(gcs_filter.block_hash())?.parent_hash();
                    if lastest_hash == this_one_parent_hash {
                        //insert into store
                        self.store.insert_gcsfilter(gcs_filter)
                          .expect("store gcs filter should be OK");
                        /*
                        //match script
                        let scripts = self
                            .get_scripts()?
                            .into_iter()
                            .map(|(script, _block_number)| script)
                            .collect::<Vec<_>>();
                        //store filtered block hash
                        let gcs_reader = build_gcs_filter_reader();
                        let Ok(ret) = gcs_reader
                            .match_any(&mut gcs_filter.filter(), &mut scripts.iter().map(|script| script.as_slice()))
                            .unwrap();
                        if ret == true {
                            if Ok(true) == pending_get_filtered_blocks.write().unwrap()
                                .insert(gcs_filter.block_hash()){
                                info!("insert block {} into pending_get_filtered_blocks.", gcs_filter.block_hash());
                                self.inner_sender.send(GcsMessage::GetFilterBlock(gcs_filter.block_hash())).unwrap();
                            }
                        }
                        */
                    }
                }
            }
            packed::GcsFilterMessageUnionReader::GcsFilterHashes(reader) => {
                let filter_hashes = reader.to_entity();
                info!("received GcsFilterHashes from peer: {}, stop_hash is {}, parent_hash is {}, filters len is {}.",
                    peer,
                    filter_hashes.stop_hash(),
                    filter_hashes.parent_hash(),
                    filter_hashes.filter_hashes().len()
                );
                //inert into filter_hashes 
                self.insert_hashes(peer, &filter_hashes);
            }
            packed::GcsFilterMessageUnionReader::GcsFilterCheckPoint(reader) => {
                //GcsFilterCheckPoint
                let filter_checkpoints = reader.to_entity();
                info!("received GcsFilterCheckPoint from peer: {}, stop_hash is {}, filter hashes len is {}.",
                    peer,
                    filter_checkpoints.stop_hash(),
                    filter_checkpoints.filter_hashes().len()
                );
                //insert checkpoints to check_points
                self.insert_checkpoints(peer, filter_checkpoints);                
            }
            _ => {
                // ignore
            }
        }
    }

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, _peer: PeerIndex) {
        self.peers.write().remove(_peer);
    }
    /*
    fn build_gcs_filter_reader() -> golomb_coded_set::GCSFilterReader {
        // use same value as bip158
        let p = 19;
        let m = 1.497_137 * f64::from(2u32.pow(p));
        golomb_coded_set::GCSFilterReader::new(0, 0, m as u64, p as u8)
    }
    */ 
}
