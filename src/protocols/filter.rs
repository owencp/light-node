use super::{ChainStore, Peers};
use crate::store::Store;

use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{packed, prelude::*, core::BlockNumber};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crossbeam_channel::{unbounded,Sender,Receiver};
use ckb_hash::blake2b_256;
use ckb_util::RwLock;

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const GCS_FILTER_MSG_TOKEN: u64 = 0;
//const SEND_GET_GCS_FILTER_HASHES_TOKEN: u64 = 1;
//const SEND_GET_GCS_CHECKPOINT_TOKEN: u64 = 2;
const CONTROL_RECEIVER_TOKEN: u64 = 1;

const MAX_PEER_SIZE: usize = 7;
const MAX_FILTER_RANGE_SIZE: usize = 200;
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
            stop_hash:cps.stop_hash(),
            check_points:Box::new(cps.filter_hashes()),
        }
    }
}

struct FilterHashes {
    flag:bool,
    hashes:packed::GcsFilterHashes
}

impl FilterHashes {
    pub fn new(hashes:packed::GcsFilterHashes, flag:bool) -> Self {
        Self {
            flag,
            hashes,
        }
    }
    
    pub fn stop_hash(&self) -> packed::Byte32 {
        self.hashes.stop_hash()
    }
    
    pub fn filter_hashes(&self) -> packed::Byte32Vec {
        self.hashes.filter_hashes()
    }
}
pub struct FilterProtocol<S> {
    store: ChainStore<S>,
    consensus: Consensus,
    control_receiver: Receiver<ControlMessage>,
    peers: Peers,
    filter_hashes: Arc<RwLock<HashMap<PeerIndex, FilterHashes>>>,
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
            peers,
            filter_hashes: Arc::new(RwLock::new(HashMap::new())),
            check_points: Arc::new(RwLock::new(HashMap::new())),
            inner_sender,
            inner_receiver,
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
/*
impl <S> FilterProtocol<S> {
    fn scale_stop_num(&self, start_num:BlockNumber, stop_num:BlockNumber) -> BlockNumber {
       if stop_num - start_num > MAX_FILTER_RANGE_SIZE as BlockNumber{
           start_block_num + MAX_FILTER_RANGE_SIZE as BlockNumber
       }else{
           stop_num
       }
    }

}
*/
impl<S: Store + Send + Sync> FilterProtocol<S> {
    //insert filter hashes
    fn insert_hashes(&mut self, peer:PeerIndex, _hashes: packed::GcsFilterHashes){
        let stop_hash = _hashes.stop_hash().clone();
        let parent_hash = _hashes.parent_hash().clone();
        let hash_len = _hashes.filter_hashes().len();

        let parent_num =  match self.store.get_header(parent_hash.clone()) {
            Ok(Some(header)) => header.number(),
            _=> 0
        };

        let stop_num = match self.store.get_header(stop_hash.clone()) {
            Ok(Some(header)) => header.number(),
            _=> 0
        };

        for _value in self.filter_hashes.read().values() {
            if _value.flag == true {
                return;
            }
        }
        //check parent_hash
        if stop_num >= parent_num && (stop_num - parent_num) as usize == hash_len  {
            
            self.filter_hashes.write().insert(peer.clone(), FilterHashes::new(_hashes, true));
            info!("insert hashes from peer: {}, stop_hash is {}, parent_hash is {}, hash len is {}.",
                    peer,
                    stop_hash,
                    parent_hash,
                    hash_len
            );
            //send msg
            self.inner_sender.send(GcsMessage::GetFilters(peer.clone())).unwrap();
        }
    }
    //insert checkpoints
    //check_points: Arc<RwLock<HashMap<PeerIndex,Box<packed::GcsFilterCheckPoint>>>>
    fn insert_checkpoints(&mut self, peer:PeerIndex, _check_points: packed::GcsFilterCheckPoint){
        let stop_hash = _check_points.stop_hash();
        if let Some(last_filter_hash) = _check_points.filter_hashes().get(_check_points.filter_hashes().len() - 1) {
            let mut _times = 1;

            for checkpoints in self.check_points.read().values() {
                //if the flag is set to true, return without process
                if checkpoints.flag == true {
                    return;
                }
                if checkpoints.stop_hash == stop_hash {
                    let len = checkpoints.check_points.len();
                    if checkpoints.check_points.get(len -1) == Some(last_filter_hash.clone()) {
                        _times += 1;
                    }
                }
            }

            let valid_peers = self.peers.valid_peer_num();
            let data_len = self.check_points.read().len();
            if _times == valid_peers || _times > 1 && _times > (data_len + 1) / 2 {
                self.check_points.write().clear();
                self.check_points.write().insert(peer.clone(), CheckPoints::new(_check_points, true));
                self.inner_sender.send(GcsMessage::GetFilterHashes(peer.clone())).unwrap();
            }else{
                self.check_points.write().insert(peer.clone(), CheckPoints::new(_check_points,false));
            }
        }
    }
}

impl<S: Store + Send + Sync> CKBProtocolHandler for FilterProtocol<S> {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        nc.set_notify(Duration::from_secs(1), GCS_FILTER_MSG_TOKEN)
            .expect("set_notify should be ok");
        nc.set_notify(Duration::from_secs(1), CONTROL_RECEIVER_TOKEN)
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
                            let stop_hash = self.store.tip()
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
                                .get_peers()
                                .read()
                                .iter()
                                .filter_map(|(peer, peer_state)| {
                                    if peer_state.state() == true {
                                        Some(*peer)
                                    } else {
                                        None
                                    } 
                                })
                                .collect();
                            info!("notify get checkpoint: stop_num is {}, interval is {}", stop_num, interval);
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
                            let start_block_hash = self.store.get_lastest_hash()
                                .expect("store should be OK")
                                .expect("filter hashes stored");
                            let mut start_block_num = self.store.get_header(start_block_hash)
                                .expect("stored should be OK")
                                .expect("get header stored")
                                .number();
                            //get stop hash from checkpoint
                            start_block_num = start_block_num + 1;
                            let stop_hash = self.check_points.read()
                                .get(&peer).unwrap().stop_hash.clone();
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
                                .get_peers()
                                .read()
                                .iter()
                                .filter_map(|(peer, peer_state)| {
                                    if peer_state.state() == true {
                                        Some(*peer)
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            info!("notify get filter hashes: start_num is {}, stop_hash is {}", start_block_num, stop_hash);
                            peers
                                .into_iter()
                                .for_each(|peer| {
                                    if let Err(err) = nc.send_message_to(peer, message.as_bytes()){
                                        debug!("GcsFilterProtocol send GetGcsFilterhash error: {:?}", err);
                                    }
                                });
                            //send GcsMessage::GetCheckPoint
                            self.check_points.write().clear();
                            //self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();
                             
                        } 
                        GcsMessage::GetFilters(peer) => {
                            //send get filters
                            //get the last filter info
                            let start_block_hash = self.store.get_lastest_hash()
                                .expect("stored should be OK")
                                .expect("stored lastest filter");

                            let mut start_block_num = self.store.get_header(start_block_hash)
                                .expect("stored should be OK")
                                .expect("store head").number();
                            //get stop hash from filterhashes
                            /* temp delete
                            let stop_hash = self
                                .filter_hashes.read()
                                .get(&peer).unwrap()
                                .stop_hash();
                            */
                            //temp, remove later
                            if let Some(stop_hash) = self.store.get_block_hash(200 as BlockNumber).expect("store hash"){
                            start_block_num = start_block_num + 1;
                            let stop_block_num = self.store.get_header(stop_hash.clone())
                                .expect("stored should be OK")
                                .expect("store head").number();
                            //stop_block_num = scale_stop_num(start_block_num, stop_block_num);
                            info!("notify get filter: start_num is {}, stop_num is {}", start_block_num, stop_block_num);
                            let message = packed::GcsFilterMessage::new_builder()
                                .set(
                                    packed::GetGcsFilters::new_builder()
                                        .start_number(start_block_num.pack())
                                        .stop_hash(stop_hash.clone())
                                        .build(),
                                ).build();
                            
                            if let Err(err) = nc.send_message_to(peer.clone(), message.as_bytes()){
                                debug!("GcsFilterProtocol send peer {} GetGcsFilters error: {:?}", err, peer);
                            }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            CONTROL_RECEIVER_TOKEN => {
                if let Ok(msg) = self.control_receiver.try_recv() {
                    match msg {
                        ControlMessage::SendTransaction(transaction) => {
                            /*
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
                            */
                            debug!("GcsFilterProtocol send SendTransaction TODO:");
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
        if (self.peers.is_peer_exsit(peer) == false){
            self.peers.insert_peer(peer, _version.to_string());
            if self.peers.peers_num() == 1 {
                self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();
            }
        }else {
            if self.peers.peers_num() == 1 {
                self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();
            }
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
                let block_hash = gcs_filter.block_hash();
                let block_num = self.store.get_header(block_hash.clone())
                    .expect("store should be OK")
                    .expect("store header")
                    .number();
                let filter:Bytes = gcs_filter.filter().unpack();
                let filter_hash = blake2b_256(filter).pack();
                
                //info!("received filter block num is {}, filter hash is {}", block_num, filter_hash ); 
                /*
                get the stop num from filter_hashes, and get the index of filter_hash,
                then compare it to hash of this filter
                */
                let mut stop_num:BlockNumber = 0;
                let compare_result = 
                    match self.filter_hashes.read().get(&peer) {
                        Some(value) => {
                            stop_num = self.store.get_header(value.stop_hash())
                                .expect("store should be OK")
                                .expect("store header")
                                .number();
                            let gap_len = (stop_num - block_num) as usize;
                            if filter_hash == value
                                .filter_hashes()
                                .get(value.filter_hashes().len() - gap_len -1).unwrap() {
                                true
                            }else {
                                false
                            }
                        },
                        _ => false,
                    };
                info!("the result is {}, block_num is {}, stop_num is {}", compare_result, block_num, stop_num); 
                if compare_result == true {
                    self.store.insert_gcsfilter(gcs_filter)
                        .expect("store gcs filter should be OK");
                    self.store.insert_record(block_num.clone(), block_hash)
                        .expect("store gcs record should be OK");
                }
                //
                if stop_num == block_num {
                    self.inner_sender.send(GcsMessage::GetCheckPoint).unwrap();   
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
                self.insert_hashes(peer, filter_hashes);
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
        self.peers.remove_peer(_peer);
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
