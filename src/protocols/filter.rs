use super::{ChainStore, HeaderProviderWrapper, HeaderVerifier};
use crate::store::Store;
use crate::{types::SyncShared, BAD_MESSAGE_BAN_TIME};
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{packed, prelude::*};
use crossbeam_channel::Receiver;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};


const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const SEND_GET_GCS_FILTER_TOKEN: u64 = 0;
const SEND_GET_GCS_FILTER_HASHES_TOKEN: u64 = 1;
const SEND_GET_GCS_CHECKPOINT_TOKEN: u64 = 2;
const CONTROL_RECEIVER_TOKEN: u64 = 3;

const MAX_PEER_SIZE: usize = 7;
const MAX_FILTER_RANGE_SIZE: usize = 2000;
const MIN_CHECK_POINT_INTERVAL: u32 = 200_000;

#[derive(Debug,Clone)]
truct peerInfo{
    timein          :Instant,
    version         :String,
}

impl peerInfo{
    
    pub fn new(
        timein  :Instant,
        version :String,
    ) -> Self {
        Self {
            timein,
            version,
        }
    }
}

struct peerTimes{
   peer:PeerIndex,
   times:usize
}

impl peerTimes{
    fn new(
        peer:PeerIndex,
        times:usize
    )->Self {
        Self{peer,times}
    }
}

//filter hashes received from peers
struct filterHashes{
    parent_hash:packed::Byte32,
    hashes:Byte32Vec,
}

impl filterHashes{

    pub fn new(
        parent_hash:packed::Byte32,
        hashes:Byte32Vec,
    ) -> Self {
        Self {
            parent_hash,
            hashes,
        }
    }
}


pub struct FilterProtocol<S> {
    store: ChainStore<S>,
    consensus: Consensus,
    control_receiver: Receiver<ControlMessage>,
    pending_get_filtered_blocks: HashSet<packed::Byte32>,
    peers: HashMap<PeerIndex, peerInfo>,
    filter_hashes: HashMap<packed::Byte32, HashMap<PeerIndex, Box<filterHashes>>>,
    check_points: HashMap<packed::Byte32, HashMap<PeerIndex,Box<Byte32Vec>>>,
}

impl<S> FilterProtocol<S> {
    pub fn new(
        store: ChainStore<S>,
        consensus: Consensus,
        control_receiver: Receiver<ControlMessage>,
    ) -> Self {
        Self {
            store,
            consensus,
            control_receiver,
            pending_get_filtered_blocks: HashSet::new(),
            peers: HashMap::default(),
            filter_hashes: HashMap::new(),
            check_points: HashMap::new(),
        }
    }
    
    //insert filter hashes
    fn insert_hashes(&mut self, peer:PeerIndex, _hashes: packed::GcsFilterHashes){
        let mut hashvalues = HashMap::new();
        let stop_hash = _hashes.stop_hash().clone();
        let parent_hash = _hashes.parent_hash().clone();
        
        hashvalues.insert(peer, Box::new(filterHashes::new(parent_hash, _hashes.filter_hashes().clone())));

        self.filter_hashes.insert(stop_hash, hashvalues);
        info!("insert hashes from peer: {}, stop_hash is {}, parent_hash is {}, filters len is {}.",
                    peer,
                    _hashes.stop_hash(),
                    _hashes.parent_hash(),
                    _hashes.filter_hashes().len()
             );
    }
    
    //remove peer
    fn delete_peer(&mut self, peer:PeerIndex){
        let mut is_delete:bool = false;
        for val in self.filter_hashes.values_mut() {
            let is_exist = val.contains_key(peer);
            if is_exsit == true {
                is_delete = true;
                val.remove(peer);
                info!("remove peer from filter_hashes: {}.", PeerIndex);   
            }
        }
        //remove peerIndex
        if is_delete == true {
            self.peers.remove(peer);
        }
    }

    //check data 
    fn check_and_delete_peer_data(&self, stop_hash:packed::Byte32) -> bool {
        let peer_count = self.peers.len();
        let peer_data_count = self.filter_hashes.get(stop_hash).unwrap().len();

        if peer_count == peer_data_count {
            if peer_count > 1 {
                //compare parent_hash and stop_hash
                let hashes = &self.filter_hashes.get(&stop_hash).unwrap();
                let mut parent_hash_judge:HashMap<packed::Byte32, Box<Vec<PeerIndex>>> = HashMap::new();
                for (peer, hashes) in hashes.iter(){
                    if let Some(times) = parent_hash_judge.get_mut(&hashes.parent_hash) {
                        times.push(peer);
                    }
                    else{
                        parent_hash_judge.insert(&hashes.parent_hash, Box::new(vec![peer]));
                    }
                }
                //check parent_hash_judge.len()
                if parent_hash_judge.len() > 1 {
                    let ok_count:usize = peer_count/2;
                    for (_,value) in &parent_hash_judge {
                        if value.len() <= ok_count {
                            value.iter().for_each(move |x| delete_peer(*x))
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }
}

pub enum ControlMessage {
    SendTransaction(packed::Transaction),
}

pub enum GetGcsFilterMessage {
    GetGcsFilters(packed::Uint64, packed::Byte32),
    GetGcsFilterHashes(packed::Uint64, packed::Byte32),
    GetGcsFilterCheckPoint(packed::Byte32, packed::Uint32),
}

impl<S: Store + Send + Sync> CKBProtocolHandler for FilterProtocol<S> {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        //TODO::    (Duration::from_millis(10) ???
        nc.set_notify(Duration::from_millis(10), SEND_GET_GCS_FILTER_TOKEN)
            .expect("set_notify should be ok");
        nc.set_notify(Duration::from_millis(10), SEND_GET_GCS_FILTER_HASHES_TOKEN)
            .expect("set_notify should be ok");
        nc.set_notify(Duration::from_secs(60), SEND_GET_GCS_CHECKPOINT_TOKEN)
            .expect("set_notify should be ok");
        nc.set_notify(Duration::from_millis(100), CONTROL_RECEIVER_TOKEN)
            .expect("set_notify should be ok");
    }

    fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        match token {
            SEND_GET_GCS_FILTER_TOKEN => {
                //paras: start_number, stop_hash
                //construct message
                let message = packed::GcsFilterMessage::new_builder()
                              .set(
                                  packed::GetGcsFilters::new_builder()
                                  .start_number(start_block.pack())
                                  .stop_hash(stop_hash.clone())
                                  .build(),
                                  )
                                  .build();
                //get peers
                let peers: Vec<PeerIndex> = self
                    .peers
                    .iter()
                    .map(|(peer, _) | Some(*peer))
                    .collect();
                //send message to each peer
                peers
                    .into_iter()
                    .for_each(|peer| {
                                         if let Err(err) = nc.send_message_to(peer, message.as_bytes()){
                                             debug!("GcsFilterProtocol send GetGcsFilters error: {:?}", err);
                                         }
                                     }
                             );
            }
            SEND_GET_GCS_FILTER_HASHES_TOKEN => {
                let message = packed::GcsFilterMessage::new_builder()
                              .set(
                                  packed::GetGcsFilterHashes::new_builder()
                                  .start_number(start_block.pack())
                                  .stop_hash(stop_hash.clone())
                                  .build(),
                                  )
                                  .build();
                let peers: Vec<PeerIndex> = self
                    .peers
                    .iter()
                    .map(|(peer, _)|Some(*peer))
                    .collect();

                peers
                    .into_iter()
                    .for_each(|peer| {
                                     if let Err(err) = nc.send_message_to(peer, message.as_bytes()){
                                         debug!("GcsFilterProtocol send GetGcsFilters error: {:?}", err);
                                     }
                             });
            }
            SEND_GET_GCS_CHECKPOINT_TOKEN => {
                //paras: stop_hash, interval
                //duration 设置为多少合适？？ 200000 * block_interval  * N ??
                let duration = Duration::from_secs();
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
                    .iter()
                    .filter_map(|(peer, last_send_at)| {
                        if now.duration_since(*last_send_at) > duration {
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
        //支持多peer
        if self.peers.keys().len() < MAX_PEER_SIZE {
            self.peers.insert(peer, peerInfo::new(Instant::now(), _version.to_string()));
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
                    if Ok(true) == pending_get_filtered_blocks.insert(gcs_filter.block_hash()){
                        info!("insert block {} into pending_get_filtered_blocks.", gcs_filter.block_hash());
                    }
                }
                
                self.store.insert_gcsfilter(gcs_filter)
                          .expect("store should be OK");
            }
            packed::GcsFilterMessageUnionReader::GcsFilterHashes(reader) => {
                let filter_hashes = reader.to_entity();
                info!("received GcsFilterHashes from peer: {}, stop_hash is {}, parent_hash is {}, filters len is {}.",
                    peer,
                    filter_hashes.stop_hash(),
                    filter_hashes.parent_hash(),
                    filter_hashes.filter_hashes().len()
                );
                //inert into tmp 
                self.insert_hashes(peer, &filter_hashes);
                if true == self.check_and_delete_peer_data(filter_hashes.stop_hash()){
                    
                }
                self.store.insert_filterhashes(filter_hashes)
                          .expect("store should be OK");
            }
            packed::GcsFilterMessageUnionReader::GcsFilterCheckPoint(reader) => {
                //GcsFilterCheckPoint
                let filter_checkpoints = reader.to_entity();
                info!("received GcsFilterCheckPoint from peer: {}, stop_hash is {}, filter hashes len is {}.",
                    peer,
                    filter_checkpoints.stop_hash(),
                    filter_checkpoints.filter_hashes().len()
                );
                //from 0 to head_number
                self.store.insert_gcs_checkpoints(filter_checkpoints)
                          .expect("store should be OK");
                
            }
            _ => {
                // ignore
            }
        }
    }

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, _peer: PeerIndex) {
        self.peers.remove(_peer);
    }

    fn build_gcs_filter_reader() -> golomb_coded_set::GCSFilterReader {
        // use same value as bip158
        let p = 19;
        let m = 1.497_137 * f64::from(2u32.pow(p));
        golomb_coded_set::GCSFilterReader::new(0, 0, m as u64, p as u8)
    }  
}
