use super::{ChainStore, HeaderProviderWrapper, HeaderVerifier, Peers};
use crate::store::Store;
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info, warn};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{
    packed::{self, Byte32},
    prelude::*,
    core::{BlockNumber, HeaderView},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::string::String;
use ckb_util::RwLock;
use std::io::Cursor;
use crossbeam_channel::{unbounded,Sender,Receiver};

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const SEND_GET_HEADERS_TOKEN: u64 = 0;
const RECOVER_PEER_STATE_TOKEN: u64 = 1;
const MATCH_AND_GET_BLOCKS: u64 = 2;
const MAX_HEADERS_LEN: usize = 2_000;
const INIT_BLOCKS_IN_TRANSIT_PER_PEER: usize = 16;

pub enum SendMessage {
    GetHeaderMsg(usize),
    //ProcessHeaderMsg,
}

pub struct SyncProtocol<S> {
    store: ChainStore<S>,
    consensus: Consensus,
    sender:Sender<SendMessage>,
    receiver:Receiver<SendMessage>,
    peers: Peers,
    filter_reader: golomb_coded_set::GCSFilterReader,
    peer_headers:Arc<RwLock<HashMap<PeerIndex, Box<Vec<HeaderView>>>>>,
}

fn build_gcs_filter_reader() -> golomb_coded_set::GCSFilterReader {
    // use same value as bip158
    let p = 19;
    let m = 1.497_137 * f64::from(2u32.pow(p));
    golomb_coded_set::GCSFilterReader::new(0, 0, m as u64, p as u8)
}

impl<S> SyncProtocol<S> {
    pub fn new(store: ChainStore<S>, consensus: Consensus, peers:Peers) -> Self {
        let (sender, receiver) = unbounded();       
        let filter_reader = build_gcs_filter_reader();
        Self {
            store,
            consensus,
            sender,
            receiver,
            peers,
            filter_reader,
            peer_headers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn insert_peer_datas(&mut self, peer:PeerIndex, headers:Vec<HeaderView>){
        let mut _peer_hash = self.peer_headers.write();
        _peer_hash.insert(peer, Box::new(headers));
        //check the numbers of peers
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
        let peers_num = peers.len();
        let peer_datas_num = _peer_hash.len();
        if peer_datas_num >= peers_num {
            /*
            let _sender = self.sender.clone();
            //send ProcessHeaderMsg to channel
            _sender.send(SendMessage::ProcessHeaderMsg).unwrap();
            */
            let num = self.check_and_insert_headers();
            _peer_hash.clear();
            //send get header
            self.sender.send(SendMessage::GetHeaderMsg(num)).unwrap();
        }
    }
    
    //notify callback
    fn check_and_insert_headers(&mut self) -> usize {
        //insert headers
        let mut cur_iter = 0;
        let mut loop_flag = false;
        let mut cur_headers = HashMap::new();
        let header_provider = HeaderProviderWrapper { store: &self.store };
        let header_verifier = HeaderVerifier::new(&self.consensus, &header_provider);
        loop {
            let mut tmp_num = 0;
            let mut tmp_peer = Default::default();
            for (peer, _headers) in self.peer_headers.read().iter() {
                if self.peers.get_peer_state(peer.clone()) == true {
                    loop_flag = true;
                    if tmp_num < _headers.len() {
                        tmp_num = _headers.len();
                        tmp_peer = peer.clone();
                    }
                }
            }
            if loop_flag == false || cur_iter >= tmp_num {
                break;
            }

            cur_headers = self.peer_headers.read().get(&tmp_peer).unwrap();
            cur_headers = cur_headers[&(cur_iter..)].to_vec();
            for header in cur_headers.get(&tmp_peer).unwrap() {
                match header_verifier.verify(&header) {
                    Ok(_) => {
                        self
                            .store
                            .insert_header(header)
                            .expect("store should be OK");
                        cur_iter += 1;
                    }
                    Err(err) => {        
                        warn!("Peer {} sends us an invalid header: {:?}", tmp_peer, err);
                        /*
                        nc.ban_peer(
                            tmp_peer,
                            BAD_MESSAGE_BAN_TIME,
                            String::from("send us an invalid header"),
                        );
                        */
                        //delete datas with start_block_hash
                        self.peers.change_peer_state(tmp_peer, false);
                        break;
                    }   
                }
            }
        }
        let header = self
            .store
            .tip()
            .expect("store should be OK")
            .expect("tip stored");
        info!("new tip {:?}, {:?}", header.number(), header.hash());
        
        cur_iter as usize
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

impl<S: Store + Send + Sync> SyncProtocol<S> {
    fn send_get_headers(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        let locator_hashes = self.store.get_locator().expect("store should be OK");
        let message = packed::SyncMessage::new_builder()
            .set(
                packed::GetHeaders::new_builder()
                    .block_locator_hashes(locator_hashes.pack())
                    .hash_stop(Byte32::zero())
                    .build(),
            )
            .build();
        if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
            debug!("SyncProtocol send GetHeaders error: {:?}", err);
        }
    }
}


impl<S: Store + Send + Sync> CKBProtocolHandler for SyncProtocol<S> {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        nc.set_notify(Duration::from_secs(1), SEND_GET_HEADERS_TOKEN)
            .expect("set_notify for SEND_GET_HEADERS_TOKEN should be ok");
        nc.set_notify(Duration::from_secs(1), MATCH_AND_GET_BLOCKS)
            .expect("set_notify for MATCH_AND_GET_BLOCKS should be ok");
        nc.set_notify(Duration::from_secs(10 * 60), RECOVER_PEER_STATE_TOKEN)
            .expect("set_notify for RECOVER_PEER_STATE_TOKEN should be ok");
    }

    fn connected(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        _version: &str,
    ) {
        //insert peer to peers
        self.peers.insert_peer(peer, _version.to_string());
        if self.peers.peers_num() == 1 {
            //if the first peer, send a msg to srart the process
            let _sender = self.sender.clone();
            _sender.send(SendMessage::GetHeaderMsg(MAX_HEADERS_LEN)).unwrap();
        }
    }

    fn received(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex, data: Bytes) {
        let message = match packed::SyncMessage::from_slice(&data) {
            Ok(msg) => msg.to_enum(),
            _ => {
                info!("Peer {} sends us a malformed sync message", peer);
                nc.ban_peer(
                    peer,
                    BAD_MESSAGE_BAN_TIME,
                    String::from("send us a malformed sync message"),
                );
                return;
            }
        };

        match message.as_reader() {
            packed::SyncMessageUnionReader::SendHeaders(reader) => {
                let headers = reader
                    .headers()
                    .to_entity()
                    .into_iter()
                    .map(packed::Header::into_view)
                    .collect::<Vec<_>>();

                let len = headers.len();
                info!("received SendHeaders from peer: {}, len: {}", peer, len);
                if len > 0 {
                    let header_provider = HeaderProviderWrapper { store: &self.store };
                    let header_verifier = HeaderVerifier::new(&self.consensus, &header_provider);
                    
                    //check the first header
                    match header_verifier.verify(&headers[0]) {
                        Ok(_) => self.insert_peer_datas(peer.clone(), headers),
                        Err(err) => {
                            nc.ban_peer(
                                peer,
                                BAD_MESSAGE_BAN_TIME,
                                String::from("send us a malformed sync message"),
                            );
                            //set peer status
                            self.peers.change_peer_state(peer.clone(), false);
                        }
                    }
                }
            }
            //receive block
            packed::SyncMessageUnionReader::SendBlock(reader) => {
                let block = reader.block().to_entity().into_view();
                //insert block
                self.store.insert_filtered_block(block).expect("store block should be OK");
            }
            _ => {
                let msg = packed::SyncMessage::new_builder()
                    .set(packed::InIBD::new_builder().build())
                    .build();
                if let Err(err) = nc.send_message_to(peer, msg.as_bytes()) {
                    debug!("SyncProtocol send InIBD message error: {:?}", err);
                }
            }
        }
    }

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        self.peers.remove_peer(peer);
    }

    fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        match token {
            SEND_GET_HEADERS_TOKEN => {
                if let Ok(msg) = self.receiver.try_recv() {
                    match msg {
                        SendMessage::GetHeaderMsg(num) => {
                            let now = Instant::now();
                            let duration = Duration::from_secs(15);
                            let mut peers:Vec<PeerIndex> = Vec::new();
                            
                            for (_peer, state) in self.peers.get_peers().read().iter(){
                                if state.state() == true {
                                    if num < MAX_HEADERS_LEN {
                                        if now.duration_since(state.send_time()) < duration {
                                            self.sender.send(SendMessage::GetHeaderMsg(0)).unwrap();
                                            return;
                                        }
                                    }
                                    peers.push(*_peer);
                                }
                            }
                            peers
                                .into_iter()
                                .for_each(|peer| {
                                    //update send time
                                    self.peers.fresh_time(peer.clone());
                                    self.send_get_headers(Arc::clone(&nc), peer);
                                });
                        }
                        _ => {
                        //
                        }
                    }
                }
            }
            RECOVER_PEER_STATE_TOKEN => {
                for (key,_) in self.peers.get_peers().read().iter() {
                    self.peers.change_peer_state(*key, true);
                }
            }
            MATCH_AND_GET_BLOCKS => {
                let mut count :usize = 0;
                //get the lastest block num from Script store as the start block
                let start_block_num: BlockNumber = match self.store.get_lastest_block_num() {
                    Ok(Some(num)) => num + 1,
                    Err(_) => 0 as BlockNumber,
                };
                
                //get the lastest block from filters store
                let  stop_block_hash: packed::Byte32 = match self.store.get_lastest_hash() {
                    Ok(Some(hash)) => hash,
                    Err(_) => {
                        return;
                    }
                };

                let stop_block_num = self.store.get_header(stop_block_hash.clone())?.number();
                let mut filtered_last_block_num :BlockNumber = 0;
                
                //let block_hashes: Vec<Byte32>= Vec::new();
                //get all Scripts
                let mut scripts = self.store
                    .get_scripts()?
                    .into_iter()
                    .map(|(script, _block_number)| script)
                    .collect::<Vec<_>>();
                let mut block_hashes = Vec::new();
                for block_number in (start_block_num..=stop_block_num).take(MAX_HEADERS_LEN) {
                    let block_hash = self.store.get_block_hash(block_number.clone())?.expect("stored block hash");
                    let filter = self.store.get_gcsfilter(block_hash.clone())?.expect("stored gcs filter");
                    let mut input = Cursor::new(filter);
                    if true == self.filter_reader
                        .match_any(&mut input, &mut scripts.iter().map(|script| script.as_slice()))
                        .unwrap(){
                        if count < INIT_BLOCKS_IN_TRANSIT_PER_PEER {
                            count += 1;
                            block_hashes.push(block_hash);
                        }else{
                            filtered_last_block_num = block_number.clone();
                            break;
                        }
                    }
                    filtered_last_block_num = block_number.clone();
                }
                //new message
                let content = packed::GetBlocks::new_builder()
                    .block_hashes(block_hashes.pack())
                    .build();
                let message = packed::SyncMessage::new_builder().set(content).build();
                //send msg
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
                    }).collect();

                    peers
                        .into_iter()
                        .for_each(|peer| {
                            if let Ok(_) = nc.send_message_to(peer, message.as_bytes()) {
                                return;
                            }
                        });
                //update filtered blocknumber 
                if filtered_last_block_num > 0 {
                    self.store.update_scripts(filtered_last_block_num);
                }
            }
            _ => unreachable!(),
        }
    }
}
