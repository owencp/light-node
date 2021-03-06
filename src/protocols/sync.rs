use super::{ChainStore, HeaderProviderWrapper, HeaderVerifier, Peers};
use crate::store::Store;
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info, warn};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{
    core::{BlockNumber, HeaderView},
    packed::{self, Byte32},
    prelude::*,
};
use ckb_util::RwLock;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::io::Cursor;
use std::string::String;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    sender: Sender<SendMessage>,
    receiver: Receiver<SendMessage>,
    peers: Peers,
    filter_reader: golomb_coded_set::GCSFilterReader,
    peer_headers: Arc<RwLock<HashMap<PeerIndex, Box<Vec<HeaderView>>>>>,
}

fn build_gcs_filter_reader() -> golomb_coded_set::GCSFilterReader {
    // use same value as bip158
    let p = 19;
    let m = 1.497_137 * f64::from(2u32.pow(p));
    golomb_coded_set::GCSFilterReader::new(0, 0, m as u64, p as u8)
}

impl<S> SyncProtocol<S> {
    pub fn new(store: ChainStore<S>, consensus: Consensus, peers: Peers) -> Self {
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

    fn insert_peer_datas(&mut self, peer: PeerIndex, headers: Vec<HeaderView>) {
        self.peer_headers.write().insert(peer, Box::new(headers));
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
        let peer_datas_num = self.peer_headers.read().len();
        if peer_datas_num >= peers_num {
            let num = self.check_and_insert_headers();
            self.peer_headers.write().clear();
            //send get header
            self.sender.send(SendMessage::GetHeaderMsg(num)).unwrap();
        }
    }

    //notify callback
    fn check_and_insert_headers(&mut self) -> usize {
        //insert headers
        let mut cur_iter = 0;
        let mut loop_flag = false;
        let mut cur_headers = Vec::new();
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

            cur_headers = self.peer_headers.read().get(&tmp_peer).unwrap().to_vec();
            cur_headers = cur_headers[cur_iter..].to_vec();
            for header in cur_headers {
                match header_verifier.verify(&header) {
                    Ok(_) => {
                        self.store
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
        if self.peers.is_peer_exsit(peer) == false {
            self.peers.insert_peer(peer, _version.to_string());
            if self.peers.peers_num() == 1 {
                //if the first peer, send a msg to srart the process
                self.sender
                    .send(SendMessage::GetHeaderMsg(MAX_HEADERS_LEN))
                    .unwrap();
            }
        } else {
            if self.peers.peers_num() == 1 {
                //if the first peer, send a msg to srart the process
                self.sender
                    .send(SendMessage::GetHeaderMsg(MAX_HEADERS_LEN))
                    .unwrap();
            }
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
                info!(
                    "received filtered block: num is {}, hash is {:?}",
                    block.number(),
                    block.hash()
                );
                self.store
                    .insert_filtered_block(block)
                    .expect("store block should be OK");
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
                            let mut peers: Vec<PeerIndex> = Vec::new();

                            for (_peer, state) in self.peers.get_peers().read().iter() {
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
                            peers.into_iter().for_each(|peer| {
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
                self.peers.refresh_peer_state();
            }
            MATCH_AND_GET_BLOCKS => {
                let scripts = self
                    .store
                    .get_scripts()
                    .expect("store script")
                    .into_iter()
                    .map(|(_script, _anyone, block_number)| block_number)
                    .collect::<Vec<_>>();
                if scripts.len() == 0 {
                    return;
                }

                let start_block_num = scripts[0] + 1;
                let mut count: usize = 0;

                //get the lastest block from filters store
                let stop_block_hash: packed::Byte32 = match self.store.get_lastest_hash() {
                    Ok(Some(hash)) => hash,
                    _ => packed::Byte32::default(),
                };

                let stop_block_num: BlockNumber =
                    match self.store.get_header(stop_block_hash.clone()) {
                        Ok(Some(header)) => header.number(),
                        _ => 0 as BlockNumber,
                    };
                if start_block_num >= stop_block_num {
                    return;
                }
                let mut filtered_last_block_num: BlockNumber = 0;

                //get all Scripts: normal script, anyone_can_pay script
                let (normal_scripts, anyone_can_pay_scripts): (
                    Vec<packed::Byte32>,
                    Vec<packed::Byte32>,
                ) = self
                    .store
                    .get_scripts()
                    .expect("store script")
                    .into_iter()
                    .map(|(script, anyone_can_pay_script, _block_number)| {
                        (
                            script.calc_script_hash(),
                            anyone_can_pay_script.calc_script_hash(),
                        )
                    })
                    .unzip();
                let scripts = normal_scripts
                    .iter()
                    .chain(anyone_can_pay_scripts.iter())
                    .collect::<Vec<_>>();
                let mut block_hashes = Vec::new();
                for block_number in (start_block_num..=stop_block_num).take(MAX_HEADERS_LEN) {
                    let block_hash = self
                        .store
                        .get_block_hash(block_number.clone())
                        .expect("store should be ok")
                        .expect("stored block hash");
                    let filter = self
                        .store
                        .get_gcsfilter(block_hash.clone())
                        .expect("store should be ok")
                        .expect("stored gcs filter")
                        .raw_data()
                        .to_vec();

                    let mut input = Cursor::new(filter);
                    let result = self.filter_reader.match_any(
                        &mut input,
                        &mut scripts.iter().map(|script| script.as_slice()),
                    );
                    match result {
                        Ok(true) => {
                            if count < INIT_BLOCKS_IN_TRANSIT_PER_PEER {
                                count += 1;
                                block_hashes.push(block_hash);
                            } else {
                                filtered_last_block_num = block_number.clone();
                                break;
                            }
                        }
                        _ => {
                            //
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
                    })
                    .collect();

                peers.into_iter().for_each(|peer| {
                    if let Ok(_) = nc.send_message_to(peer, message.as_bytes()) {
                        //update filtered blocknumber
                        if filtered_last_block_num > 0 {
                            self.store
                                .update_scripts(filtered_last_block_num)
                                .expect("update scripts should be OK");
                        }
                        return;
                    }
                });
            }
            _ => unreachable!(),
        }
    }
}
