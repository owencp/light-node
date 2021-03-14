use super::Peers;
//use crate::store::Store;
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info};

use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{
    core::{Cycle, TransactionView},
    packed::{
        Byte32, Byte32Vec, RelayMessage, RelayMessageUnionReader, RelayTransaction,
        RelayTransactionHashes, RelayTransactionVec, RelayTransactions,
    },
    prelude::*,
};
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const RELAY_TX_TOKEN: u64 = 0;
const RECEIVE_TX_FROM_RPC: u64 = 1;
const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const MAX_RELAY_TXS_NUM_PER_BATCH: usize = 32767;
const MAX_RELAY_TXS_BYTES_PER_BATCH: usize = 1024 * 1024;

pub enum ControlMessage {
    SendTx((TransactionView, Cycle)),
}

pub struct RelayProtocol {
    consensus: Consensus,
    receiver: Receiver<ControlMessage>,
    peers: Peers,
    pending_txs_map: Arc<RwLock<HashMap<Byte32, (TransactionView, Cycle)>>>,
    //pending_txs_record: Arc<RwLock<Vec<Byte32>>>,
}

impl RelayProtocol {
    pub fn new(consensus: Consensus, receiver: Receiver<ControlMessage>, peers: Peers) -> Self {
        Self {
            consensus,
            receiver,
            peers,
            pending_txs_map: Arc::new(RwLock::new(HashMap::new())),
            //pending_txs_record: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl RelayProtocol {
    //send tx_hash to all peers
    fn send_relay_tx_hash(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        hash: Byte32,
        peer: PeerIndex,
    ) {
        info!("send tx hash {:?} to full node {:?}", hash, peer);

        //build msg
        let content = RelayTransactionHashes::new_builder()
            .tx_hashes([hash.clone()].iter().map(ToOwned::to_owned).pack())
            .build();

        let msg = RelayMessage::new_builder().set(content).build().as_bytes();

        if let Err(err) = nc.send_message_to(peer.clone(), msg) {
            debug!("send tx hash {:?} to node {} error {:?}", hash, peer, err);
        }
    }

    fn process_send_relay_txs(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        hashes: Byte32Vec,
        peer: PeerIndex,
    ) {
        let txs: Vec<RelayTransaction> = {
            hashes
                .into_iter()
                .map(|hash| {
                    let (tx, cycles) = self
                        .pending_txs_map
                        .read()
                        .unwrap()
                        .get(&hash)
                        .cloned()
                        .expect("should store tx");

                    RelayTransaction::new_builder()
                        .cycles(cycles.pack())
                        .transaction(tx.data())
                        .build()
                })
                .collect()
        };
        if !txs.is_empty() {
            let mut relay_bytes = 0;
            let mut relay_txs = Vec::new();
            for tx in txs {
                if relay_bytes + tx.total_size() > MAX_RELAY_TXS_BYTES_PER_BATCH {
                    self.send_relay_transactions(
                        Arc::clone(&nc),
                        relay_txs.drain(..).collect(),
                        peer,
                    );
                    relay_bytes = tx.total_size();
                    relay_txs.push(tx);
                } else {
                    relay_bytes += tx.total_size();
                    relay_txs.push(tx);
                }
            }
            if !relay_txs.is_empty() {
                self.send_relay_transactions(Arc::clone(&nc), relay_txs, peer);
            }
        }
    }

    fn send_relay_transactions(
        &self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        txs: Vec<RelayTransaction>,
        peer: PeerIndex,
    ) {
        let message = RelayMessage::new_builder()
            .set(
                RelayTransactions::new_builder()
                    .transactions(RelayTransactionVec::new_builder().set(txs.clone()).build())
                    .build(),
            )
            .build();

        if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
            debug!("Send RelayTransactions error {:?}", err);
        }

        //update pending_txs_map

        txs.iter().for_each(move |relay_tx| {
            self.pending_txs_map
                .write()
                .unwrap()
                .remove(&relay_tx.transaction().into_view().hash());
        });
    }
}

impl CKBProtocolHandler for RelayProtocol {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        nc.set_notify(Duration::from_secs(1), RELAY_TX_TOKEN)
            .expect("set_notify for RELAY_TX_TOKEN should be ok");
        nc.set_notify(Duration::from_secs(1), RECEIVE_TX_FROM_RPC)
            .expect("set_notify for RECEIVE_TX_FROM_RPC should be ok");
    }

    fn connected(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        _version: &str,
    ) {
        //do nothing
    }

    fn received(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex, data: Bytes) {
        let message = match RelayMessage::from_slice(&data) {
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
            //full nodes message for GetRelayTransaction
            RelayMessageUnionReader::GetRelayTransactions(reader) => {
                let tx_hashes = reader.tx_hashes();
                let len = tx_hashes.len();
                info!(
                    "received GetRelayTransactions from peer: {}, len: {}",
                    peer, len
                );
                if len > MAX_RELAY_TXS_NUM_PER_BATCH {
                    return;
                }

                if len > 0 {
                    self.process_send_relay_txs(Arc::clone(&nc), tx_hashes.to_entity(), peer);
                }
            }
            _ => {
                //do nothing
            }
        }
    }

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        //do nothing
    }

    fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        match token {
            RELAY_TX_TOKEN => {
                if let Ok(msg) = self.receiver.try_recv() {
                    match msg {
                        ControlMessage::SendTx((tx_view, cycles)) => {
                            //store the tx to map
                            self.pending_txs_map
                                .write()
                                .unwrap()
                                .insert(tx_view.hash(), (tx_view.clone(), cycles));
                            /*
                                                        self.pending_txs_record
                                                            .write()
                                                            .unwrap()
                                                            .push(tx_view.hash());
                            */
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
                                self.send_relay_tx_hash(Arc::clone(&nc), tx_view.hash(), peer);
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }
}
