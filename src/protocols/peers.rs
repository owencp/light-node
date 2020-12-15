use ckb_network::PeerIndex;
use std::collections::HashMap;
use std::sync::Arc;
use ckb_util::RwLock;
use std::string::String;
use std::time::{Duration, Instant};


#[derive(Clone)]
pub struct PeerState{
    version:String,
    send_time:Instant,
    state:bool,
}
impl PeerState {
    pub fn new(version:String, state:bool) -> Self {
        Self {
            version,
            send_time:Instant::now(),
            state
        }
    }
    
    pub fn state(&self) -> bool {
        self.state
    }
    
    pub fn send_time(&self) -> Instant {
        self.send_time
    }
}
#[derive(Clone)]
pub struct Peers{
   _peers: Arc<RwLock<HashMap<PeerIndex, PeerState>>>
}


impl Peers{
    pub fn new()->Self {
        Self {
            _peers:Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub fn insert_peer(&mut self, peer:PeerIndex, ver:String) {
        let state = PeerState::new(ver,true);
        self._peers.write().insert(peer, state);
    }

    pub fn remove_peer(&mut self, peer:PeerIndex){
        self._peers.write().remove(&peer);
    }
    
    pub fn get_peers(&self) -> Arc<RwLock<HashMap<PeerIndex, PeerState>>> {
        self._peers.clone()
    }

    pub fn change_peer_state(&mut self, peer:PeerIndex, state:bool) {
        //let peer_state = PeerState::new(ver,state);
        
        self._peers.write()
            .get_mut(&peer)
            .unwrap().state = state;
    }
    pub fn get_send_time(&self, peer:PeerIndex) -> Instant {
        match self._peers.read().get(&peer) {
            Some(state) => state.send_time,
            None => Instant::now(),
        }
    } 
    fn get_version(&mut self, peer:PeerIndex) -> String {
        let val = self._peers.read();
        if val.contains_key(&peer) == true {
            val.get(&peer).unwrap().version.to_string()
        }else{
           "0.0.0".to_string()
        }
    }

    pub fn fresh_time(&mut self, peer:PeerIndex) {
        self._peers.write()
            .get_mut(&peer).unwrap()
            .send_time = Instant::now();
    }

    pub fn get_peer_state(&mut self, peer:PeerIndex) -> bool {
        let val = self._peers.read();
        if val.contains_key(&peer) == true {
            val.get(&peer).unwrap().state
        }else{
           false
        }
    }
    
    pub fn peers_num(&self) ->usize {
        self._peers.read().len() as usize
    }

    pub fn valid_peer_num(&self) -> usize {
        let mut num:usize = 0;
        for _peer_state in self._peers.read().values() {
            if _peer_state.state == true {
                num = num + 1;
            }
        }
        num
    }
}

