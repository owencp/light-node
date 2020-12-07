use ckb_network::PeerIndex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::string::String;
use std::time::{Duration, Instant};


#[derive(Clone)]
struct PeerState{
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
}
#[derive(Clone)]
pub struct Peers{
   _peers: Arc<RwLock<HashMap<PeerIndex, PeerState>>>
}


impl Peers{
    pub fn new()->Slef {
        Self {
            _peers:Arc::new(RWLock::new(HashMap::new()))
        }
    }

    fn insert_peer(&mut self, peer:PeerIndex, ver:String) {
        let state = PeerState::new(ver,true);
        self._peers.write().unwrap().insert(peer, state);
    }

    fn remove_peer(&mut self, peer:PeerIndex){
        self._peers.write().unwrap().remove(peer);
    }
    
    fn get_peers(&slef) -> Arc<RwLock<HashMap<PeerIndex, PeerState>>> {
        self._peers
    }

    fn change_peer_state(&mut self, peer:PeerIndex, state:bool) {
        let peer_state = PeerState::new(ver,state);
        self._peers.write().unwrap().insert(peer, peer_state);
    }
    
    fn get_version(&mut self, peer:PeerIndex) -> String {
        let val = self._peers.read().unwrap();
        if val.contains_key(peer) == true {
            val.get(&peer).unwrap().version.to_string()
        }else{
           "0.0.0".to_string()
        }
    }

    fn fresh_time(&mut self, peer:PeerIndex) {
        self._peers.write().unwrap()
            .get_mut(&peer).unwrap()
            .send_time = Instant::now();
    }

    fn get_peer_state(&mut self, peer:PeerIndex) -> bool {
        let val = self._peers.read().unwrap();
        if val.contains_key(peer) == true {
            val.get(&peer).unwrap().state
        }else{
           false
        }
    }
    
    fn peers_num(&self) ->u32 {
        self._peers.read().unwrap().len() as u32
    }

    fn valid_peer_num(&self) -> u32 {
        let num:u32 = 0;
        for _peer_state in self.read().unwrap().values() {
            if _peer_state.state == true {
                num += 1;
            }
        }
        num
    }
}

