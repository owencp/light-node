use ckb_app_config::NetworkConfig;
use gcs_filter::protocols::{ChainStore, FilterProtocol, SyncProtocol};
use gcs_filter::service::RpcService;
use gcs_filter::store::{SledStore, Store};
use ckb_logger::info;
use ckb_logger_config::Config as LogConfig;
use ckb_network::{
    BlockingFlag, CKBProtocol, DefaultExitHandler, ExitHandler, NetworkService, NetworkState,
    NetworkProtocols,MAX_FRAME_LENGTH_GCSFILTER, MAX_FRAME_LENGTH_RELAY, MAX_FRAME_LENGTH_SYNC,
};
use clap::{App, Arg};
use crossbeam_channel::unbounded;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    pub logger: LogConfig,
    pub network: NetworkConfig,
}

fn main() {
    let matches = App::new("ckb fcs light node")
        .arg(
            Arg::with_name("listen_uri")
                .short("l")
                .help("Light node rpc http service listen address, default 127.0.0.1:8121")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dir")
                .short("d")
                .help("Sets the working dir to use")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("chain")
                .short("c")
                .help("Chain name, default mainnet")
                .takes_value(true),
        )
        .get_matches();

    let mut path = PathBuf::new();

    path.push(matches.value_of("dir").expect("required arg"));
    path.push("config.toml");
    let mut config: Config =
        toml::from_slice(&std::fs::read(path.clone()).expect("load config file"))
            .expect("deserialize config file");

    path.pop();
    path.push("run.log");
    config.logger.file = path.clone();
    let _logger_guard = ckb_logger_service::init(config.logger).unwrap();

    path.pop();
    path.push("db");
    config.network.path = path.clone();

    path.pop();
    path.push("private_keys_store");

    let rpc_listen_address = matches.value_of("listen_uri").unwrap_or("127.0.0.1:8121");

    let chain = matches.value_of("chain").unwrap_or("mainnet");

    init(
        config.network,
        rpc_listen_address,
        path.to_str().unwrap(),
        chain,
    );
}

fn init(
    config: ckb_app_config::NetworkConfig,
    rpc_listen_address: &str,
    private_keys_store_path: &str,
    chain: &str,
) {
    let sleddb = Arc::new(SledStore::new(config.path.to_str().unwrap()));
    info!("store statistics: {:?}", sleddb.statistics().unwrap());
    let store = ChainStore { store: sleddb };

    let resource = ckb_resource::Resource::bundled(format!("specs/{}.toml", chain));
    let spec = ckb_chain_spec::ChainSpec::load_from(&resource).expect("load spec by name");
    if store.tip().expect("store should be OK").is_none() {
        let genesis = spec.build_genesis().expect("build genesis");
        store.init(genesis.header()).expect("store should be OK");
    }
    let consensus = spec.build_consensus().expect("build consensus");

    let (sender, receiver) = unbounded();

    let _server = RpcService::new(
        store.clone(),
        sender,
        rpc_listen_address,
        private_keys_store_path,
        &consensus,
    )
    .start();

    let genesis_hash = store
        .get_block_hash(0)
        .expect("store should be OK")
        .unwrap();
    info!("chain genesis hash: {:#x}", genesis_hash);

    let network_state =
        Arc::new(NetworkState::from_config(config).expect("Init network state failed"));
    let exit_handler = DefaultExitHandler::default();
    let required_protocol_ids = vec![
        NetworkProtocol::SYNC.into(),
        NetworkProtocol::RELAY.into(),
        NetworkProtocol::GCSFILTER.into(),
    ];

    let mut blocking_recv_flag = BlockingFlag::default();
    blocking_recv_flag.disable_connected();
    blocking_recv_flag.disable_disconnected();
    blocking_recv_flag.disable_notify();

    let protocols = vec![
        CKBProtocol::new(
            "syn".to_string(),
            NetworkProtocol::SYNC.into(),
            &["1".to_string()][..],
            MAX_FRAME_LENGTH_SYNC,
            Box::new(synchronizer.clone()),
            Arc::clone(&network_state),
            blocking_recv_flag,
        ),
        CKBProtocol::new(
            "rel".to_string(),
            NetworkProtocol::RELAY.into(),
            &["1".to_string()][..],
            MAX_FRAME_LENGTH_RELAY,
            Box::new(relayer),
            Arc::clone(&network_state),
            blocking_recv_flag,
        ),
        CKBProtocol::new(
            "gcs".to_string(),
            NetworkProtocol::GCSFILTER.into(),
            &["1".to_string()][..],
            MAX_FRAME_LENGTH_GCSFILTER,
            Box::new(gcs_filter),
            Arc::clone(&network_state),
            blocking_recv_flag,
        ),
    ];

    let _network_controller = NetworkService::new(
        Arc::clone(&network_state),
        protocols,
        required_protocol_ids,
        consensus.identify_name(),
        "ckb-light-node-demo".to_string(),
        exit_handler.clone(),
    )
    .start(Some("NetworkService"))
    .expect("Start network service failed");

    let exit_handler_clone = exit_handler.clone();
    ctrlc::set_handler(move || {
        exit_handler_clone.notify_exit();
    })
    .expect("Error setting Ctrl-C handler");
    exit_handler.wait_for_exit();
}
