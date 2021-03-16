pub mod chain_store;
pub mod filter;
pub mod header_verifier;
pub mod peers;
pub mod relay;
pub mod sync;

pub use self::chain_store::{ChainStore, HeaderProviderWrapper};
pub use self::filter::FilterProtocol;
pub use self::header_verifier::{HeaderProvider, HeaderVerifier};
pub use self::peers::Peers;
pub use self::relay::{ControlMessage, RelayProtocol};
pub use self::sync::SyncProtocol;

use ckb_script::TransactionScriptsVerifier;
use ckb_types::{
    bytes::Bytes,
    core::{
        cell::{CellMeta, CellMetaBuilder, ResolvedTransaction},
        Cycle, DepType, HeaderView, ScriptHashType, TransactionBuilder, TransactionView,
    },
    packed::{
        self, Byte32, CellDep, CellInput, CellOutput, OutPoint, OutPointVec, Script, WitnessArgs,
        WitnessArgsBuilder,
    },
    prelude::*,
};

use ckb_error::Error;

use ckb_traits::{CellDataProvider, HeaderProvider as ChainHeaderProvider};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Default, Clone)]
pub struct GcsDataLoader {
    pub headers: Arc<RwLock<HashMap<Byte32, HeaderView>>>,
    //key:OutPoint; value:(CellOutput, output_data)
    pub cells: Arc<RwLock<HashMap<OutPoint, (CellOutput, Bytes)>>>,
}

impl GcsDataLoader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_cell(&self, out_point: &OutPoint, output: &CellOutput, output_data: &Bytes) {
        self.cells
            .write()
            .unwrap()
            .insert(out_point.clone(), (output.clone(), output_data.clone()));
    }

    pub fn delete_cell(&self, out_point: &OutPoint) {
        self.cells.write().unwrap().remove(out_point);
    }

    pub fn insert_dep_cell(&self, out_point: &OutPoint, output: &CellOutput, output_data: &Bytes) {
        self.cells
            .write()
            .unwrap()
            .insert(out_point.clone(), (output.clone(), output_data.clone()));
    }
}

impl CellDataProvider for GcsDataLoader {
    fn load_cell_data(&self, cell: &CellMeta) -> Option<(Bytes, Byte32)> {
        cell.mem_cell_data
            .as_ref()
            .map(ToOwned::to_owned)
            .or_else(|| self.get_cell_data(&cell.out_point))
    }
    /*
    fn load_cell_data_hash(&self, cell: &CellMeta) -> Option<Byte32> {
        cell.mem_cell_data_hash
            .as_ref()
            .map(ToOwned::to_owned)
            .or_else(|| self.get_cell_data_hash(&cell.out_point))
    }
    */
    /// fetch cell_data from storage
    fn get_cell_data(&self, out_point: &OutPoint) -> Option<(Bytes, Byte32)> {
        let (_, data) = self.cells.read().unwrap().get(out_point).unwrap().clone();
        let data_hash = packed::CellOutput::calc_data_hash(&data.clone());
        Some((data.clone(), data_hash))
    }
    /*
    /// fetch cell_data_hash from storage
    fn get_cell_data_hash(&self, out_point: &OutPoint) -> Option<Byte32> {
        let let (_, data) = self.cells.read().unwrap().get(out_point).unwrap();
        let data_hash = packed::CellOutput::calc_data_hash(&data);
        data_hash
    }
    */
}

impl ChainHeaderProvider for GcsDataLoader {
    // load header
    fn get_header(&self, block_hash: &Byte32) -> Option<HeaderView> {
        None
    }
}

fn parse_dep_group_data(slice: &[u8]) -> Result<OutPointVec, String> {
    if slice.is_empty() {
        Err("data is empty".to_owned())
    } else {
        match OutPointVec::from_slice(slice) {
            Ok(v) => {
                if v.is_empty() {
                    Err("dep group is empty".to_owned())
                } else {
                    Ok(v)
                }
            }
            Err(err) => Err(err.to_string()),
        }
    }
}

fn build_cellmeta_from_outpoint(data_loader: GcsDataLoader, outpoint: OutPoint) -> CellMeta {
    let (dep_output, dep_data) = data_loader
        .cells
        .read()
        .unwrap()
        .get(&outpoint.clone())
        .unwrap()
        .clone();
    CellMetaBuilder::from_cell_output(dep_output.to_owned(), dep_data.to_owned())
        .out_point(outpoint)
        .build()
}

pub fn build_resolved_tx(data_loader: GcsDataLoader, tx: TransactionView) -> ResolvedTransaction {
    let mut resolved_cell_deps = Vec::default();

    for dep in tx.cell_deps() {
        let deps_out_point = dep.clone();
        let cell_meta =
            build_cellmeta_from_outpoint(data_loader.clone(), deps_out_point.out_point());
        if dep.dep_type() == DepType::DepGroup.into() {
            //build CellMeta
            let data = cell_meta
                .mem_cell_data
                .clone()
                .expect("Load cell meta must with data");
            let sub_out_points = parse_dep_group_data(&data.0).map_err(|_| "error").unwrap();
            for sub_out_point in sub_out_points.into_iter() {
                let sub_cell_meta =
                    build_cellmeta_from_outpoint(data_loader.clone(), sub_out_point);
                resolved_cell_deps.push(sub_cell_meta)
            }
        } else {
            resolved_cell_deps.push(cell_meta);
        }
    }

    let mut resolved_inputs = Vec::new();
    for index in 0..tx.inputs().len() {
        let previous_out_point = tx.inputs().get(index).unwrap().previous_output();
        let (input_output, input_data) = data_loader
            .cells
            .read()
            .unwrap()
            .get(&previous_out_point)
            .unwrap()
            .clone();
        resolved_inputs.push(
            CellMetaBuilder::from_cell_output(input_output.to_owned(), input_data.to_owned())
                .out_point(previous_out_point)
                .build(),
        );
    }

    ResolvedTransaction {
        transaction: tx.clone(),
        resolved_cell_deps,
        resolved_inputs,
        resolved_dep_groups: vec![],
    }
}

pub fn verify_and_get_cycles<'a, DL: CellDataProvider + ChainHeaderProvider>(
    rtx: &'a ResolvedTransaction,
    data_loader: &'a DL,
) -> Result<Cycle, Error> {
    let verifier = TransactionScriptsVerifier::new(rtx, data_loader);
    //max_cycles ???
    verifier.verify(10000000000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_chain_spec::{
        consensus::Consensus, OUTPUT_INDEX_SECP256K1_BLAKE160_SIGHASH_ALL,
        OUTPUT_INDEX_SECP256K1_DATA,
    };
    use ckb_test_chain_utils::{
        always_success_cell, ckb_testnet_consensus, load_input_data_hash_cell,
    };
    use ckb_types::{
        bytes::Bytes,
        core::{
            capacity_bytes, Capacity, EpochNumberWithFraction, ScriptHashType, TransactionBuilder,
            TransactionView,
        },
        h256, H256,
        packed::{Byte32, CellInput, CellOutput, CellOutputBuilder, OutPoint, Script},
    };
    const TX_FEE: Capacity = capacity_bytes!(10);
    const max_cycles: u64 = 10000000000000;

    pub fn secp256k1_blake160_sighash_cell(consensus: Consensus) -> (packed::CellOutput, Bytes) {
        let genesis_block = consensus.genesis_block();
        let tx = genesis_block.transactions()[0].clone();
        let (cell_output, data) = tx
            .output_with_data(OUTPUT_INDEX_SECP256K1_BLAKE160_SIGHASH_ALL as usize)
            .unwrap();
        (cell_output, data)
    }

    pub fn secp256k1_data_cell(consensus: Consensus) -> (packed::CellOutput, Bytes) {
        let genesis_block = consensus.genesis_block();
        let tx = genesis_block.transactions()[0].clone();
        let (cell_output, data) = tx
            .output_with_data(OUTPUT_INDEX_SECP256K1_DATA as usize)
            .unwrap();
        (cell_output, data)
    }

    pub fn secp_dep_cell(consensus: Consensus) -> (packed::CellOutput, Bytes) {
        let genesis_block = consensus.genesis_block();
        let tx = genesis_block.transactions()[1].clone();
        let (cell_output, data) = tx.output_with_data(0).unwrap();
        (cell_output, data)
    }
    //data-loader
    fn build_data_loader(consensus: Consensus) -> GcsDataLoader {
        //let mut cells = Arc::new(RwLock::new(HashMap::new()));
        let mut cells = Arc::new(RwLock::new(HashMap::default()));

        //load cell and cell_deps
        let dep_out_point1 = packed::OutPoint::new(
            consensus.genesis_block().transactions()[0].hash(),
            OUTPUT_INDEX_SECP256K1_BLAKE160_SIGHASH_ALL as u32,
        );
        let (output1, data1) = secp256k1_blake160_sighash_cell(consensus.clone());
        cells
            .write()
            .unwrap()
            .insert(dep_out_point1, (output1, data1));

        let dep_out_point2 = packed::OutPoint::new(
            consensus.genesis_block().transactions()[0].hash(),
            OUTPUT_INDEX_SECP256K1_DATA as u32,
        );
        let (output2, data2) = secp256k1_data_cell(consensus.clone());
        cells
            .write()
            .unwrap()
            .insert(dep_out_point2, (output2, data2));

        let dep_out_point3 =
            packed::OutPoint::new(consensus.genesis_block().transactions()[1].hash(), 0);
        let (output3, data3) = secp_dep_cell(consensus.clone());
        cells
            .write()
            .unwrap()
            .insert(dep_out_point3, (output3, data3));

        GcsDataLoader {
            headers: Arc::new(RwLock::new(HashMap::new())),
            cells,
        }
    }
    fn build_tx(consensus: Consensus, data_loader: GcsDataLoader) -> TransactionView {
        let (cell, cell_data, always_success_script) = always_success_cell();
        let cell_dep = packed::CellDep::new_builder()
            .out_point(packed::OutPoint::new(
                consensus.genesis_block().transactions()[1].hash(),
                0,
            ))
            .dep_type(DepType::DepGroup.into())
            .build();

        let out_point = packed::OutPoint::new(
            h256!("0x1234").pack(),
            0,
        );
        let input = CellInput::new(out_point.clone(), 0);
        data_loader.cells.write().unwrap().insert(out_point.clone(), (cell.clone(), cell_data.clone()));
        TransactionBuilder::default()
            .output(
                CellOutputBuilder::default()
                    .capacity(capacity_bytes!(5_000).pack())
                    .lock(always_success_script.clone())
                    .build(),
            )
            .input(input)
            .cell_dep(
                CellDep::new_builder()
                    .out_point(cell_dep.out_point())
                    .build(),
            )
            .build()
       
    }

    #[test]
    fn test_verify() {
        let consensus = ckb_testnet_consensus();
        let data_loader = build_data_loader(consensus.clone());
        let tx_view = build_tx(consensus.clone(), data_loader.clone());
        let rtx = build_resolved_tx(data_loader.clone(), tx_view.clone());
        let verifier = TransactionScriptsVerifier::new(&rtx, &data_loader);
println!("len = {:?}", data_loader.cells.read().unwrap().len());
        let cycle = verifier.verify(max_cycles).unwrap();
println!("cycle is {:?}", cycle);
        assert!(cycle <= max_cycles);
    }
}
