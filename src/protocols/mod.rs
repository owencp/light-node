pub mod chain_store;
pub mod filter;
pub mod header_verifier;
pub mod peers;
pub mod sync;

pub use self::chain_store::{ChainStore, HeaderProviderWrapper};
pub use self::filter::{ControlMessage, FilterProtocol};
pub use self::header_verifier::{HeaderProvider, HeaderVerifier};
pub use self::peers::Peers;
pub use self::sync::SyncProtocol;

use ckb_script::TransactionScriptsVerifier;
use ckb_types::{
    bytes::Bytes,
    core::{
        cell::{CellMeta, CellMetaBuilder, ResolvedTransaction},
        BlockExt, Capacity, DepType, EpochExt, HeaderView, ScriptHashType, TransactionBuilder,
        TransactionView,
    },
    packed::{
        self, Byte32, CellDep, CellInput, CellOutput, OutPoint, Script, WitnessArgs,
        WitnessArgsBuilder,
    },
    prelude::*,
};

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
/*
impl ChainHeaderProvider for GcsDataLoader {
    // load header
    fn get_header(&self, block_hash: &Byte32) -> Option<HeaderView> {
        HeaderView::default();
    }
}
*/

pub fn build_resolved_tx(data_loader: GcsDataLoader, tx: TransactionView) -> ResolvedTransaction {
    let resolved_cell_deps = tx
        .cell_deps()
        .into_iter()
        .map(|dep| {
            let deps_out_point = dep.clone();
            let (dep_output, dep_data) = data_loader
                .cells
                .read()
                .unwrap()
                .get(&deps_out_point.out_point())
                .unwrap()
                .clone();
            CellMetaBuilder::from_cell_output(dep_output.to_owned(), dep_data.to_owned())
                .out_point(deps_out_point.out_point().clone())
                .build()
        })
        .collect();

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
