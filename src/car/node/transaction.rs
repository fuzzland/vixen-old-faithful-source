use crate::car::{
    node::{DataFrame, Kind, NodeError},
    util,
};

// type Transaction struct {
//   kind     Int
//   # Raw transaction data.
//   data     DataFrame
//   # Raw tx metadata data.
//   metadata DataFrame
//   # The slot number where this transaction was created.
//   slot     Int
//   # The index of the position of this transaction in the block (0-indexed).
//   index nullable optional  Int
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Transaction {
    pub data: DataFrame,
    pub metadata: DataFrame,
    pub slot: u64,
    pub index: Option<u64>,
}

impl TryFrom<&[u8]> for Transaction {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Transaction {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Transaction")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Transaction::kind")? as u64,
                    Kind::Transaction,
                )?,
                1 => node.data = DataFrame::try_from(value)?,
                2 => node.metadata = DataFrame::try_from(value)?,
                3 => node.slot = util::cbor::get_int(value, "Transaction::slot")? as u64,
                4 => {
                    node.index =
                        util::cbor::get_int_opt(value, "Transaction::index")?.map(|v| v as u64)
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}
