use {
    crate::car::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// # Epoch is the top-level data structure in the DAG. It contains a list of
// # subsets, which in turn contain a list of blocks. Each block contains a list
// # of entries, which in turn contain a list of transactions.
// type Epoch struct {
//   # The kind of this object. This is used to determine which fields are
//   # present, and how to interpret them. This is useful for knowing which
//   # type of object to deserialize.
//   kind   Int
//   # The epoch number.
//   epoch  Int
//   # The list of subsets in this epoch.
//   subsets [ Link ] # [ &Subset ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Epoch {
    pub epoch: u64,
    pub subsets: Vec<Cid>,
}

impl TryFrom<&[u8]> for Epoch {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Epoch {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Epoch")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Epoch::kind")? as u64,
                    Kind::Epoch,
                )?,
                1 => node.epoch = util::cbor::get_int(value, "Epoch::epoch")? as u64,
                2 => {
                    node.subsets =
                        util::cbor::get_array_cids(value, "Epoch::subsets", "Epoch::subsets[]")?;
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}
