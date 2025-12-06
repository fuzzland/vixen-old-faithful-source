use {
    crate::car::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// type Subset struct {
//   kind   Int
//   # First slot in this subset.
//   first  Int
//   # Last slot in this subset.
//   last   Int
//   # The list of blocks in this subset.
//   blocks [ Link ] # [ &Block ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Subset {
    pub first: u64,
    pub last: u64,
    pub blocks: Vec<Cid>,
}

impl TryFrom<&[u8]> for Subset {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Subset {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Subset")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Subset::kind")? as u64,
                    Kind::Subset,
                )?,
                1 => node.first = util::cbor::get_int(value, "Subset::first")? as u64,
                2 => node.last = util::cbor::get_int(value, "Subset::last")? as u64,
                3 => {
                    node.blocks =
                        util::cbor::get_array_cids(value, "Subset::blocks", "Subset::blocks[]")?
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}
