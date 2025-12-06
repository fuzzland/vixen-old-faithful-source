use {
    crate::car::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// type Entry struct {
//   kind         Int
//   numHashes    Int
//   hash         Hash
//   # The list of transactions in this entry.
//   transactions [ Link ] # [ &Transaction ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Entry {
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub transactions: Vec<Cid>,
}

impl TryFrom<&[u8]> for Entry {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Entry {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Entry")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Entry::kind")? as u64,
                    Kind::Entry,
                )?,
                1 => node.num_hashes = util::cbor::get_int(value, "Entry::num_hashes")? as u64,
                2 => node.hash = util::cbor::get_bytes(value, "Entry::hash")?,
                3 => {
                    node.transactions = util::cbor::get_array_cids(
                        value,
                        "Entry::transactions",
                        "Entry::transactions[]",
                    )?
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}
