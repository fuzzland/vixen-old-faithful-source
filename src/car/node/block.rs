use {
    crate::car::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// type Block struct {
//   kind      Int
//   # The slot number where this block was created.
//   slot      Int
//   shredding [ Shredding ]
//   entries   [ Link ] # [ &Entry ]
//   # The metadata for this block.
//   meta      SlotMeta
//   # Link to the rewards for this block.
//   rewards   Link     # &Rewards
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Block {
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    pub entries: Vec<Cid>,
    pub meta: SlotMeta,
    pub rewards: Cid,
}

impl TryFrom<&[u8]> for Block {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for Block {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Block")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "Block::kind")? as u64,
                    Kind::Block,
                )?,
                1 => node.slot = util::cbor::get_int(value, "Block::slot")? as u64,
                2 => {
                    for value in util::cbor::get_array(value, "Block::shredding")? {
                        node.shredding.push(Shredding::try_from(value)?);
                    }
                }
                3 => {
                    node.entries =
                        util::cbor::get_array_cids(value, "Block::entries", "Block::entries[]")?
                }
                4 => node.meta = SlotMeta::try_from(value)?,
                5 => node.rewards = util::cbor::get_cid(value, "Block::rewards")?,
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

// type Shredding struct {
//   entryEndIdx Int
//   shredEndIdx Int
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Shredding {
    pub entry_end_idx: i64,
    pub shred_end_idx: i64,
}

impl TryFrom<serde_cbor::Value> for Shredding {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "Shredding")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => {
                    node.entry_end_idx =
                        util::cbor::get_int(value, "Shredding::entry_end_idx")? as i64
                }
                1 => {
                    node.shred_end_idx =
                        util::cbor::get_int(value, "Shredding::shred_end_idx")? as i64
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}

// type SlotMeta struct {
//   # The parent slot of this slot.
//   parent_slot         Int
//   # Block time of this slot.
//   blocktime           Int
//   # Block height of this slot.
//   block_height nullable optional Int
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct SlotMeta {
    pub parent_slot: u64,
    pub blocktime: u64,
    pub block_height: Option<u64>,
}

impl TryFrom<serde_cbor::Value> for SlotMeta {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "SlotMeta")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => node.parent_slot = util::cbor::get_int(value, "SlotMeta::parent_slot")? as u64,
                1 => node.blocktime = util::cbor::get_int(value, "SlotMeta::blocktime")? as u64,
                2 => {
                    node.block_height =
                        util::cbor::get_int_opt(value, "SlotMeta::block_height")?.map(|v| v as u64)
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}
