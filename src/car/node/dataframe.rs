use {
    crate::car::{
        node::{Kind, NodeError},
        util,
    },
    cid::Cid,
};

// # DataFrame is a chunk of data that is part of a larger whole. It contains
// # a hash of the whole data, and the index of this chunk in the larger whole.
// # This is used to verify that the data is not corrupted, and to reassemble
// # the data in the correct order.
// #
// # The data is stored in a Buffer, which is a raw byte array.
// # The hash is stored as a CRC64 ISO 3309.
// #
// # The `next` field is used to link multiple frames together. This is used
// # when the data is too large to fit in a single frame.
// #
// # Example: a payload is too large to fit in a single frame, so it is
// # split into multiple frames. Let's say it is split into 10 frames.
// # These are what the frames would look like (excluding some fields):
// # - DataFrame { index: 0, total: 10, data: [...], next: [cid1, cid2, cid3, cid4, cid5] }
// # - DataFrame { index: 1, total: 10, data: [...], next: [] }
// # - DataFrame { index: 2, total: 10, data: [...], next: [] }
// # - DataFrame { index: 3, total: 10, data: [...], next: [] }
// # - DataFrame { index: 4, total: 10, data: [...], next: [] }
// # - DataFrame { index: 5, total: 10, data: [...], next: [cid6, cid7, cid8, cid9] }
// # - DataFrame { index: 6, total: 10, data: [...], next: [] }
// # - DataFrame { index: 7, total: 10, data: [...], next: [] }
// # - DataFrame { index: 8, total: 10, data: [...], next: [] }
// # - DataFrame { index: 9, total: 10, data: [...], next: [] }
// type DataFrame struct {
//   kind  Int
//   # Hash of the whole data across all frames, using CRC64 ISO 3309.
//   hash nullable optional  Int
//   # Index of this frame among all frames (0-indexed).
//   index nullable optional Int
//   # Total number of frames.
//   total nullable optional Int
//   # Raw data, stored as a byte array.
//   data                    Buffer
//   # The next frames in the list (if any).
//   next nullable optional  [ Link ] # [ &DataFrame ]
// } representation tuple
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct DataFrame {
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
    pub next: Vec<Cid>,
}

impl TryFrom<&[u8]> for DataFrame {
    type Error = NodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(serde_cbor::from_slice::<serde_cbor::Value>(value)?)
    }
}

impl TryFrom<serde_cbor::Value> for DataFrame {
    type Error = NodeError;

    fn try_from(value: serde_cbor::Value) -> Result<Self, Self::Error> {
        let mut node = Self::default();
        for (index, value) in util::cbor::get_array(value, "DataFrame")?
            .into_iter()
            .enumerate()
        {
            match index {
                0 => NodeError::assert_invalid_kind(
                    util::cbor::get_int(value, "DataFrame::kind")? as u64,
                    Kind::DataFrame,
                )?,
                1 => {
                    node.hash = util::cbor::get_int_opt(value, "DataFrame::hash")?.map(|v| v as u64)
                }
                2 => {
                    node.index =
                        util::cbor::get_int_opt(value, "DataFrame::index")?.map(|v| v as u64)
                }
                3 => {
                    node.total =
                        util::cbor::get_int_opt(value, "DataFrame::total")?.map(|v| v as u64)
                }
                4 => node.data = util::cbor::get_bytes(value, "DataFrame::data")?,
                5 => {
                    node.next = util::cbor::get_array_opt(value, "DataFrame::next")?
                        .unwrap_or_default()
                        .into_iter()
                        .map(|value| util::cbor::get_cid(value, "DataFrame::next[]"))
                        .collect::<Result<_, _>>()?
                }
                _ => return Err(NodeError::UnexpectedCborValues),
            }
        }
        Ok(node)
    }
}
