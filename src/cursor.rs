use crate::datasource::HashAndHeight;
use std::fmt;

#[derive(PartialEq, Debug)]
pub struct Cursor {
    pub block: HashAndHeight,
    pub finalized: HashAndHeight,
}

impl Cursor {
    pub fn new(block: HashAndHeight, finalized: HashAndHeight) -> Cursor {
        Cursor { block, finalized }
    }
}

impl TryFrom<String> for Cursor {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let split: Vec<_> = value.split(':').collect();

        if split.len() != 4 {
            return Err("invalid cursor".to_string());
        }

        let block = HashAndHeight {
            hash: split[1].to_string(),
            height: split[0].parse().map_err(|_| "invalid block height")?,
        };

        let finalized = HashAndHeight {
            hash: split[3].to_string(),
            height: split[2]
                .parse()
                .map_err(|_| "invalid finalized block height")?,
        };

        Ok(Cursor { block, finalized })
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.block.height, self.block.hash, self.finalized.height, self.finalized.hash
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::cursor::Cursor;
    use crate::datasource::HashAndHeight;

    #[test]
    fn display_cursor() {
        let cursor = Cursor {
            block: HashAndHeight {
                hash: "hash0".to_string(),
                height: 0,
            },
            finalized: HashAndHeight {
                hash: "hash1".to_string(),
                height: 1,
            },
        };

        let expected = "0:hash0:1:hash1".to_string();
        assert_eq!(cursor.to_string(), expected);
    }

    #[test]
    fn try_cursor_from_string() {
        let value = "0:hash0:1:hash1".to_string();
        let cursor = Cursor::try_from(value).unwrap();

        let expected = Cursor {
            block: HashAndHeight {
                hash: "hash0".to_string(),
                height: 0,
            },
            finalized: HashAndHeight {
                hash: "hash1".to_string(),
                height: 1,
            },
        };
        assert_eq!(cursor, expected);
    }
}
