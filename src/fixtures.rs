//! Read blocks from a fixtures file: one JSON object per line with a `borsh_hex` field.

use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

use crate::block::Block;

#[derive(Deserialize)]
struct Line {
    borsh_hex: String,
}

pub fn read_blocks(path: impl AsRef<Path>) -> anyhow::Result<Vec<Block>> {
    let path = path.as_ref();
    let text =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let mut blocks = Vec::new();
    for (i, line) in text
        .lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
    {
        let l: Line = serde_json::from_str(line)
            .with_context(|| format!("line {}: not valid JSON", i + 1))?;
        let bytes =
            hex::decode(&l.borsh_hex).with_context(|| format!("line {}: bad hex", i + 1))?;
        blocks.push(
            Block::decode(&bytes).with_context(|| format!("line {}: not a valid block", i + 1))?,
        );
    }
    Ok(blocks)
}
