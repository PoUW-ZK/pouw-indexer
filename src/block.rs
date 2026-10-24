// Field order in each struct is the wire order, do not reorder fields.

use borsh::{BorshDeserialize, BorshSerialize};
use sha2::{Digest, Sha256};

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct BlockHeader {
    pub version: u32,
    pub height: u64,
    pub prev_hash: [u8; 32],
    pub timestamp: u64, // unix seconds
    pub miner: [u8; 32],
    pub difficulty: u64,
    pub bucket_count: u32,
    pub bucket_id: u32,
    pub tx_count: u32, // leaves of the Merkle tree: txs + proof_txs
    pub tx_root: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum ProofSystem {
    Groth16,  // 0
    PlonkKzg, // 1
    Marlin,   // 2
}

/// borsh writes an enum as one tag byte and then the fields of that variant.
#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum TxKind {
    Transfer {
        to: [u8; 32],
        amount: u64,
    }, // tag 0
    ProofRequest {
        circuit: [u8; 32], // txid of the RegisterCircuit tx
        public_inputs: Vec<[u8; 31]>,
        witness: Vec<u8>,
    }, // tag 1
    RegisterCircuit {
        system: ProofSystem,
        constraint_count: u64,
        verifying_key: Vec<u8>,
        circuit: Vec<u8>,
    }, // tag 2
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct TxBody {
    pub chain_id: u32,
    pub sender: [u8; 32],
    pub nonce: u64,
    pub fee: u64,
    pub kind: TxKind,
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct SignedTx {
    pub body: TxBody,
    pub signature: [u8; 64],
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct ProofTx {
    pub request: SignedTx, // kind must be ProofRequest
    pub usage_counter: u64,
    pub proof: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub txs: Vec<SignedTx>, // transfers and circuit registrations
    pub proof_txs: Vec<ProofTx>,
}

fn hash_of<T: BorshSerialize>(value: &T) -> [u8; 32] {
    sha256(&borsh::to_vec(value).expect("borsh encoding into a Vec cannot fail"))
}

impl BlockHeader {
    /// block hash = sha256(borsh(header))
    pub fn hash(&self) -> [u8; 32] {
        hash_of(self)
    }
}

impl SignedTx {
    /// txid = sha256(borsh(SignedTx)), the signature is included
    pub fn txid(&self) -> [u8; 32] {
        hash_of(self)
    }
}

impl ProofTx {
    /// proof hash = sha256(borsh(ProofTx))
    pub fn hash(&self) -> [u8; 32] {
        hash_of(self)
    }
}

impl Block {
    /// Decode a whole block. Fails if any byte is left over.
    pub fn decode(bytes: &[u8]) -> Result<Block, std::io::Error> {
        Block::try_from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Line {
        height: u64,
        hash: String,
        borsh_hex: String,
    }

    #[test]
    fn fixture_blocks_decode_and_hashes_match() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/blocks.borsh.jsonl"
        );
        let text = std::fs::read_to_string(path).expect("fixture file is missing");
        let mut count = 0;
        let mut proofs = 0;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let l: Line = serde_json::from_str(line).unwrap();
            let bytes = hex::decode(&l.borsh_hex).unwrap();
            let block = Block::decode(&bytes).expect("block must decode");

            assert_eq!(block.header.height, l.height);
            assert_eq!(format!("blk:{}", hex::encode(block.header.hash())), l.hash);
            // encoding again must give the same bytes
            assert_eq!(borsh::to_vec(&block).unwrap(), bytes);
            // header says how many leaves the Merkle tree has
            assert_eq!(
                block.header.tx_count as usize,
                block.txs.len() + block.proof_txs.len()
            );
            for p in &block.proof_txs {
                assert!(matches!(p.request.body.kind, TxKind::ProofRequest { .. }));
                proofs += 1;
            }
            count += 1;
        }
        assert_eq!(count, 20);
        assert!(proofs > 0, "the fixtures should contain proof transactions");
    }
}
