//! A fake chain built with the node's own types (`pouw-types`, `pouw-api`), so every id,
//! Merkle root, derived field and JSON shape is what a real node produces. The proofs are
//! random bytes and the proof-of-work is not searched: the indexer does not check either.
//!
//! Used by the tests, by `indexer gen-fixtures` and by `indexer seed` without a file.

use std::collections::BTreeMap;

use pouw_api::view::{BlockView, ProofTxView};
use pouw_types::crypto::{Address, FieldInput, Hash32, SigningKey, hash_to_field, sha256};
use pouw_types::{
    Amount, Block, BlockHeader, BlockId, ChainId, CircuitId, ConstraintCount, Nonce, ProofSystem,
    ProofTx, SignedTx, Target, TxBody, TxKind, U32, U64, UsageCounter, block_id, eta_chain, mask,
    sign_body, txid,
};

const GENESIS_TIME: u64 = 1_792_790_429;
const MINERS: [&str; 2] = ["fake-miner-1", "fake-miner-2"];
/// (system, constraint count, proof size in bytes)
const CIRCUITS: [(ProofSystem, u64, usize); 3] = [
    (ProofSystem::Groth16, 85_000, 128),
    (ProofSystem::PlonkKzg, 60_000, 928),
    (ProofSystem::Marlin, 25_000, 1008),
];

fn key(name: &str) -> SigningKey {
    SigningKey::from_seed(sha256(&[b"pouw-indexer/fake/", name.as_bytes()]).0)
}

/// Builds blocks one after another. `Clone` it to make a fork.
#[derive(Clone)]
pub struct ChainBuilder {
    rng: u64,
    chain_id: u32,
    height: Option<u64>,
    tip: BlockId,
    time: u64,
    nonces: BTreeMap<[u8; 32], u64>,
    circuits: Vec<(CircuitId, ProofSystem)>,
    usage: BTreeMap<([u8; 32], [u8; 32]), u64>,
}

impl ChainBuilder {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: seed,
            chain_id: 1,
            height: None,
            tip: BlockId::ZERO,
            time: GENESIS_TIME,
            nonces: BTreeMap::new(),
            circuits: Vec::new(),
            usage: BTreeMap::new(),
        }
    }

    /// The same chain so far, but the next blocks differ: a competing branch.
    #[allow(dead_code)] // used by the tests
    pub fn reseeded(&self, salt: u64) -> Self {
        let mut other = self.clone();
        other.rng ^= salt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        other
    }

    fn next_u64(&mut self) -> u64 {
        // splitmix64
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn bytes(&mut self, n: usize) -> Box<[u8]> {
        (0..n).map(|_| self.next_u64() as u8).collect()
    }

    fn sign(&mut self, who: &str, fee: u64, kind: TxKind) -> SignedTx {
        let k = key(who);
        let address = k.address();
        let counter = self.nonces.entry(address.0).or_insert(0);
        let nonce = *counter;
        *counter += 1;
        let body = TxBody {
            chain_id: ChainId(self.chain_id),
            sender: address,
            nonce: Nonce(nonce),
            fee: Amount(fee),
            kind,
        };
        sign_body(&body, &k)
    }

    fn transfer(&mut self) -> SignedTx {
        let (from, to) = if self.below(2) == 0 {
            ("alice", "bob")
        } else {
            ("bob", "alice")
        };
        let amount = Amount(self.below(500) + 1);
        self.sign(
            from,
            10,
            TxKind::Transfer {
                to: key(to).address(),
                amount,
            },
        )
    }

    fn registration(&mut self, system: ProofSystem, constraints: u64) -> SignedTx {
        let verifying_key = self.bytes(384);
        let circuit = self.bytes(2048);
        let tx = self.sign(
            "registrar",
            100,
            TxKind::RegisterCircuit {
                system,
                constraint_count: ConstraintCount(constraints),
                verifying_key,
                circuit,
            },
        );
        self.circuits.push((CircuitId(txid(&tx)), system));
        tx
    }

    fn proof_tx(&mut self, miner: Address) -> ProofTx {
        let pick = self.below(self.circuits.len() as u64) as usize;
        let (circuit, system) = self.circuits[pick];
        let proof_len = CIRCUITS.iter().find(|c| c.0 == system).map_or(128, |c| c.2);
        let inputs = (0..1 + self.below(3))
            .map(|_| {
                let mut f = [0u8; 31];
                f.iter_mut().for_each(|b| *b = self.next_u64() as u8);
                FieldInput(f)
            })
            .collect();
        let witness_len = 64 + self.below(448) as usize;
        let requester = ["req-1", "req-2", "req-3"][self.below(3) as usize];
        let witness = self.bytes(witness_len);
        let request = self.sign(
            requester,
            30,
            TxKind::ProofRequest {
                circuit,
                public_inputs: inputs,
                witness,
            },
        );
        let counter = self.usage.entry((miner.0, circuit.hash().0)).or_insert(0);
        *counter += 1;
        let usage_counter = UsageCounter(*counter);
        ProofTx {
            request,
            usage_counter,
            proof: self.bytes(proof_len),
        }
    }

    fn assemble(
        &mut self,
        miner: Address,
        txs: Vec<SignedTx>,
        proof: Option<ProofTx>,
    ) -> BlockView {
        let height = self.height.map_or(0, |h| h + 1);
        if self.height.is_some() {
            self.time += 5 + self.below(10);
        }
        let header = BlockHeader {
            version: U32::new(1),
            chain_id: U32::new(self.chain_id),
            system_id: U64::new(0),
            height: U64::new(height),
            parent: self.tip,
            timestamp: U64::new(self.time),
            miner,
            tx_root: Hash32::ZERO,
            target: Target([0xff; 32]),
            nonce: U64::new(self.next_u64()),
        };
        let mut block = Block {
            header,
            txs,
            proof_txs: proof.into_iter().collect(),
        };
        block.header.tx_root = block.compute_tx_root();
        let id = block_id(&block.header);
        let etas = eta_chain(&self.tip, &block.compute_txs_root(), &block.proof_txs);
        let proofs = block
            .proof_txs
            .iter()
            .zip(etas)
            .map(|(p, eta)| {
                let circuit = match &p.request.body.kind {
                    TxKind::ProofRequest { circuit, .. } => *circuit,
                    _ => unreachable!("a proof tx holds a proof request"),
                };
                let system = self
                    .circuits
                    .iter()
                    .find(|c| c.0 == circuit)
                    .map_or(ProofSystem::Groth16, |c| c.1);
                let m = mask(&miner, &circuit, p.usage_counter);
                ProofTxView::new(p, hash_to_field(&eta), hash_to_field(&m), system)
            })
            .collect();
        self.tip = id;
        self.height = Some(height);
        BlockView::new(&block, proofs)
    }

    /// The next block: the genesis first, then a registration of three circuits, then a
    /// few transfers and usually one proved request per block (the node allows one).
    pub fn next(&mut self) -> BlockView {
        let Some(height) = self.height else {
            return self.assemble(Address([0; 32]), Vec::new(), None);
        };
        let miner = key(MINERS[self.below(2) as usize]).address();
        let mut txs = Vec::new();
        if height == 0 {
            for (system, constraints, _) in CIRCUITS {
                txs.push(self.registration(system, constraints));
            }
        }
        for _ in 0..self.below(4) {
            txs.push(self.transfer());
        }
        let proof = (height > 0 && self.below(10) < 7).then(|| self.proof_tx(miner));
        self.assemble(miner, txs, proof)
    }

    /// A block with no transactions, cheap to make. For size and speed tests.
    #[allow(dead_code)] // used by the tests
    pub fn next_empty(&mut self) -> BlockView {
        let miner = if self.height.is_some() {
            key(MINERS[0]).address()
        } else {
            Address([0; 32])
        };
        self.assemble(miner, Vec::new(), None)
    }

    pub fn take(&mut self, n: usize) -> Vec<BlockView> {
        (0..n).map(|_| self.next()).collect()
    }

    #[allow(dead_code)] // used by the tests
    pub fn take_empty(&mut self, n: usize) -> Vec<BlockView> {
        (0..n).map(|_| self.next_empty()).collect()
    }
}

/// `n` blocks from the genesis, with some activity.
pub fn demo_chain(n: usize) -> Vec<BlockView> {
    ChainBuilder::new(42).take(n)
}
