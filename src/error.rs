//! Errors, one enum per layer. A fault of ours (the database) and bad data from the node are
//! different variants, because they are handled differently: the first is logged as our
//! problem, the second as the node's, and a node is never blamed for what the database did.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0} is not set")]
    Missing(&'static str),
    #[error("{var}: {reason}")]
    Invalid { var: &'static str, reason: String },
}

/// What in a block does not fit the column it is written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockField {
    Size,
    Entries,
    Position,
    VerifyingKey,
    Circuit,
    ProofBytes,
    WitnessBytes,
    PublicInputs,
    Transfers,
    Registrations,
    Proofs,
}

impl std::fmt::Display for BlockField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Size => "the size of the block",
            Self::Entries => "the number of entries",
            Self::Position => "a position",
            Self::VerifyingKey => "a verifying key",
            Self::Circuit => "a circuit",
            Self::ProofBytes => "a proof",
            Self::WitnessBytes => "a witness",
            Self::PublicInputs => "the public inputs",
            Self::Transfers => "the number of transfers",
            Self::Registrations => "the number of registrations",
            Self::Proofs => "the number of proofs",
        })
    }
}

/// A block from a node that does not add up. These are validation errors: they say what is
/// wrong and where, as data. How they read is decided by whoever reports them.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockError {
    #[error("block {height}: the id does not match the header")]
    IdMismatch { height: u64 },
    #[error("block {height}: a txid does not match its transaction")]
    TxidMismatch { height: u64 },
    #[error("block {height}: tx_root does not match the transactions")]
    TxRootMismatch { height: u64 },
    #[error("block {height}: entry {position} of txs is not a transfer or a registration")]
    UnexpectedTx { height: u64, position: usize },
    #[error("block {height}: proof_txs entry {position} is not a proof request")]
    NotAProofRequest { height: u64, position: usize },
    #[error("block {height}: the fees add up to more than a u64")]
    FeeOverflow { height: u64 },
    #[error("block {height}: {field} is too large for the database")]
    TooLarge { height: u64, field: BlockField },
}

/// The database layer. Infrastructure (`Db`, `Migrate`), a block that does not add up
/// (`Block`) and values that cannot be stored or read back (the rest) are separate variants.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migrations: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// The node sent a block that does not add up. Not a fault of the database.
    #[error("bad data from the node: {0}")]
    Block(#[from] BlockError),
    /// A number from the node above what a BIGINT holds (9.2e18).
    #[error("{value} does not fit in a Postgres BIGINT")]
    NotBigint { value: u64 },
    /// The database holds a negative number where a count was expected.
    #[error("{value} is negative where a count was expected")]
    Negative { value: i64 },
    /// A column that must hold 32 bytes holds something else.
    #[error("{column} is not 32 bytes")]
    BadLength { column: &'static str },
}

/// Talking to a node: the request itself, or an answer that is not what the API promises.
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("{path}: the node answered {status}: {body}")]
    Status {
        path: String,
        status: u16,
        body: String,
    },
    #[error("the node has no blocks")]
    NoBlocks,
    /// A fake node that fails on purpose, in tests.
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the node sent a broken page of blocks: {0}")]
    BadPage(String),
}

impl SyncError {
    /// True when the fault is on our side (database), not the node's.
    pub fn is_local_fault(&self) -> bool {
        matches!(
            self,
            Self::Store(
                StoreError::Db(_)
                    | StoreError::Migrate(_)
                    | StoreError::Negative { .. }
                    | StoreError::BadLength { .. }
            )
        )
    }
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the node broke the event contract: {0}")]
    Contract(String),
}

/// An event of a known kind whose content does not make sense. The event is kept raw.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct BadEvent(pub String);
