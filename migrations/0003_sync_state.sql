-- Where the indexer stopped following a node, and when blocks were dropped by a reorg.
-- Additive only: the API views in 0002 read the existing tables, so nothing is renamed or dropped.

-- One row per node we follow. height and hash are the last block written from that node.
-- It is saved in the same transaction as the blocks, so a restart never repeats or skips.
CREATE TABLE sync_state (
    node_id     TEXT PRIMARY KEY,
    height      BIGINT NOT NULL CHECK (height >= 0),
    hash        BYTEA NOT NULL CHECK (octet_length(hash) = 32),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- When a block stopped being part of the chain. NULL while it is canonical.
ALTER TABLE blocks ADD COLUMN orphaned_at TIMESTAMPTZ;
CREATE INDEX blocks_orphaned ON blocks (height) WHERE NOT canonical;
