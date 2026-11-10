-- The node's block header (pouw-node, crates/pouw-types) is 168 bytes with a proof-of-work
-- target and a nonce. It has no difficulty and no buckets, which block.md of pouw-docs still
-- describes (a header of 136 bytes). The node is the newer truth, so the bucket columns become
-- optional and the real header fields are added. Additive for readers: the API views of 0002
-- keep working, they show NULL where the header has no such field.

ALTER TABLE blocks
    ALTER COLUMN difficulty   DROP NOT NULL,
    ALTER COLUMN bucket_count DROP NOT NULL,
    ALTER COLUMN bucket_id    DROP NOT NULL;

ALTER TABLE blocks
    ADD COLUMN chain_id   BIGINT,
    ADD COLUMN system_id  BIGINT,                                  -- 0 = hash proof of work (header v1)
    ADD COLUMN target     BYTEA CHECK (octet_length(target) = 32), -- big-endian, block id must not exceed it
    ADD COLUMN pow_nonce  NUMERIC(20, 0);                          -- u64, can be above BIGINT
