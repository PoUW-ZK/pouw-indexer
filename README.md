# pouw-indexer
Indexer and real-time API backed by PostgreSQL

## Run locally

```bash
docker compose up -d     # Postgres on 127.0.0.1:5432
export DATABASE_URL=postgres://pouw:pouw@localhost:5432/pouw_indexer
cargo run &              # migrations, then the API on 0.0.0.0:8000 (BIND_ADDR), /health
cargo run -- seed        # load tests/fixtures/blocks.borsh.jsonl
./pre_push_tests.sh      # fmt, clippy, tests
```

### .env alternative
The code also supports `.env` file. This means it you can, alternatively, put the DATABASE_URL variable in a .env file and run the script as follows:

```bash
docker compose up -d     # Postgres on 127.0.0.1:5432
cargo run &              # migrations, then the API on 0.0.0.0:8000 (BIND_ADDR), /health
cargo run -- seed        # load tests/fixtures/blocks.borsh.jsonl
./pre_push_tests.sh      # fmt, clippy, tests
```

## Dockerfile

`Dockerfile` builds the image that pouw-deploy runs as `indexer`
(`ghcr.io/pouw-zk/pouw-indexer`, published from `main` by `docker.yml`). To run the
whole network (node, indexer, Hasura, dashboard) with your local changes, clone
pouw-deploy next to this repository and run `make dev` there.
