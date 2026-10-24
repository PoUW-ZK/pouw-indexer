#! /usr/bin/env bash

cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test