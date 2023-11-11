#!/bin/bash

set -xeu -o pipefail

cargo test
cargo test --no-default-features
cargo test --no-default-features --features "gzip"
cargo test --no-default-features --features "bgzip"
cargo test --no-default-features --features "bzip2"
cargo test --no-default-features --features "xz"
cargo test --no-default-features --features "zstd"
cargo test --no-default-features --features "tokio"
cargo test --no-default-features --features "tokio_fs"
cargo test --no-default-features --features "tokio_fs" --features "gzip"

