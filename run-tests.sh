#!/bin/bash

set -xeu -o pipefail

cargo test --features "additionaltest"
# cargo test --no-default-features
cargo test --no-default-features --features "gzip" --tests
cargo test --no-default-features --features "bgzip" --tests
cargo test --no-default-features --features "bzip2" --tests
cargo test --no-default-features --features "xz" --tests
cargo test --no-default-features --features "zstd" --tests
cargo test --no-default-features --features "tokio" --features "xz" --tests
cargo test --no-default-features --features "tokio_fs" --features "zstd" --tests
cargo test --no-default-features --features "tokio_fs" --features "gzip" --tests
cargo test --no-default-features --features "rayon" --features "gzip" --tests
cargo test --no-default-features --features "rayon" --features "bzip2" --tests

