#!/bin/bash

cargo +nightly rustdoc -F full -- --cfg docsrs
