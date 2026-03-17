<img src="logo.svg" alt="Chronicle" width="180" align="left">

<h3>Chronicle</h3>

Modern distributed indexed streaming system.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![Stars](https://img.shields.io/github/stars/chronicle-laboratory/chronicle?style=flat-square&logo=github)](https://github.com/chronicle-laboratory/chronicle)

<br clear="left">

## Features

- **Streaming API** — Simple `record` and `fetch` operations
- **Key-based compaction** — Retain latest value per key for changelog and state snapshot workloads
- **Secondary offset** — Look up events by both primary offset and secondary index
- **Transactions** — Atomic writes across multiple keys
- **Native schema** — Schema enforcement built into the core
- **Replication** — Full replication with all-ack durability guarantee
- **Auto rebalancing** — Seamless cluster scaling without downtime
- **Placement policies** — Rack-aware and zone-aware replica placement
- **Native offloading** — Tiered storage with automatic offload to object store
- **TLA+ verified** — Formal verification for correctness
- **Rust core, C ABI** — Built in Rust, exposes C ABI for multi-language bindings