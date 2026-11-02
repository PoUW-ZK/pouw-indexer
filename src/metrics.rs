//! Prometheus metrics of the indexer, served as text on /metrics.

use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};

pub struct Metrics {
    registry: Registry,
    pub indexed_height: IntGauge,
    pub node_height: IntGauge,
    pub lag_blocks: IntGauge,
    pub blocks_written: IntCounter,
    pub reorgs: IntCounter,
    pub last_reorg_depth: IntGauge,
    pub node_errors: IntCounter,
    pub batch_seconds: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let gauge = |name: &str, help: &str| {
            let g = IntGauge::new(name, help).expect("valid metric");
            registry.register(Box::new(g.clone())).expect("register");
            g
        };
        let counter = |name: &str, help: &str| {
            let c = IntCounter::new(name, help).expect("valid metric");
            registry.register(Box::new(c.clone())).expect("register");
            c
        };
        let batch_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "pouw_indexer_batch_seconds",
                "Time to write one page of blocks",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
        )
        .expect("valid metric");
        registry
            .register(Box::new(batch_seconds.clone()))
            .expect("register");
        Self {
            indexed_height: gauge(
                "pouw_indexer_indexed_height",
                "Height of the last block in the database",
            ),
            node_height: gauge("pouw_indexer_node_height", "Height of the node's chain tip"),
            lag_blocks: gauge(
                "pouw_indexer_lag_blocks",
                "Blocks the indexer is behind the node",
            ),
            blocks_written: counter("pouw_indexer_blocks_written_total", "New blocks written"),
            reorgs: counter("pouw_indexer_reorgs_total", "Reorganisations handled"),
            last_reorg_depth: gauge(
                "pouw_indexer_last_reorg_depth",
                "Blocks dropped by the last reorganisation",
            ),
            node_errors: counter("pouw_indexer_node_errors_total", "Failed sync attempts"),
            batch_seconds,
            registry,
        }
    }

    /// Prometheus text format.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("encode");
        String::from_utf8(buf).expect("prometheus text is utf-8")
    }
}
