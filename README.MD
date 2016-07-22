# `emit_elasticsearch`

Log events with the [`emit`](http://emit-rs.github.io/emit/emit/index.html) structured logger to Elasticsearch.

This is a lightweight `Collector` that indexes events in a timestamped index with a `_bulk` command.
The template format can be changed by specifying an `IndexTemplate`, but the log type will always
be `emitlog`.
Logged events should play nice with [kibana](http://elastic.co) out-of-the-box.

# Usage

Reference `emit_elasticsearch` in your `Cargo.toml`:

```
[dependencies]
emit = "*"
emit_elasticsearch = "*"
```

Log to Elasticsearch by adding an `ElasticCollector` to your pipeline:

```rust
#[macro_use]
extern crate emit;
extern crate emit_elasticsearch;

use emit::PipelineBuilder;
use emit_elasticsearch::prelude::*;

# fn main() {
let pipeline = PipelineBuilder::new()
    .write_to(
        ElasticCollector::default()
        .send_template().unwrap())
    .init();
# }
```

![Event in es](https://raw.githubusercontent.com/KodrAus/emit_elasticsearch/master/assets/events_in_es.png)