<img src="https://raw.githubusercontent.com/Roenbaeck/bareclad/master/bareclad.svg" width="250">
<p/>

Bareclad is an experimental database engine based on Transitional Modeling, designed to capture conflicting, unreliable, and varying information over time. It blends ideas from relational, graph, columnar, and key–value stores.

- [The Philosophical Foundations of Bareclad](THEORY.md)
- [Paper: Modeling Conflicting, Unreliable, and Varying Information](https://www.researchgate.net/publication/329352497_Modeling_Conflicting_Unreliable_and_Varying_Information)

## Why Bareclad?

Most databases assume a single, consistent truth. In reality, facts are messy: they conflict across sources, change over time, and sometimes carry uncertainty. Bareclad treats this as a first‑class concern:

- Contradictions are preserved, not overwritten (assertions can be affirmed or negated with certainty).
- Time is built into every posit, so “what was true when” is natural to ask.
- Set‑based evaluation with roaring bitmaps keeps pattern matching fast without exploding joins.

This makes Bareclad well‑suited for master data management, regulated domains, investigations/intel, and any workflow where evidence accumulates and is revised.

<br/>

## Traqula DSL

Traqula is Bareclad's domain-specific language for defining roles, positing facts with time, and querying data through pattern matching. It supports variables that persist across commands, allowing complex multi-step operations.

For the complete language reference, examples, and details on posits, variables, and search patterns, see [TRAQULA.md](TRAQULA.md).

## Build and run

Prereqs: Rust toolchain and a C toolchain for rusqlite (bundled is enabled).

Build:

```sh
cargo build
```

Run the binary; it will read configuration from bareclad.json and execute the Traqula script at traqula/example.traqula:

```sh
target/debug/bareclad
```

Config (bareclad.json):

```json
{
	"database_file_and_path": "bareclad.db",
	"recreate_database_on_startup": true,
	"traqula_file_to_run_on_startup": "traqula/adds.traqula"
}
```

## Initialization Modes

The engine now uses an explicit persistence mode enum:

```rust
use bareclad::construct::{Database, PersistenceMode};

// Ephemeral: nothing is written, all data lost when process exits
let db = Database::new(PersistenceMode::InMemory);

// File-backed persistence (creates or reuses SQLite file)
let db = Database::new(PersistenceMode::File("bareclad.db".to_string()));

// Derive from config style flags
let enable = true; // imagine read from config
let mode = PersistenceMode::from_config(enable, "bareclad.db");
let db2 = Database::new(mode);
```

When running the provided binary, the `enable_persistence` flag in `bareclad.json` selects between these modes internally.

## Integrity Ledger

When running in file‑backed persistence mode, Bareclad records a compact integrity signal alongside persisted posits: a rolling hash over each persisted row. This ledger is always maintained for file‑backed databases and is intended as a lightweight way to spot accidental edits or simple corruption during local inspection.

Note: this is not a full audit or tamper‑proof trail. It provides a quick, low‑overhead check but does not protect against an attacker with write access who can recompute the chain or against external threats without anchoring. See the `persist` module for implementation details if you need stronger guarantees.

## Client / Server Architecture

Bareclad can run as a library or an HTTP server. The server layer (Axum + Tokio) exposes a JSON endpoint:

`POST /v1/query`

Request body:
```jsonc
{ "script": "search [{(*, name)}, +n, *] return n;", "stream": false, "timeout_ms": 5000 }
```

Response (single result set):
```jsonc
{
	"id": 0,
	"status": "ok",
	"elapsed_ms": 1.23,
	"columns": ["n"],
	"row_types": [["String"]],
	"row_count": 2,
	"limited": false,
	"rows": [["Alice"],["Bob"]]
}
```

If the script contains multiple `search` commands, the response omits top-level `columns/rows` and instead returns `result_sets` (array of result set objects) with cumulative `row_count`.

### Starting the server

You can run the server directly with the binary or use the convenience scripts provided for different platforms.

Windows (PowerShell):
```powershell
. .\scripts\bareclad.ps1                  # dot-source to load functions
Start-Bareclad -LogProfile normal -Tail   # run and stream logs live
Stop-Bareclad                             # stop
Restart-Bareclad -LogProfile verbose      # restart with different profile
```

macOS / Linux (bash):
```bash
chmod +x scripts/bareclad.sh            # first time
./scripts/bareclad.sh start --profile normal --tail   # foreground (logs to console)
./scripts/bareclad.sh stop
./scripts/bareclad.sh restart --profile verbose --force-rebuild
./scripts/bareclad.sh start --log 'warn,bareclad=info'  # custom RUST_LOG filter
./scripts/bareclad.sh tail               # follow log file if started in background
```

Both scripts support a common set of logging profiles mapped to `RUST_LOG`:

LogProfile | RUST_LOG
:--|:--
quiet | `error`
normal | `info`
verbose | `debug,bareclad=info`
trace | `trace`

You can override the profile with an explicit `--log` / `-Log` argument (EnvFilter syntax) such as `warn,axum=info,bareclad=debug`.

The bash script maintains a PID file at `.bareclad.pid` and writes background logs to `bareclad.out`; use `--tail` (bash) or `-Tail` (PowerShell) to stream logs directly instead.

Logging uses `tracing` with `RUST_LOG` filtering.

### Web UI (bareclad.html)

A minimal static HTML client (`bareclad.html`) demonstrates submitting scripts to the server endpoint and rendering results. Open it in a browser (or host it) and point the form to your server's `/v1/query` URL.

<img src="https://github.com/Roenbaeck/bareclad/blob/master/bareclad_web_app.png?raw=true">

## Updated Status and Roadmap

Implemented:
* Roles, appearances, appearance sets, heterogeneous posits with times
* Persistence via SQLite + tamper-evident ledger (file mode)
* Traqula parsing (Pest) for add/search/where/return, unions, multi-result scripts
* Bitmap-backed indexes for fast intersections
* Time filtering (variable vs literal and variable vs variable)
* Value predicate filtering (variable vs literal & variable vs variable) with type-aware ordering checks
* Certainty percent-only literals and strict ordering rules
* HTTP server (Axum), JSON query endpoint, multi-result response encoding
* PowerShell helper script for lifecycle (start/stop/restart) with logging presets
* Minimal HTML client page
* Execution error surfacing (unknown variable, type mismatch, ordering misuse)
* Streaming row delivery over HTTP (chunked / SSE)

Planned/next:
* WHERE enhancements: OR, grouping, BETWEEN, IN
* Aggregations and tuple-shaped / structured returns
* Projection type annotations stabilization (avoid dynamic probing)
* Authentication / access control for the server
* Optimization: caching value extraction during predicate evaluation
* Optional JSON/CSV export helpers

## License

This work is dual-licensed under Apache 2.0 and MIT. You can choose between one of them if you use this work.

SPDX-License-Identifier: Apache-2.0 OR MIT
