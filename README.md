<img src="https://raw.githubusercontent.com/Roenbaeck/bareclad/master/bareclad.svg" width="250">

Bareclad is an experimental database engine based on Transitional Modeling, designed to capture conflicting, unreliable, and varying information over time. It blends ideas from relational, graph, columnar, and key–value stores.

- [Paper: Modeling Conflicting, Unreliable, and Varying Information (Transitional Modeling)](https://www.researchgate.net/publication/329352497_Modeling_Conflicting_Unreliable_and_Varying_Information)
- [Background posts](https://www.anchormodeling.com/tag/transitional/)

## Why Bareclad?

Most databases assume a single, consistent truth. In reality, facts are messy: they conflict across sources, change over time, and sometimes carry uncertainty. Bareclad treats this as a first‑class concern:

- Contradictions are preserved, not overwritten (assertions can be affirmed or negated with certainty).
- Time is built into every posit, so “what was true when” is natural to ask.
- Set‑based evaluation with roaring bitmaps keeps pattern matching fast without exploding joins.

This makes Bareclad well‑suited for master data management, regulated domains, investigations/intel, and any workflow where evidence accumulates and is revised.

<br/>

<img src="https://raw.githubusercontent.com/Roenbaeck/bareclad/master/Traqula.svg" width="175">

Traqula is a small DSL with this shape:

	add role <role>[, <role>...]
	add posit [{((+var|var|*), <role>)}, <value>, <time>][, ...]
	search <pattern> [where <condition>] return <projection>

Note: an aggregate stage is planned but not implemented yet.

It lets you:
- add roles
- posit facts (value + time) about identities appearing in roles
- search with pattern matching over roles/values/times
- optionally filter with WHERE and project with RETURN

### Core concepts (engine)

- Thing: an opaque identity (u64 internally). Roles, appearance sets, and posits are all addressable Things.
- Role: a named placeholder (e.g., "wife", "name").
- Appearance: pairing of a Thing and a Role.
- AppearanceSet: a set of Appearances (max one per role), used as the left-hand side of a posit.
- Posit<V>: a proposition (AppearanceSet, Value, Time) with its own identity.

The engine maintains roaring bitmap backed indexes for fast set operations:
- role_to_posit_thing_lookup: Role -> {Posit IDs}
- appearance_set_to_posit_thing_lookup: AppearanceSet -> {Posit IDs}
- posit_thing_to_appearance_set_lookup: Posit ID -> AppearanceSet
- posit_time_lookup: Posit ID -> Time
- role_name_to_data_type_lookup: [role names] -> {value data types}

These keep joins/intersections efficient and avoid role-to-appearance blowups.

### Language essentials

- add role wife, husband, name;  // declare roles
- add posit ...                  // insert one or more posits
- search ... where ... return ...

Bindings and tokens:
- +x declares a new variable and binds it (e.g., +w for a wife identity)
- x recalls an existing variable
- * is a wildcard in search slots
- (w|h) is a union in role appearance position (either spouse)

Values support multiple types (String, JSON, Decimal, i64, Certainty, Time). Times accept literals like 'YYYY-MM-DD' or constants @NOW, @BOT, @EOT.

WHERE supports comparisons on time variables: <, <=, >, >=, =, ==.

### Data types and literals ("look‑alike" / WYSIWYG)

Traqula aims for “what you see is what you get” typing: the way a literal looks determines how it’s parsed and stored.

- String: "Alice" (double quotes). To embed a quote, double it: "" -> ".
- JSON: { "street": "..." } or [1, 2, 3] parsed as JSON.
- Decimal: 3.14159 parsed as arbitrary‑precision Decimal.
- Integer: 42 parsed as i64.
- Certainty: 100% or -100% parsed as Certainty.
- Time:
	- '1972' (year), '1972-02' (year‑month), '1972-02-13' (date), or '1972-02-13 12:34:56' (datetime)
	- Special constants: @NOW, @BOT (beginning of time), @EOT (end of time)

On insert, Bareclad records which value type is used for a given role set (role_name_to_data_type_lookup). The engine uses this to avoid mismatched types during reads and to keep projection fast.

### Example

See traqula/example.traqula for a complete startup script. A few highlights:

```
add role  wife, husband, name, age, address, epithet;

add posit +p1 [{(+idw, wife), (+idh, husband)}, "married", '2004-06-19'],
					+p1 [{(idh, name)}, "Lars Samuelsson", '1972-08-20'],
					[{(idw, name)}, "Anneli", '1972-02-13'];

/* names valid on a specific day */
search [{(*, name)}, +n, '1972-02-13']
return n;

/* names valid on or before a day */
search [{(*, name)}, +n, +t]
where t <= '1999-12-31'
return n;

/* married couples with an ascertained posit, union over spouse for name at date */
search +p [{(+w, wife), (+h, husband)}, "married", *],
					[{(p, posit), (*, ascertains)}, *, *],
					[{(w|h, name)}, +n, '2004-07-01']
return n;
```

### How the current engine evaluates

- Intersects role bitmaps to get candidate posits quickly.
- Captures candidate sets per bound variable:
	- value variables: which posits contribute the values
	- time variables: which posits carry the time
- Applies WHERE by filtering time-variable candidate bitmaps with the comparator.
- Projects in RETURN based on variable kinds inferred from the parser (Identity, Value, Time) and uses role to variable kind partitions to avoid probing impossible types.

This is intentionally minimal but efficient; more expressive joins and aggregations can be layered on top.

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
	"traqula_file_to_run_on_startup": "traqula/example.traqula"
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

## Integrity Ledger (Tamper-Evident Chain)

When running in file‑backed mode (`PersistenceMode::File`), every new posit is appended to an integrity ledger that forms a hash chain. This provides a lightweight, tamper‑evident signal that the sequence (and contents) of stored posits has not been rewritten silently.

### How it works

For each newly persisted posit the engine computes a BLAKE3 hash over a canonical serialization:

```
<Posit_Identity>|<AppearanceSet>|<ValueType_Identifier>|<AppearingValue>|<AppearanceTime>|prev=<PreviousHash>
```

Where:
- `AppearanceSet` is the pipe‑separated `thing_id,role_id` pairs in natural order (the same string stored in the `Posit` table).
- `ValueType_Identifier` is the numeric stable identifier of the value's data type.
- `AppearingValue` and `AppearanceTime` are serialized as (lossless) text representations.
- `PreviousHash` is the hash of the prior posit in ascending `Posit_Identity` order (or 64 zeros for the genesis entry).

An entry is then inserted into the `PositHash` table: `(Posit_Identity, PrevHash, Hash)` and the rolling head (hash + count) is maintained in the single‑row `LedgerHead` table (`Name='PositLedger'`).

### Backfilling / Verification

On startup the engine:
1. Restores things / roles / posits.
2. If `PositHash` is empty but posits exist (fresh upgrade), it backfills the entire chain deterministically.
3. If hashes exist, it recomputes and verifies them in sequence, logging any mismatch but not mutating historical rows.

### Accessing the head

Library callers can obtain the current head hash + count (file‑backed mode only):

```rust
if let Some((hash, count)) = db.persistor.lock().unwrap().current_superhash() {
	println!("ledger head: {hash} ({count} posits)");
}
```

### Purpose

The ledger helps detect accidental corruption or naive row‑level tampering (e.g. manually editing a value in SQLite) because any modification changes downstream hashes.

### Limitations & Threat Model

This is NOT a full audit or cryptographic anchoring system:
- Anyone with write access to both `Posit` and `PositHash` tables can rewrite history and recompute the chain from genesis (64 zero hash) without detection.
- There is no external timestamping or cross‑system anchoring by default.
- Reordering of inserts (while preserving identities) is detectable only if identities are strictly increasing and you compare prior head externally.

### Strengthening (Optional Ideas)

If stronger guarantees are needed you can layer on:
- Periodic external anchoring: write the `(HeadHash, Count, WallClockTime)` tuple to an append‑only log / transparency service / blockchain.
- Independent watchers: run a sidecar process that snapshots heads and alerts on unexpected regressions or chain divergence.
- Incorporate additional context in the hash preimage (e.g. deployment/version, machine ID) if that has provenance value.

### Rationale

This design deliberately favors simplicity and zero runtime cryptographic ceremony (one fast hash per posit). It offers a pragmatic middle ground: low overhead integrity signals without dictating a heavy auditing infrastructure.

## Status and roadmap

Implemented:
- Roles, appearances, appearance sets, heterogeneous posits with times
- Persistence via SQLite
- Traqula parsing (Pest) for add/search/where/return and union of recalls
- Bitmap-backed indexes for fast intersections
- Time filtering and per-variable candidate tracking for precise returns

Planned/next:
- Richer WHERE (OR, grouping) and comparisons on non-time values
- Aggregations and tuple-shaped returns
- Type tags for posits to remove remaining dynamic probing
- More docs and doctests

## License

This work is dual-licensed under Apache 2.0 and MIT. You can choose between one of them if you use this work.

SPDX-License-Identifier: Apache-2.0 OR MIT
