<img src="https://raw.githubusercontent.com/Roenbaeck/bareclad/master/bareclad.svg" width="250">

Bareclad is an experimental database engine based on Transitional Modeling, designed to capture conflicting, unreliable, and varying information over time. It blends ideas from relational, graph, columnar, and key–value stores.

- [Paper: Modeling Conflicting, Unreliable, and Varying Information (Transitional Modeling)](https://www.researchgate.net/publication/329352497_Modeling_Conflicting_Unreliable_and_Varying_Information)
- [Background posts](https://www.anchormodeling.com/tag/transitional/)

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
- Projects in RETURN based on variable kinds inferred from the parser (Identity, Value, Time) and uses role->type partitions to avoid probing impossible types.

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
