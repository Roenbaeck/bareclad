# Bareclad AI Coding Guidelines

## Architecture Overview
Bareclad is a database engine implementing Transitional Modeling concepts for handling conflicting, unreliable, and varying information over time. The core data model consists of:

- **Thing**: Opaque u64 identity
- **Role**: Named semantic placeholder (e.g., "wife", "name")
- **Appearance**: (Thing, Role) pairing
- **AppearanceSet**: Sorted, duplicate-free set of appearances (at most one per role)
- **Posit**: Proposition of (AppearanceSet, Value, Time) with its own Thing identity

All constructs follow a "keeper" pattern for canonical storage and deduplication using Arc-wrapped instances.

## Key Components
- `construct.rs`: Core data structures and keepers (Database, RoleKeeper, AppearanceKeeper, etc.)
- `datatype.rs`: DataType trait implementations (String, JSON, Decimal, Time, Certainty)
- `persist.rs`: SQLite persistence layer with schema management
- `traqula.rs`: Pest-based parser and execution engine for the Traqula DSL
- `traqula.pest`: Grammar definition for the query language

## Development Workflow
- **Build**: `cargo build`
- **Run**: `target/debug/bareclad` (reads `bareclad.json` config)
- **Config**: Specifies database file path, recreate flag, and startup Traqula script
- **Debug**: Prints construct counts when built in debug mode

## Coding Patterns
- **Keeper Pattern**: Use keepers for canonical storage - call `create_*` methods on Database rather than constructing directly
- **Identity Management**: Things and Posits have their own identities; use Database methods to generate new ones
- **AppearanceSet Ordering**: Maintain sorted order for appearance sets (natural order by thing_id,role_id pairs)
- **Data Type Inference**: Record data types per role set in `role_name_to_data_type_lookup` to avoid runtime type probing
- **Bitmap Operations**: Use roaring bitmaps (RoaringTreemap) for efficient set intersections/unions
- **Time Built-in**: Every posit includes a Time component; use @NOW, @BOT, @EOT constants

## Traqula DSL Patterns
- **Variable Binding**: `+var` declares new variable, `var` recalls existing, `*` is wildcard
- **Union in Roles**: `(w|h, name)` matches either recalled wife or husband identities
- **Pattern Matching**: Search patterns use same structure as posit insertion
- **WHERE Clauses**: Support time comparisons (`t <= '1999-12-31'`) with AND conjunctions

## Persistence Schema
- `Thing(Thing_Identity)`
- `Role(Role_Identity, Role, Reserved)`
- `DataType(DataType_Identity, DataType)`
- `Posit(Posit_Identity, AppearanceSet, AppearingValue, ValueType_Identity, AppearanceTime)`
- AppearanceSets serialized as pipe-separated `thing_id,role_id` pairs

## Performance Considerations
- Roaring bitmaps enable fast set operations without exploding joins
- Indexes maintained: role→posit, appearance_set→posit, posit→appearance_set, posit→time
- Candidate tracking per bound variable (value variables, time variables)
- Avoid premature allocation with tri-state ResultSetMode (Empty/Thing/Multi)

## Error Handling
- Current implementation panics on SQLite errors (future: domain error types)
- Use anyhow for custom error handling in new code
- Config loading uses config crate with HashMap conversion

## Testing
- Use criterion for benchmarking (see `benches/benchmark.rs`)
- Focus on set operation performance with roaring bitmaps
- Test with various result set sizes (empty, single element, large sets)