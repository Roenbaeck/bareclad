<img src="https://raw.githubusercontent.com/Roenbaeck/bareclad/master/Traqula.svg" alt="Traqula Language Reference" width="200">
<p/>

Traqula is Bareclad's domain-specific language (DSL) for interacting with the database. It provides a simple, declarative way to define roles, posit facts, and query data using pattern matching.

## Overview

Traqula has a minimal syntax focused on three main operations:

```
add role <role>[, <role>...]
add posit [{((+var|var|*), <role>)}, <value>, <time>][, ...]
search <pattern> [where <condition>] [return <projection>] [limit <N>]
```

- **add role**: Declares new roles in the schema.
- **add posit**: Inserts propositions (posits) into the database.
- **search**: Queries the database using pattern matching, with optional filtering and projection.

Scripts are semicolon-separated sequences of these commands. The engine processes them sequentially, with variables persisting across commands.

## Core Concepts

### Posits: The Fundamental Unit

At the heart of Bareclad is the **posit** – a proposition that asserts a fact about identities at a specific point in time. Posits capture the essence of Transitional Modeling: information can be conflicting, uncertain, or change over time.

A posit has three components:
- **AppearanceSet**: A set of (identity, role) pairs describing what the posit is about
- **Value**: The asserted fact (typed data like strings, numbers, etc.)
- **Time**: When this assertion holds

Posits are themselves identities (Things), allowing meta-operations like "this posit ascertains another."

#### How Posits Work

- **Assertions vs. Reality**: Posits don't overwrite; they accumulate. Multiple posits can exist for the same appearance set with different values or times, representing conflicting or evolving information.
- **Certainty and Negation**: Values can include certainty levels (e.g., "75%"), and posits can be negated to represent retractions.
- **Temporal Nature**: Every posit has a time, enabling queries like "what was true on this date?"

For deeper background on posits and atomic data:
- [Anchor Modeling: Atomic Data](https://www.anchormodeling.com/atomic-data/)
- [Anchor Modeling: Colorful Transitional](https://www.anchormodeling.com/transitional/colorfulTransitional.html)

### Identities and Roles

- **Thing**: An opaque identity (internally a u64). Everything in Bareclad is a Thing: people, roles, posits themselves.
- **Role**: A named semantic placeholder (e.g., "wife", "name", "age").
- **Appearance**: A (Thing, Role) pair.
- **AppearanceSet**: A sorted set of appearances (at most one per role), forming the subject of a posit.

### Variables and Binding

Variables allow referencing identities across commands:

- `+var` (e.g., `+person`): Declares a new variable and binds it to a fresh identity.
- `var` (e.g., `person`): Recalls a previously bound variable.
- `*`: Wildcard that matches any identity without binding.

Variables persist across the entire script, enabling multi-step operations like creating an identity in one posit and referencing it in searches or additional posits.

## Language Essentials

### Adding Roles

```
add role wife, husband, name, age;
```

Declares roles. Roles must be added before they can be used in posits.

### Adding Posits

```
add posit [{(+person, name)}, "Alice", '2023-01-01'];
```

Inserts one or more posits. Each posit is an array: `[AppearanceSet, Value, Time]`.

- AppearanceSet: `{(identity, role), ...}`
- Value: Typed literal (see Data Types)
- Time: Time literal or constant

Variables can be bound here and reused later.

### Searching

```
search [{(*, name)}, +n, *]
where t <= '1999-12-31'
return n
limit 10;
```

Pattern matches against existing posits. The pattern mirrors posit structure but uses variables and wildcards.

#### Pattern Matching

- `{(var, role)}`: Matches posits where the identity in that role matches the variable.
- `{(w|h, role)}`: Union – matches either role.
- `*` in any position: Wildcard.

#### WHERE Clauses

Filter results with comparisons:

- **Time comparisons**: `t < '2020-01-01'`, `t1 == t2`
- **Value comparisons**: `v == "Alice"`, `age > 30`, `certainty >= 80%`

Supports variable vs literal and variable vs variable.

#### RETURN Projection

Specifies what to output. Variables are projected based on their type (Identity, Value, Time).

#### LIMIT Clause

```
search ... return ... limit 5;
```

Caps the number of rows returned. Includes a `limited` flag in results if more matches exist.

## Data Types and Literals

Traqula uses "look-alike" typing – the literal's appearance determines its type.

- **String**: `"Alice"` (double quotes; escape with `""`)
- **JSON**: `{ "key": "value" }` or `[1, 2, 3]`
- **Decimal**: `3.14159` (arbitrary precision)
- **Integer**: `42` (i64)
- **Certainty**: `75%` (must end with `%`)
- **Time**:
  - `'1972'`, `'1972-02'`, `'1972-02-13'`, `'1972-02-13 12:34:56'`
  - Constants: `@NOW`, `@BOT`, `@EOT`

## Examples

See the `traqula/` folder for complete scripts. Highlights:

```
add role wife, husband, name, age, address, epithet;

# Create identities and posit relationships
add posit +p1 [{(+idw, wife), (+idh, husband)}, "married", '2004-06-19'],
         +p1 [{(idh, name)}, "Lars Samuelsson", '1972-08-20'],
         [{(idw, name)}, "Anneli", '1972-02-13'];

# Find names valid on a specific date
search [{(*, name)}, +n, '1972-02-13']
return n;

# Find names valid on or before a date
search [{(*, name)}, +n, +t]
where t <= '1999-12-31'
return n;

# Complex query: married couples with ascertainment
search +p [{(+w, wife), (+h, husband)}, "married", *],
       [{(p, posit), (*, ascertains)}, *, *],
       [{(w|h, name)}, +n, '2004-07-01']
return n;
```

## Engine Evaluation

The engine uses roaring bitmap indexes for efficiency:

- Intersects role bitmaps to find candidate posits
- Tracks candidate sets per variable (value and time)
- Applies WHERE filters to candidate bitmaps
- Projects results using type-aware inference

This keeps queries fast without expensive joins.