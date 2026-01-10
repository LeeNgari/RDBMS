# Minimal RDBMS Design Plan

This document summarizes the high-level design decisions for the minimal RDBMS built for the Pesapal Challenge '26. It reflects the **final agreed architecture** and the reasoning behind each decision.

---

## 1. Database Layout (Final Decision)

### High-level structure

Each **database is a directory**. Inside it:

* A **database-level meta.json** stores global metadata
* Each **table is a subdirectory**
* Each table directory contains:

  * `meta.json` (schema + constraints)
  * `data.json` (rows)

```
database/
├── meta.json              ← database-level metadata
├── users/
│   ├── meta.json          ← users table schema
│   └── data.json          ← users table rows
├── orders/
│   ├── meta.json
│   └── data.json
└── products/
    ├── meta.json
    └── data.json
```

### Why this structure was chosen

| Option                                     | Decision | Reason                                        |
| ------------------------------------------ | -------- | --------------------------------------------- |
| One file per DB                            | ❌        | Becomes hard to reason about as features grow |
| One file per table                         | ❌        | Lacks clear schema/data separation            |
| **Directory per DB + directory per table** | ✅        | Clean, scalable, easy to inspect and explain  |

This layout mirrors how real databases (and filesystems) model hierarchy while staying simple.

---

## 2. Database-Level `meta.json`

Stores global metadata only:

```json
{
  "version": 1,
  "tables": ["users", "orders"],
  "created_at": "2026-01-10"
}
```

### Purpose

* Discover tables quickly
* Validate database structure
* Support future extensions (versioning, migrations)

### Why not store schemas here?

* Avoids coupling table schemas together
* Tables remain self-contained
* Easier table deletion and creation

---

## 3. Table-Level `meta.json`

Each table owns its schema and constraints.

```json
{
  "table": "users",
  "columns": [
    { "name": "id", "type": "INT", "primary": true, "unique": true },
    { "name": "email", "type": "TEXT", "unique": true }
  ]
}
```

### Why table-level metadata?

* Clear ownership
* Simplifies schema loading
* Mirrors real DB catalogs

---

## 4. Table Data Storage (`data.json`)

Rows are stored as JSON objects:

```json
[
  { "id": 1, "email": "a@b.com" },
  { "id": 2, "email": "c@d.com" }
]
```

### Trade-offs

| Choice | Result                            |
| ------ | --------------------------------- |
| JSON   | Human-readable, easy debugging    |
| Binary | Faster but unnecessary complexity |

JSON was chosen to maximize clarity and debuggability.

---

## 5. Indexes (In-Memory Only)

Indexes are rebuilt on startup and live **only in memory**.

### Structure

```
Table
 ├── Rows (data.json)
 └── Indexes (memory)
      └── column → value → [row positions]
```

Example:

```
users.id index
1 → [0]
2 → [1]
```

### Why not persist indexes?

| Reason      | Explanation                |
| ----------- | -------------------------- |
| Consistency | Avoid index/data desync    |
| Simplicity  | No WAL or atomic updates   |
| Scope       | Not required for challenge |

Rebuilding indexes is cheap for the dataset sizes involved.

---

## 6. Query Results Format

All query results are returned internally as a **ResultSet**:

```json
{
  "columns": ["id", "email"],
  "rows": [
    [1, "a@b.com"],
    [2, "c@d.com"]
  ]
}
```

### Why not return raw JSON rows?

* Preserves column order
* Works naturally for joins
* Matches how real databases behave

---

## 7. JOIN Model (Initial Scope)

Only **INNER JOIN** is supported.

```sql
SELECT users.email, orders.id
FROM users
INNER JOIN orders
ON users.id = orders.user_id;
```

### Execution model

```
users table           orders table
id email              id user_id
1  a@b.com     ←→     10 1
2  c@d.com
```

Algorithm:

1. Scan left table
2. Use index on join column if available
3. Combine matching rows into a single result row

---


