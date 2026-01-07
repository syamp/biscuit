# Design Principles & Rationale

## Predictable TSDB with SQL on FoundationDB

### High-level goal

Build a **time-series system with predictable behavior**:

* Storage is **bounded and deterministic**
* Write pressure maps **directly** to system pressure
* Reducing volume **immediately reduces problems**
* No background “catch-up” work that explodes later
* SQL is available, but **query engine is not the storage engine**

This intentionally makes different tradeoffs than append-heavy TSDBs (e.g., Cassandra) so operational behavior stays predictable, and some of the reasoning may apply to other systems that rely heavily on background merges.

---

## Core Design Principles

### 1. Bounded storage by construction (not policy)

**Rule:**
Never create new time keys. Always overwrite existing keys.

**How:**
Per-metric fixed-size ring:

```
key = (metric_id, slot)
slot = (ts / step) % slots
```

**Consequences:**

* Number of keys per metric is constant
* Total key count is fixed:

  ```
  total_keys = num_metrics × slots
  ```
* Disk usage is deterministic:

  ```
  disk ≈ total_keys × record_size × replication
  ```
* No TTL
* No deletes
* No tombstones
* No key growth over time

**Comparison with other TSDB layouts:**

| System          | Key growth          |
| --------------- | ------------------- |
| Cassandra       | Unbounded (ts keys) |
| S3/Parquet      | Unbounded (files)   |
| **This design** | **Fixed forever**   |

---

### 2. Overwrite-in-place instead of append-and-clean

**Rule:**
Writes replace old values; they do not create new ones.

**Why this matters:**

* Append-based systems must clean up later
* Cleanup introduces:

  * compactions
  * tombstones
  * merges
  * background IO storms
* Cleanup continues *after* traffic drops

**FDB overwrite model:**

* Old versions exist only briefly (MVCC window, seconds)
* Versions are garbage-collected automatically
* Version history is **bounded**
* Disk does not grow with write rate

**Key outcome:**

> Write faster → higher latency
> Write slower → system cools immediately

No delayed consequences.

---

### 3. No deletes, no TTL, no compaction tuning

**Rule:**
Do not delete data. Do not expire data. Do not rely on TTL.

**Why:**

* Deletes create hidden work
* TTL creates delayed work
* Compaction turns yesterday’s load into today’s outage

**This design:**

* Storage window is enforced by ring math
* Old data is overwritten, not removed
* FDB GC is internal, automatic, and bounded

**Operational difference snapshot:**

| Pain point      | LSM / Columnar | This design |
| --------------- | -------------- | ----------- |
| Tombstones      | Yes            | No          |
| Compaction debt | Yes            | No          |
| Delete storms   | Yes            | No          |
| Surprise IO     | Yes            | No          |

---

### 4. Predictable backpressure instead of surprise failure

**When overloaded, this system does exactly one thing:**

* Latency increases
* Transactions throttle
* Writes slow or fail fast

**What does *not* happen:**

* Disk does not suddenly fill
* Background tasks do not pile up
* Reducing load actually helps immediately

**FDB behavior under pressure:**

* TLogs saturate → commits slow
* Storage servers push back
* Client sees errors/latency
* No runaway cleanup jobs

This maps cleanly to operational control.

---

### 5. Query engine is separate from storage

**Rule:**
Storage should not try to be a SQL engine.

**Architecture:**

```
FoundationDB  →  Ring storage (overwrite)
DataFusion    →  SQL execution
Coordinator   →  Fan-out + merge (later)
```

**Why this matters:**

* Storage stays simple and predictable
* SQL layer can evolve independently
* No coupling between query complexity and write stability

---

### 6. SQL, but intentionally constrained

**Supported SQL shape:**

* SELECT
* WHERE metric_id IN (...)
* WHERE ts BETWEEN start AND end
* GROUP BY ts_bucket(ts, N)
* Aggregates: avg, sum, min, max, count

**Explicit non-goals:**

* No PromQL-style magic
* No implicit joins across shards
* No unbounded scans

SQL is a **tool**, not the storage contract.

---

### 7. Linear scaling knobs (no hidden multipliers)

The system load scales **linearly** with:

* Number of metrics
* Sampling rate (`1 / step`)
* Ring size (`slots`)
* Record size

Reducing any of these immediately reduces:

* Write volume
* Disk usage
* CPU
* Network
* Latency

There is no secondary effect later.

---

## Comparison to Known Pain Points

### Cassandra / LSM trees (also relevant to other background-merge designs)

**Typical approach:** append new keys, delete later via TTL or compaction. Great for high write throughput but introduces tombstones and background cleanup.

**This design’s tradeoff:** overwrite-in-place with a fixed keyspace. You give up unbounded retention flexibility, but the system never schedules large compaction jobs and disk usage follows the ring math exactly.

---

### S3 / Parquet / DuckDB-style immutable files

**Typical approach:** batch-append immutable files and rewrite them for retention or compaction. Excellent for cold analytics, less ideal for per-sample updates.

**This design’s tradeoff:** mutable key-value storage optimized for overwrite and bounded windows. You sacrifice cheap historical reprocessing, but gain predictable real-time writes.

---

## Summary Statement

> This system is intentionally designed to trade maximum theoretical throughput for **predictability**.
>
> Storage is bounded by construction, not policy.
> Writes overwrite in place.
> There are no deletes, no TTLs, and no background compaction debt.
>
> If load increases, latency increases.
> If load decreases, pressure decreases immediately.
>
> There are no delayed failure modes.
