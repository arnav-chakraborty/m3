---
title: Data Rollup
weight: 15
---

This page describes M3DB's automatic data rollup feature, which downsamples data *already stored in M3DB* as it approaches its original Time-To-Live (TTL). This allows for longer retention of aggregated data at coarser resolutions.

If you are looking for M3Aggregator's mapping and rollup rules, which process metrics *before* they are stored in M3DB, please see the [M3Aggregator Mapping and Rollup Rules documentation](/docs/operational_guide/mapping_rollup).

## Overview

M3DB supports automatic data rollup, allowing you to downsample high-resolution data into lower resolutions with longer retention periods. This is useful for managing storage costs while retaining aggregated data for long-term analysis.

When a fileset for a namespace is about to expire due to its Time-To-Live (TTL), M3DB can be configured to automatically roll up the data in that fileset to a new, coarser resolution. This rolled-up data is then stored with its own, typically longer, TTL.

Currently, M3DB supports configuring one rollup rule per namespace.

## How Rollup Works

1.  **Configuration**: Rollup is configured at the namespace level. You define a target resolution and a new TTL for the rolled-up data.
2.  **Trigger**: The rollup process is triggered as part of the regular data cleanup cycle. When M3DB identifies a fileset that has reached its original TTL and is due for deletion, it first checks if a rollup rule is configured for that namespace.
3.  **Execution**:
    *   If rollup is enabled, M3DB reads the data from the expiring fileset.
    *   It aggregates the data to the configured rollup resolution. The current default aggregation strategy is **"last value in window"**. This means for each series, and for each new rollup resolution window:
        *   The timestamp of the data point written to the rollup fileset will be the start time of that rollup window.
        *   The value of this data point will be the value of the *last original data point* that fell within that specific window.
        *   Annotations from that last original data point are preserved on the rolled-up data point.
        *   Other aggregation strategies (e.g., min, max, mean) may become configurable in the future.
    *   The rolled-up data is written to a new set of files (Info, Data, Index, Summaries, Bloom filter, Digests, Checkpoint) in a separate directory structure, typically `data/<namespace_id>/<shard_id>/rollup/<resolution_str>/`.
    *   This new rolled-up fileset has its own TTL as specified in the rollup configuration.
4.  **Cleanup**: After the rollup attempt (successful or not, though this behavior might be configurable in the future), the original high-resolution fileset is deleted as per its original TTL.
5.  **Querying**: M3DB's query engine discovers and can query data from these rolled-up filesets. See "Reading Rolled-Up Data" below for more details.

## Configuration

Rollup is configured within the namespace settings in your M3DB configuration file. Add a `rollup` section to your namespace definition:

```yaml
namespaces:
  - id: "my_namespace_with_rollup"
    # ... other namespace options (bootstrapEnabled, retention, etc.) ...
    index:
      enabled: true
      blockSize: 2h
    retention:
      retentionPeriod: 48h # Original data TTL
      blockSize: 2h
      # ... other retention options ...
    rollup:
      resolution: "1h"    # Target resolution for rollup, e.g., 1 hour
      newTTL: "720h"      # TTL for the rolled-up data, e.g., 30 days
```

**Rollup Configuration Fields:**

*   `resolution`: (String) The target resolution for the rollup (e.g., "5m", "1h", "6h"). This duration string must be parsable by Go's `time.ParseDuration`.
*   `newTTL`: (String) The Time-To-Live for the data once it has been rolled up to the new resolution (e.g., "30d", "90d", "365d"). This duration string must be parsable by Go's `time.ParseDuration`.

If the `rollup` section is omitted for a namespace, rollup will not be performed for that namespace.

## Reading Rolled-Up Data

M3DB automatically discovers rolled-up filesets. When querying data, M3DB can access both the original high-resolution data (within its TTL) and any available rolled-up data (within its respective TTL).

The current data retrieval strategy is to **prefer the most granular data available**. This means:
*   If a specific time point or a queried time range is covered by both original data and one or more rolled-up resolutions, M3DB will select and return the data from the fileset with the smallest block size (i.e., the highest/most granular resolution).
*   If only rolled-up data is available for a queried period (e.g., because the original data has passed its retention period), the available rolled-up data will be selected and returned.
*   Query results will reflect the actual resolution of the data served. For example, if you query for a period covered by 1-hour rolled-up data, you will receive data points at 1-hour intervals.

(Further details on how query APIs might allow specifying resolution preferences or how mixed-resolution results are presented will be documented as those aspects of the query engine are finalized.)

## Metrics

The rollup process emits several metrics to help monitor its operation. These metrics are typically found under a scope like `m3db.cleanup.rollup` (the exact path might vary based on your metrics configuration) and are tagged by `namespace`, `shard`, and `resolution`.

*   `attempts`: (Counter) Number of rollup operations attempted.
*   `success`: (Counter) Number of rollup operations that completed successfully.
*   `failures`: (Counter) Number of rollup operations that failed. Can be further broken down by error type tags.
*   `duration`: (Timer) Duration of successful rollup operations.
*   `active-rollups`: (Gauge) Number of rollup operations currently in progress.
*   `series-read`: (Counter) Number of series read from the original fileset during rollup.
*   `series-written`: (Counter) Number of series written to the new rollup fileset.
*   `data-read-bytes`: (Counter) Total bytes read from original filesets.
*   `data-written-bytes`: (Counter) Total bytes written to new rollup filesets.

Monitoring these metrics is crucial for understanding the health and performance of the rollup feature.
