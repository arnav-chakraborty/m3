# M3DB ARM Performance Optimization Analysis and Changes

This document outlines the investigation into M3DB performance characteristics on ARM architectures and the changes made to address specific x86-64 optimizations.

## Initial Problem

Users reported higher latencies, increased CPU utilization, and marginally higher memory usage when deploying M3DB on ARM-based machines compared to x86-64. This prompted an investigation into architecture-specific code paths and optimization opportunities for ARM.

## 1. CPU Core Detection (`src/x/sync`)

### Finding
The primary architecture-specific code identified was in `src/x/sync` related to CPU core detection:
- `src/x/sync/cpu_linux_amd64.s`: This file contained x86-64 assembly code using the `RDTSCP` instruction to get the current CPU core ID.
- `src/x/sync/index_cpu.go`: The `CPUCore()` function utilized this assembly code.

The `RDTSCP` instruction is not available on ARM processors. This meant that `CPUCore()` would not function correctly, potentially leading to all affinity-based operations defaulting to a single queue or behaving unpredictably.

This was critical because `CPUCore()` and `NumCores()` are used in performance-sensitive areas:
- `src/dbnode/storage/index_insert_queue.go`
- `src/dbnode/storage/shard_insert_queue.go`
These queues use `CPUCore()` for CPU affinity (sharding insert operations by CPU core to reduce lock contention and improve cache locality) and `NumCores()` for sizing internal data structures (like channel buffers).

### Changes Implemented
To address this, the following changes were made (committed in `arm-cpu-sync-optimizations` branch):
1.  **Created `src/x/sync/cpu_linux_arm64.go`**:
    - This file provides an ARM64-specific implementation of the `getCore() int` function.
    - It uses the `unix.SchedGettcpu()` system call (from `golang.org/x/sys/unix`), which is the standard way to get the current CPU on Linux and is compatible with ARM64.
    - Build tags: `//go:build linux && arm64`
2.  **Adjusted Build Tags in `src/x/sync/cpu_unsupported_arch_supported_os.go`**:
    - Changed build tag from `// +build !amd64,linux` to `//go:build linux && !amd64 && !arm64` (and `// +build linux,!amd64,!arm64`).
    - This ensures the new ARM64 implementation is selected for `linux/arm64` builds and the stub is used for other non-AMD64, non-ARM64 Linux architectures.
3.  **Updated Comment in `src/x/sync/index_cpu.go`**:
    - The comment in `CPUCore()` was changed from mentioning `RDTSCP` to refer to `platform-specific getCore()`, reflecting the new multi-architecture support.
4.  **`NumCores()`**: The existing `NumCores()` implementation in `src/x/sync/num_cores_linux.go` (which reads `/proc/cpuinfo`) was deemed generally portable to ARM Linux and was not changed.

## 2. Investigation of Other Potential Areas

Several other areas of the codebase were reviewed for potential x86-specific code or areas needing ARM-specific attention:

### a. Time-Series Encoding (`src/dbnode/encoding/m3tsz`)
- **Finding**: The M3TSZ encoding logic is implemented in pure Go and appears to be architecture-agnostic. Performance will depend on the Go compiler and ARM hardware's arithmetic/bitwise operation efficiency.
- **Action**: No code changes required for compatibility.

### b. Filesystem and Memory Mapping (`src/dbnode/persist/fs`, `src/x/mmap`)
- **Finding**: The Linux memory mapping implementation (`src/x/mmap/mmap_linux.go`) uses standard Linux syscalls (`mmap`, `munmap`, `madvise`) available on ARM. Filesystem operations in `src/dbnode/persist/fs` are standard Go.
- **Action**: No code changes required for compatibility. Potential performance differences in `MAP_HUGETLB` behavior are runtime/OS-dependent, not code-level issues.

### c. Indexing Engine (`src/m3ninx`)
- **Finding**: `m3ninx` and its primary dependency `github.com/m3dbx/pilosa/roaring` (for Roaring bitmaps) are implemented in pure Go. While `pilosa/roaring` uses `unsafe` for performance (direct memory casting), this usage is generally portable. No explicit x86 assembly or CPU-specific intrinsics were found.
- **Action**: No code changes required for compatibility. Performance depends on the Go compiler and ARM hardware.

### d. Memory Operations (`src/query/util/memset.go`, `src/metrics/encoding/protobuf/buffer.go`)
- **Finding**:
    - `memset.go`: Uses pure Go. For zero values, it relies on compiler optimization of loops. For non-zero values, it uses the built-in `copy()` function.
    - `buffer.go`: Manages byte buffers, also relying on `copy()` for data movement during resizes.
- **Recommendation**: These are portable. If profiling on ARM shows these specific operations (or the underlying `copy()`) to be significant bottlenecks, targeted ARM NEON SIMD implementations could be considered as a future optimization. This would require careful implementation and benchmarking.

### e. Object Pooling (`src/x/pool/object.go`)
- **Finding**: The generic object pooling mechanism is pure Go and portable.
- **Action**: No code changes required.

### f. Memory Pooling Configuration (`src/x/pool/config.go`, `src/x/pool/bucketized.go`)
- **Finding**: Bucketized memory pools (e.g., `BytesPool`) are highly configurable via YAML files (`poolingPolicy` in M3DBNode, `bytesPool` sections in M3Aggregator). Default bucket configurations also exist in Go code as fallbacks (e.g., in `src/cmd/services/m3dbnode/config/pooling.go`).
- **Recommendation**: This is a key area for ARM performance tuning.
    - **Tune via YAML**: Adjust `capacity` and `count` for buckets in YAML configurations based on ARM CPU cache sizes (typically 64-byte or 128-byte L1 cache lines for server ARM CPUs) and observed memory allocation patterns from profiling M3DB on ARM.
    - **Goal**: Minimize direct allocations (pool misses) and improve cache utilization by choosing bucket sizes that align well with common allocation sizes and cache lines.
    - **Example Default Buckets (M3DBNode BytesPool)**: `{16, 32, 64, 128, 256, 1440, 4096}` bytes. These offer a reasonable starting point but should be validated on ARM.

### g. Concurrency and Lock Contention (`src/dbnode/storage/*_insert_queue.go`)
- **Finding**: The index and shard insert queues use a per-CPU core sharding strategy for enqueueing data, with fine-grained locks for each core's queue. This is a sound design for multicore systems. The batch rotation and aggregation steps in the processing loop have sequential components but are necessary for centralizing work.
- **Recommendation**: The design is portable. Performance on ARM will depend on the Go runtime's lock, channel, and scheduler efficiency on ARM, and potential microarchitectural differences in cache coherency during data aggregation. No immediate code changes to locking strategy are recommended without ARM-specific profiling data indicating specific contention points not already mitigated by the per-core design.

### h. Buffer Operations (`src/dbnode/storage/series/buffer.go`, `src/m3ninx/x/bytes/slice_arraypool_gen.go`)
- **Finding**:
    - `series/buffer.go`: Orchestrates higher-level buffer and stream operations using encoders and pools. It does not contain direct low-level buffer manipulation code that would be an obvious candidate for architecture-specific SIMD.
    - `slice_arraypool_gen.go`: This generated code provides a pool for `[][]byte`. Its internal `grow` method efficiently nils out slice headers, which is not an operation typically targeted by SIMD.
- **Action**: These components are portable. Performance depends on underlying encoders and Go runtime features.

## 3. General Recommendations for ARM Performance
1.  **Profiling**: Thoroughly profile M3DB (CPU, memory, lock contention) on the target ARM hardware under realistic workloads. This is crucial to identify actual bottlenecks.
2.  **Configuration Tuning**:
    - **Memory Pools**: Adjust `BytesPool` and other pool bucket sizes in YAML configurations.
3.  **Targeted Optimizations (If Necessary)**:
    - If profiling reveals specific hotspots in pure Go code (e.g., `memset.go`, critical loops in encoding/indexing) that are significantly less performant on ARM than x86 (and where the Go compiler isn't already generating optimal ARM code), consider hand-optimizing with ARM NEON assembly. This should be a last resort due to complexity and maintenance overhead.
4.  **Compiler and Go Version**: Ensure use of a recent Go version, as ARM support and code generation quality improve over time. Use appropriate ARM architecture flags during compilation (e.g., `GOARCH=arm64`).

## Conclusion
The most critical x86-specific code (`RDTSCP` for CPU core detection) has been addressed by providing an ARM64-compatible implementation. The rest of the M3DB codebase reviewed largely consists of portable Go code. Future ARM performance improvements will primarily come from careful profiling on ARM hardware, tuning configurations (especially memory pooling), and potentially highly targeted SIMD optimizations if clear, significant bottlenecks are identified in pure Go code that the compiler cannot sufficiently optimize for ARM.
