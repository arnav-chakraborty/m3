# M3TSZ-Advanced Encoding Scheme

## 1. Overview

M3TSZ-Advanced is a time series compression algorithm designed to improve upon the compression ratios of the standard M3TSZ encoding, particularly for series that exhibit regular patterns in timestamp deltas or float values. It achieves this by incorporating more sophisticated compression techniques:

*   **Adaptive Delta Encoding for Timestamps:** The number of bits used to store timestamp deltas adjusts based on the magnitude of recent deltas.
*   **Run-Length Encoding (RLE):** Sequences of identical timestamp deltas or identical float values are compressed efficiently.
*   **Gorilla-style XOR Compression for Float Values:** Float values are compressed using the well-known Gorilla algorithm, which XORs consecutive values and compresses the resulting bit patterns by efficiently encoding leading and trailing zeros.

These techniques are combined to offer potentially better compression, especially for metrics with periods of stability or predictable changes.

## 2. Timestamp Compression

Timestamps are compressed using a combination of adaptive delta-of-delta encoding and run-length encoding.

### 2.1. Adaptive Delta Encoding

1.  **First Timestamp:** The first timestamp in a block is typically written relative to a start time known by both encoder and decoder, or as a full absolute value if no prior reference exists. (Implementation detail: our current encoder uses a `prevTimestamp` initialized at stream start, and the first encoded timestamp's delta is calculated from this).

2.  **Delta Calculation:** For subsequent timestamps, the delta (`current_ts - prev_ts`) is calculated.

3.  **Adaptive Mechanism:**
    *   The number of bits required to represent this delta is determined (e.g., by counting significant bits of the delta value, plus a sign bit).
    *   The encoder maintains an `adaptiveDeltaSize` (in bits). If the current delta requires more bits than `adaptiveDeltaSize`, `adaptiveDeltaSize` is increased. It can also decrease if subsequent deltas are consistently smaller (though current implementation primarily focuses on growing and does not implement shrinking).
    *   A **control bit** (`0`) is written to indicate that the delta is encoded using the current `adaptiveDeltaSize`. (Future extensions might use '1' for other sizing strategies).
    *   A **sign bit** is written ('0' for positive/zero delta, '1' for negative).
    *   The delta value itself is then written using `adaptiveDeltaSize - 1` bits. If `adaptiveDeltaSize` is 1 (for a zero delta), only the sign bit ('0') is written, and the value (0) is implicit.

### 2.2. Run-Length Encoding (RLE) for Timestamps

When a sequence of identical timestamp deltas occurs, RLE is used:

1.  **Actual Delta vs. RLE Marker:**
    *   A **marker bit '0'** indicates that an actual delta value (encoded as described above) follows. This is used for the first occurrence of any delta value.
    *   A **marker bit '1'** indicates an RLE sequence for the *previously encoded delta value*.

2.  **RLE Sequence Encoding:**
    *   If the marker bit is '1':
        *   A **count** of *additional* repetitions is read. This count is typically encoded in a fixed number of bits (e.g., 4 bits).
        *   If 4 bits are used, this can represent 0 to 15 additional repeats. The encoder logic maps this to actual run lengths (e.g., a value of `N` written by the encoder means `N` *additional* occurrences of the previous delta). For example, if the previous delta repeated 2 more times (total run of 3 including the first one written as an actual delta), the count written would be 2.
        *   The decoder then knows to apply the `prevDelta` for `count` more times.

## 3. Value Compression (Float64)

Float64 values are compressed using Gorilla-style XOR compression and run-length encoding.

### 3.1. Gorilla-style XOR Compression

1.  **Bit Conversion:** Float64 values are first converted to their `uint64` bit representation using `math.Float64bits()`.

2.  **XOR Operation:** The `uint64` representation of the current value is XORed with the `uint64` of the previous value (`xor_result = current_value_bits ^ prev_value_bits`).

3.  **Encoding Logic (after a non-RLE marker '0' for value stream):**
    *   **Identical Values:** If `xor_result` is `0` (current value is identical to the previous):
        *   A single bit '0' is written.
    *   **Non-Identical Values:** If `xor_result` is non-zero:
        *   A single bit '1' is written.
        *   The number of leading zeros (`lz`) and trailing zeros (`tz`) of `xor_result` are calculated.
        *   A **control bit** determines how the significant bits of `xor_result` are encoded:
            *   **Control Bit '0' (Reuse Previous Block Structure):**
                *   This is used if the current `lz` and `tz` are greater than or equal to the `prev_lz` and `prev_tz` stored from the last non-zero XOR that used Control Bit '1'. This means the "block" of significant bits from the current `xor_result` fits within the previously defined structure.
                *   The significant bits (`xor_result >> prev_tz`) are written using `64 - prev_lz - prev_tz` bits.
            *   **Control Bit '1' (New Block Structure):**
                *   This is used if the current `lz` and `tz` don't fit the previous structure.
                *   The new `lz` count is written (e.g., using 6 bits).
                *   The length of the meaningful significant bit block (`len_meaningful = 64 - lz - tz`) is written. Typically, `len_meaningful - 1` is stored (e.g., using 6 bits, allowing lengths 1-64).
                *   The significant bits (`xor_result >> tz`) are written using `len_meaningful` bits.
                *   `prev_lz` and `prev_tz` are updated with the current `lz` and `tz`.

### 3.2. Run-Length Encoding (RLE) for Values

When a sequence of identical float values occurs:

1.  **Actual Value vs. RLE Marker:**
    *   A **marker bit '0'** indicates that an actual float value (encoded as described in 3.1) follows. This is used for the first occurrence of any float value.
    *   A **marker bit '1'** indicates an RLE sequence for the *previously encoded float value*.

2.  **RLE Sequence Encoding:**
    *   If the marker bit is '1':
        *   A **count** of *additional* repetitions is read (similar to timestamp RLE, e.g., 4 bits for 1-15 additional repeats).
        *   The decoder applies the `prevFloatValueBits` for `count` more times.

## 4. Stream Structure (Conceptual)

The encoded stream interleaves timestamp and value information for each datapoint.

*   For each datapoint, a timestamp is encoded, followed by its corresponding value.
*   **Timestamp Stream:**
    *   Starts with a marker bit: '0' for actual delta, '1' for RLE of previous delta.
    *   If '0': Further control bits for adaptive sizing, sign bit, then delta bits.
    *   If '1': RLE count bits.
*   **Value Stream:**
    *   Starts with a marker bit: '0' for actual value, '1' for RLE of previous value.
    *   If '0' (actual value):
        *   Another bit: '0' if value same as previous, '1' if different.
        *   If '1' (different): Gorilla control bits, LZ/TZ length bits (if needed), significant bits.
    *   If '1' (RLE): RLE count bits.

This structure ensures that for each point, both time and value are decoded before moving to the next.

## 5. Examples

*(Note: Bit patterns are illustrative and simplified)*

### Example 1: Steady Timestamps, Constant Float Values

*   **Timestamps:** `1000, 1010, 1020, 1030, 1040` (Assume `prev_ts = 990` initially for first delta)
    *   `dp1_ts = 1000`: Actual delta = 10.
        *   Timestamp Stream: `0` (actual) + `0` (adaptive_ctrl) + `0` (sign) + `bits_for_10`
        *   `prev_ts = 1000`, `prev_delta = 10`, `ts_rle_count = 1`
    *   `dp2_ts = 1010`: Actual delta = 10. (delta == `prev_delta`)
        *   `ts_rle_count` becomes 2.
    *   `dp3_ts = 1020`: Actual delta = 10.
        *   `ts_rle_count` becomes 3.
    *   `dp4_ts = 1030`: Actual delta = 10.
        *   `ts_rle_count` becomes 4.
    *   `dp5_ts = 1040`: Actual delta = 10.
        *   `ts_rle_count` becomes 5.
    *   (End of stream or next different delta): Timestamp RLE for delta=10, count=4 (additional) is written.
        *   Timestamp Stream (for dp2-dp5 effectively): `1` (RLE marker) + `bits_for_4_repeats`

*   **Values:** `55.5, 55.5, 55.5, 55.5, 55.5` (Assume `prev_val_bits = 0` initially)
    *   `dp1_val = 55.5`: (bits `B_55.5`)
        *   Value Stream: `0` (actual) + `1` (differs from 0) + `1` (new LZ/TZ) + `lz_bits` + `len_bits` + `sig_bits_of_B_55.5`
        *   `prev_val_bits = B_55.5`, `val_rle_count = 1`
    *   `dp2_val = 55.5`: (bits `B_55.5`) XOR with `prev_val_bits` is 0.
        *   `val_rle_count` becomes 2.
    *   `dp3_val = 55.5`: `val_rle_count` becomes 3.
    *   `dp4_val = 55.5`: `val_rle_count` becomes 4.
    *   `dp5_val = 55.5`: `val_rle_count` becomes 5.
    *   (End of stream or next different value): Value RLE for `B_55.5`, count=4 (additional) is written.
        *   Value Stream (for dp2-dp5 effectively): `1` (RLE marker) + `bits_for_4_repeats`

### Example 2: Timestamps with Varying Deltas, Floats with Small Variations

*   **Timestamps:** `1000, 1015, 1035, 1045, 1085` (Assume `prev_ts = 1000` for first delta calculation for simplicity here, or based on encoder init time)
    *   `dp1_ts = 1000`: Encoded with its delta from initial `prev_ts`. `prev_delta` becomes this delta. `adaptiveDeltaSize` adjusts.
        *   TS: `0` (actual) + `ctrl` + `sign` + `delta_bits_1`
    *   `dp2_ts = 1015`: Delta = 15. `adaptiveDeltaSize` adjusts if needed. `prev_delta = 15`.
        *   TS: `0` (actual) + `ctrl` + `sign` + `delta_bits_for_15`
    *   `dp3_ts = 1035`: Delta = 20. `adaptiveDeltaSize` adjusts. `prev_delta = 20`.
        *   TS: `0` (actual) + `ctrl` + `sign` + `delta_bits_for_20`
    *   `dp4_ts = 1045`: Delta = 10. `adaptiveDeltaSize` may adjust (or not shrink aggressively). `prev_delta = 10`.
        *   TS: `0` (actual) + `ctrl` + `sign` + `delta_bits_for_10`
    *   `dp5_ts = 1085`: Delta = 40. `adaptiveDeltaSize` adjusts. `prev_delta = 40`.
        *   TS: `0` (actual) + `ctrl` + `sign` + `delta_bits_for_40`

*   **Values:** `12.0, 12.1, 12.0, 12.2, 12.3`
    *   `dp1_val = 12.0` (bits `B0`): Encoded fully (or vs initial `prev_val_bits=0`). `prev_val_bits = B0`.
        *   Val: `0` (actual) + `1` (diff) + `1` (new LZ/TZ) + ...
    *   `dp2_val = 12.1` (bits `B1`): XOR `B1^B0`. Small difference. LZ/TZ calculated. `prev_val_bits = B1`.
        *   Val: `0` (actual) + `1` (diff) + `ctrl_for_LZTZ` + ...
    *   `dp3_val = 12.0` (bits `B0`): XOR `B0^B1`. Small difference. `prev_val_bits = B0`.
        *   Val: `0` (actual) + `1` (diff) + `ctrl_for_LZTZ` + ...
    *   `dp4_val = 12.2` (bits `B2`): XOR `B2^B0`. `prev_val_bits = B2`.
        *   Val: `0` (actual) + `1` (diff) + `ctrl_for_LZTZ` + ...
    *   `dp5_val = 12.3` (bits `B3`): XOR `B3^B2`. `prev_val_bits = B3`.
        *   Val: `0` (actual) + `1` (diff) + `ctrl_for_LZTZ` + ...

### Example 3: Mixed RLE and Adaptive Encoding

*   **Timestamps:** `1000, 1010, 1020, 1020, 1020, 1030`
    *   `dp1_ts = 1000`: (Delta D1 from init_prev_ts). Actual: `0 + D1_bits`. `prev_delta = D1`. `ts_rle_count=1`.
    *   `dp2_ts = 1010`: (Delta D2=10). Actual: `0 + D2_bits`. `prev_delta = D2`. `ts_rle_count=1`.
    *   `dp3_ts = 1020`: Delta D3=10 from dp2_ts=1010. The last *actual* delta written was D2 (also 10). Since D3 == D2, the RLE count for delta D2 is incremented to 2. No immediate bitstream write for this specific timestamp's delta yet, as RLE for D2 is pending.
    *   `dp4_ts = 1020`: Delta D4=0 from dp3_ts=1020. This delta (0) is different from the *last actual delta written and RLE-tracked*, which was D2 (10).
        *   Flush RLE for D2(10): `ts_rle_count` for D2 was 2. Write RLE marker '1' + `bits_for_1_repeat` (as D2 was already written once as actual).
        *   Encode current D4(0): Write actual marker '0' + `bits_for_0`. Update `prev_delta = 0`. Set `ts_rle_count = 1` for this new delta (0).
    *   `dp5_ts = 1020`: (Delta D5=0). `prev_delta` is 0. D5==0. `ts_rle_count` becomes 2.
    *   `dp6_ts = 1030`: Delta D6=10 from dp5_ts=1020. This delta (10) is different from the *last actual delta written and RLE-tracked*, which was D4 (0).
        *   Flush RLE for D4(0): `ts_rle_count` for D4 was 2. Write RLE marker '1' + `bits_for_1_repeat`.
        *   Encode current D6(10): Write actual marker '0' + `bits_for_10`. Update `prev_delta = 10`. Set `ts_rle_count = 1`.

*   **Values:** `20.0, 20.0, 21.0, 21.0, 21.0, 22.0`
    *   `dp1_val = 20.0` (B20): Actual: `0 + gorilla_bits_for_B20`. `prev_val_bits = B20`. `val_rle_count=1`.
    *   `dp2_val = 20.0`: `prev_val_bits` is B20. Same. `val_rle_count` becomes 2.
    *   `dp3_val = 21.0` (B21): `prev_val_bits` was B20. Current value B21 is different.
        *   Flush RLE for B20: `val_rle_count` for B20 was 2. Write RLE marker '1' + `bits_for_1_repeat`.
        *   Encode B21: Write actual marker '0' + `gorilla_bits_for_B21_vs_B20`. Update `prev_val_bits = B21`. Set `val_rle_count = 1`.
    *   `dp4_val = 21.0`: `prev_val_bits` is B21. Same. `val_rle_count` becomes 2.
    *   `dp5_val = 21.0`: `prev_val_bits` is B21. Same. `val_rle_count` becomes 3.
    *   `dp6_val = 22.0` (B22): `prev_val_bits` was B21. Current value B22 is different.
        *   Flush RLE for B21: `val_rle_count` for B21 was 3. Write RLE marker '1' + `bits_for_2_repeats`.
        *   Encode B22: Write actual marker '0' + `gorilla_bits_for_B22_vs_B21`. Update `prev_val_bits = B22`. Set `val_rle_count = 1`.

This detailed breakdown should clarify how M3TSZ-Advanced handles different data patterns by combining its core techniques.
