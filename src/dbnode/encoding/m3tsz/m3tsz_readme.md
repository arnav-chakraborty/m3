# M3TSZ Encoding Scheme (Original)

## 1. Overview

M3TSZ is M3DB's primary time series compression algorithm. It's designed for efficient storage and transmission of time series data, balancing compression ratio with encoding/decoding speed.

The key techniques employed by the original M3TSZ include:

*   **Delta-of-Delta Encoding for Timestamps:** Compresses timestamps by storing the difference from the previous delta, adapting to varying data reporting frequencies.
*   **Integer Optimization for Values:** Attempts to convert float64 values to scaled integers if they represent whole numbers or can be precisely represented as such with a limited multiplier. This allows for more efficient integer compression techniques.
*   **XOR-Based Float Compression:** For float64 values that cannot be optimized as integers, M3TSZ uses XOR-based compression, similar to Gorilla, to store only the differing bits from the previous float value.
*   **Specialized Opcodes:** A system of opcodes is used to signal different states and data types within the compressed stream, minimizing the need for explicit metadata.

## 2. Timestamp Encoding

Timestamp encoding is managed by the `TimestampEncoder` (found in `timestamp_encoder.go` and used by `encoder.go`).

### 2.1. Initial Timestamp and Time Units

*   **Initial Timestamp:** The very first timestamp of a series or block is encoded relative to a `startTime` provided during encoder initialization. The initial delta `(initial_ts - startTime)` is written.
*   **Time Units:** M3TSZ can handle various time units (seconds, milliseconds, microseconds, nanoseconds).
    *   The encoder determines an initial time unit based on the `startTime` and the `DefaultTimeUnit` from encoding options.
    *   If a subsequent timestamp requires a change in time unit for optimal delta encoding, a special **Time Unit Marker** (`defaultTimeUnitMarker`) is written, followed by bits indicating the new time unit. This allows the stream to adapt to changes in reporting precision or magnitude.
    *   The `TimestampEncoder` maintains `prevTimeDelta` and `prevTimeUnit` to track the state for subsequent delta-of-delta calculations.

### 2.2. Delta-of-Delta Encoding (`writeTime` logic)

After the first timestamp, subsequent timestamps are encoded using deltas, and often deltas-of-deltas:

1.  **Calculate Delta:** The current delta is calculated: `delta = current_ts - prev_ts`.
2.  **Handle Same Timestamp:**
    *   If `delta == 0` (timestamp is identical to the previous one), a specific opcode (`opcodeZeroTimestamp`) is written.
3.  **Handle Same Delta:**
    *   If `delta == prev_time_delta` (the rate of change is constant), an opcode (`opcodeSameTimestampDelta`) is written.
4.  **Encode New Delta (Delta-of-Delta):**
    *   If the delta is new, the delta-of-delta (`dod = delta - prev_time_delta`) is calculated.
    *   The `dod` is then encoded using a variable number of bits based on its magnitude, using a set of opcodes to indicate the bit width required (e.g., `opcodeControlSave`, `opcodeControl majorité`, etc., which map to time buckets defined in `TimeEncodingScheme`).
        *   `TimeEncodingScheme` defines buckets (e.g., for deltas fitting in 7, 9, or 12 bits) each with a unique opcode prefix.
        *   A "zero bucket" handles `dod == 0` efficiently.
        *   A "default bucket" handles `dod` values larger than what the predefined buckets cover, typically using more bits.
    *   The actual `dod` value (or `delta` if it's the first after `startTime`) is then written using the number of bits specified by the chosen bucket/opcode.
5.  `prev_ts` and `prev_time_delta` are updated.

### 2.3. Annotation Handling

Annotations can be encoded with timestamps:

*   If an annotation is present and different from the previous annotation (or if it's the first):
    *   An **Annotation Marker** (`defaultAnnotationMarker`) is written.
    *   The length of the annotation (in bytes) is written (e.g., using 16 bits).
    *   The annotation payload (byte slice) is written directly.
*   The checksum of the annotation is stored to detect changes efficiently.

## 3. Value Encoding

Value encoding aims to use integer compression where possible, falling back to float compression.

### 3.1. Integer Optimization (`convertToIntFloat` logic)

*   For each `float64` value, M3TSZ attempts to convert it into a scaled integer.
*   The function `convertToIntFloat(value, prev_max_multiplier)` tries to find if `value * 10^m` is a whole number (or very close, within floating point precision limits) for `m` from `prev_max_multiplier` up to a maximum multiplier (e.g., `maxMult = 6`, so up to `10^6`).
*   If a valid integer representation `i` and multiplier `m` are found such that `value ≈ i / 10^m`, and `i` is within a representable range (e.g., `maxOptInt`), the value is considered integer-optimizable.
*   **Mode Signaling:**
    *   `opcodeIntMode` (`0b0`): Signals the following value (or value diff) is encoded as an integer.
    *   `opcodeFloatMode` (`0b1`): Signals the following value is encoded as a float.

### 3.2. Encoding First Value (`writeFirstValue`)

*   **If Integer Optimized:**
    1.  Write `opcodeIntMode`.
    2.  Store `enc.intVal = scaled_integer_representation`, `enc.maxMult = multiplier_used`.
    3.  Determine if `enc.intVal` is negative.
    4.  Write `writeIntSigMult`:
        *   Number of significant bits for `abs(enc.intVal)` (using opcodes like `opcodeZeroSig`, `opcodeNonZeroSig` followed by bits for length).
        *   Multiplier `enc.maxMult` (using `numMultBits`).
        *   This does *not* use `opcodeUpdateMult` for the first value's multiplier.
    5.  Write the actual significant bits of `abs(enc.intVal)`, preceded by a bit indicating if the original `enc.intVal` was negative (`0` for negative, `1` for positive/zero).
*   **If Float Mode:**
    1.  Write `opcodeFloatMode`.
    2.  Write the full 64 bits of the float value (`enc.floatEnc.writeFullFloat`).
    3.  Set `enc.isFloat = true`.

### 3.3. Encoding Subsequent Values (`writeNextValue`, `writeIntVal`, `writeFloatVal`)

The encoding of subsequent values depends on whether the current value can also be integer-optimized and how it compares to the previous value and its encoding scheme.

*   **`opcodeUpdate` (1 bit) vs `opcodeNoUpdate` (1 bit):** This is a primary control bit.
    *   `opcodeUpdate` (`0b0`): Indicates the encoding scheme (int/float mode, significant bit bucket for int diffs, or multiplier) *might* change, OR the value is repeated.
    *   `opcodeNoUpdate` (`0b1`): Indicates the scheme does *not* change, and the value is different (a new diff or XOR float will follow directly).

*   **If `opcodeUpdate` is written:**
    *   **`opcodeRepeat` (1 bit) vs `opcodeNoRepeat` (1 bit):**
        *   `opcodeRepeat` (`0b0`): The current value is identical to the previous value. No further value bits are written.
        *   `opcodeNoRepeat` (`0b1`): The value is different, and the encoding scheme itself (mode, int sig bits, mult) is being updated.
            *   Another bit for **Mode**: `opcodeIntMode` or `opcodeFloatMode`.
            *   If `opcodeIntMode`: Call `writeIntSigMult` to write new significant bit length for diff and potentially new multiplier, then write integer diff.
            *   If `opcodeFloatMode`: Write full float bits (`enc.floatEnc.writeFullFloat`). This happens when switching from int to float.

*   **If `opcodeNoUpdate` is written (scheme is same, value is different):**
    *   If current mode is **Float Mode**: `enc.floatEnc.writeNextFloat` is called, which XORs the current float's bits with the previous float's bits and writes control bits for leading/trailing zeros and the significant XORed bits (Gorilla-like).
    *   If current mode is **Int Mode**: The difference `valDiff = enc.intVal - current_scaled_int_val` is calculated. `enc.sigTracker.WriteIntValDiff` writes this difference, potentially using the previously established number of significant bits for diffs.

### 3.4. `writeIntSigMult` Details (when `opcodeUpdate` + `opcodeNoRepeat` + `opcodeIntMode`)

This function updates the parameters for encoding integer differences:

1.  **Significant Bits for Diff:** `enc.sigTracker.WriteIntSig` writes the number of significant bits needed for the *current integer difference* (not the full value). This uses opcodes `opcodeZeroSig` (0 bits needed) or `opcodeNonZeroSig` followed by `numSigBitsVal` (6 bits) for the actual length.
2.  **Multiplier Update:**
    *   `opcodeUpdateMult` (`0b1`): If the current value's multiplier (`mult`) is greater than the previous `enc.maxMult`, or if only the float mode changed but other params are same. The new `mult` is written using `numMultBits` (3 bits). `enc.maxMult` is updated.
    *   `opcodeNoUpdateMult` (`0b0`): If the multiplier does not need to be updated.

## 4. Key Opcodes (Summary - Conceptual)

*(This is a non-exhaustive list, based on common patterns in the code)*

*   **Timestamp Opcodes (from `timestamp_encoder.go` logic, often multi-bit prefixes):**
    *   `opcodeZeroTimestamp`: Timestamp is same as previous.
    *   `opcodeSameTimestampDelta`: Timestamp delta is same as previous delta.
    *   Time bucket opcodes (e.g., `0b0`, `0b10`, `0b110`, `0b1110`, `0b1111`): Indicate bit width for new delta-of-delta.
*   **Value Mode Opcodes:**
    *   `opcodeIntMode` (`0b0`): Following value/diff is integer-based.
    *   `opcodeFloatMode` (`0b1`): Following value is float-based.
*   **Value Update Opcodes:**
    *   `opcodeUpdate` (`0b0`): Signals potential change in encoding parameters or a repeated value.
    *   `opcodeNoUpdate` (`0b1`): Signals no change in encoding parameters, new diff/XOR follows.
*   **Value Repeat Opcode (follows `opcodeUpdate`):**
    *   `opcodeRepeat` (`0b0`): Value is identical to previous.
    *   `opcodeNoRepeat` (`0b1`): Value is different, and scheme parameters follow.
*   **Integer Significant Bit Opcodes (for int diffs, from `IntSigBitsTracker`):**
    *   `opcodeZeroSig` (`0b0`): Difference is zero or fits in zero bits (after sign).
    *   `opcodeNonZeroSig` (`0b1`): Difference is non-zero, followed by length of significant bits.
*   **Multiplier Update Opcode (within `writeIntSigMult`):**
    *   `opcodeUpdateMult` (`0b1`): Multiplier for int optimization is changing.
    *   `opcodeNoUpdateMult` (`0b0`): Multiplier is not changing.
*   **Float XOR Opcodes (within `FloatEncoderAndIterator` for `writeNextFloat`):**
    *   Bit '0': XOR is 0 (value same as previous float). (This is after the main `opcodeNoUpdate` path).
    *   Bit '1': XOR is non-zero. Followed by:
        *   Control Bit '0': Use previous XOR block structure.
        *   Control Bit '1': New XOR block structure (new LZ, new length).

## 5. Examples

*(Simplified bitstreams, focusing on logic)*

### Example 1: Integer Optimizable Values

*   Data: `(t1, 10.0), (t2, 11.0), (t3, 11.0)`
    *   Assume `t1, t2, t3` have simple, identical deltas for timestamp part.
    *   `prev_val = 0, prev_max_mult = 0, isFloat = false` initially.

1.  **`(t1, 10.0)`:**
    *   `10.0` -> `intVal = 10`, `mult = 0`.
    *   Value Stream: `0` (IntMode)
        + `writeIntSigMult`:
            *   SigBits for 10 (e.g., 4 bits): `1` (NonZeroSig) + `000011` (4-1)
            *   Mult 0: (No `opcodeUpdateMult` for first val, mult bits directly) `000`
        + `1` (PositiveSign) + `1010` (value 10)
    *   `enc.intVal = 10`, `enc.maxMult = 0`, `enc.isFloat = false`.

2.  **`(t2, 11.0)`:**
    *   `11.0` -> `currInt = 11`, `currMult = 0`.
    *   `valDiff = enc.intVal - currInt = 10 - 11 = -1`. `abs(valDiff) = 1`.
    *   `isFloat` same (false), `currMult` (0) not greater than `enc.maxMult` (0). Sig bits for diff 1 (e.g. 1 bit) might be different from previous full value's sig bits. Assume scheme changes slightly or first diff.
    *   Value Stream: `0` (Update) + `1` (NoRepeat) + `0` (IntMode)
        + `writeIntSigMult`:
            *   SigBits for diff 1 (e.g. 1 bit): `1` (NonZeroSig) + `000000` (1-1)
            *   Mult 0 (no change): `0` (NoUpdateMult)
        + `0` (NegativeSign for diff) + `1` (value 1)
    *   `enc.intVal = 11`.

3.  **`(t3, 11.0)`:**
    *   `11.0` -> `currInt = 11`, `currMult = 0`.
    *   `valDiff = enc.intVal - currInt = 11 - 11 = 0`.
    *   Mode, mult same. Diff is 0.
    *   Value Stream: `0` (Update) + `0` (Repeat)

### Example 2: Pure Float Values (Non-Optimizable)

*   Data: `(t1, 10.123), (t2, 11.456), (t3, 12.789)`
    *   `prev_float_bits = 0, prev_leading_zeros = ^0, prev_trailing_zeros = 0`

1.  **`(t1, 10.123)`:** (bits `B1`)
    *   Cannot be int optimized.
    *   Value Stream: `1` (FloatMode) + `B1` (64 bits for first float)
    *   `enc.isFloat = true`, `enc.floatEnc.PrevFloatBits = B1`.

2.  **`(t2, 11.456)`:** (bits `B2`)
    *   Mode is float. Value is different.
    *   `xor = B2 ^ B1`. Calculate `lz, tz` for `xor`.
    *   Value Stream: `1` (NoUpdate) + `1` (XOR non-zero)
        + `1` (New LZ/TZ block info) + `lz_bits` + `len_meaningful_bits` + `significant_xor_bits`
    *   `enc.floatEnc.PrevFloatBits = B2`, `enc.floatEnc.PrevLeadingZeros = lz`, `enc.floatEnc.PrevTrailingZeros = tz`.

3.  **`(t3, 12.789)`:** (bits `B3`)
    *   Mode is float. Value is different.
    *   `xor = B3 ^ B2`. Calculate `new_lz, new_tz` for this `xor`.
    *   Assume `new_lz, new_tz` fit within `enc.floatEnc.PrevLeadingZeros, PrevTrailingZeros` structure.
    *   Value Stream: `1` (NoUpdate) + `1` (XOR non-zero)
        + `0` (Use prev LZ/TZ block info) + `significant_xor_bits_based_on_prev_structure`
    *   `enc.floatEnc.PrevFloatBits = B3`. (LZ/TZ on floatEnc don't change if control bit is 0).

### Example 3: Mixed Mode (Int changing to Float)

*   Data: `(t1, 25.0), (t2, 25.5)`

1.  **`(t1, 25.0)`:**
    *   `25.0` -> `intVal = 25`, `mult = 0`.
    *   Value Stream: `0` (IntMode) + `writeIntSigMult_for_25_mult_0` + `sign_and_bits_for_25`
    *   `enc.intVal = 25`, `enc.maxMult = 0`, `enc.isFloat = false`.

2.  **`(t2, 25.5)`:**
    *   `25.5` cannot be int optimized with current `enc.maxMult=0` (or even up to `maxMult=6`). So, `isFloat = true`.
    *   This is a mode change from int to float.
    *   Value Stream: `0` (Update) + `1` (NoRepeat) + `1` (FloatMode) + `bits_of_25.5` (64 bits)
    *   `enc.isFloat = true`, `enc.floatEnc.PrevFloatBits = bits_of_25.5`.

This documentation provides a foundational understanding of the original M3TSZ encoding scheme.
