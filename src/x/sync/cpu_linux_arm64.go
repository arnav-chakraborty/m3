// Copyright (c) 2023 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:build linux && arm64

package sync

import "golang.org/x/sys/unix"

// getCore returns the currently running CPU core for arm64 linux.
func getCore() int {
	cpu, err := unix.SchedGettcpu()
	if err != nil {
		// Return -1 or some other indicator of error, as the function
		// signature does not allow returning an error.
		// This matches the behavior of some other getCore implementations
		// which might return 0 or an uninitialized value on failure,
		// though -1 is more explicit if callers were to check.
		// However, current usage in index_cpu.go does not check for < 0.
		// It assumes if NumCores is > 1, getCore() is valid.
		// Consider panicking if an error here is truly unexpected and fatal.
		// For now, returning 0 to mimic behavior on unsupported arch/os
		// where it might effectively return an uninitialized/default value.
		return 0
	}
	return cpu
}
