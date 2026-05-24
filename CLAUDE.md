# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

This is a study project. The primary role of Claude here is to **explain, consult, run code, review code, and debug** — not to write large amounts of code. Small targeted changes (fixing a bug, filling in a stub, adjusting a line) are fine. Avoid writing entire implementations or large new blocks of code unprompted; instead explain the concept and let the student implement it.

## Project

OS Exercise 2 — a C++20 MapReduce framework library (no `main`, no printing). The framework is a black-box multi-threaded library; clients supply only `map` and `reduce` logic.

## Build & Run

There is no Makefile — compile manually with g++ and C++20:

```bash
# Build the sample client (character frequency counter)
g++ --std=c++20 -o sample_client sample_client/SampleClient.cpp MapReduceJob.cpp MapContext.cpp ReduceContext.cpp
./sample_client 4

# Build a test
g++ --std=c++20 -o test01 tests/test01_word_count.cpp MapReduceJob.cpp MapContext.cpp ReduceContext.cpp
./test01

# Verify output against expected
./test01 | diff - tests/test01_word_count.txt

# Run presubmission checker
~os/ex2_presubmit .

# Run against school's reference solution
~os/ex/ex2/run_school_solution sample_client/SampleClient.cpp
```

Check for memory errors:
```bash
valgrind --leak-check=full ./test01
```

## Architecture

The framework is split into two roles:

**Client** (not implemented here — do not modify):
- `MapReduceClient.h` — abstract base with `map(K1,V1,MapContext)` and `reduce(IntermediateVec,ReduceContext)`
- `MapReduceKeys.h` — type aliases: `InputVec`, `IntermediateVec`, `OutputVec`; base classes K1/V1/K2/V2/K3/V3

**Framework** (what you implement):
- `MapReduceJob` — orchestrates all threads; constructor starts work immediately, destructor blocks until done
- `MapContext` — per-thread context passed to `map`; `addIntermediate` appends to that thread's private vector
- `ReduceContext` — single shared context passed to `reduce`; `addOutput` writes to the output vector

### Thread execution model

With `multiThreadLevel=N`:
- N threads are created. Thread 0 is special.
- **Map phase**: all N threads atomically grab input pairs and call `client.map()`; each thread writes to its own `IntermediateVec` (no locking needed between threads).
- **Sort phase**: each thread sorts its own intermediate vector by K2 key.
- **Barrier**: shuffle cannot start until all threads finish sort. Use `std::barrier<>` (C++20).
- **Shuffle phase** (thread 0 only): merges all sorted intermediate vectors into a queue of `IntermediateVec`, one per unique K2 key (by popping from the back of sorted vectors). Threads 1..N-1 wait.
- **Reduce phase**: all N threads atomically pop a group from the queue and call `client.reduce()`.

### Atomic state encoding

`getState()` must be thread-safe and non-blocking. Encode the entire state in a single `std::atomic<uint64_t>`:
- bits 63–62 (2 bits): stage (0=UNDEFINED, 1=MAP, 2=SHUFFLE, 3=REDUCE)
- bits 61–31 (31 bits): total items to process in current stage
- bits 30–0 (31 bits): items processed so far

Atomically set all three fields together when transitioning stages to avoid torn reads.

### Key comparison

Keys only have `operator<`, not `==`. To test equality: `!(a < b) && !(b < a)`.

### wait() safety

`wait()` may be called from multiple threads concurrently. Use a mutex + `joined` flag to ensure `thread::join()` is called exactly once per thread.

## Constraints

- C++20 only; no external libraries; no C-level threading APIs (`pthread_t`, `sem_t`, `mutex_t`)
- No pipes, user-level threads, or forks
- Library must not contain `main` or print anything
- Do **not** modify `MapReduceClient.h` or `MapReduceKeys.h`
- Submit: `MapReduceJob.h/.cpp`, `MapContext.h/.cpp`, `ReduceContext.h/.cpp`, `README`
