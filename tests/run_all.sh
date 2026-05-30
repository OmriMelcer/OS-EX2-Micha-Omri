#!/usr/bin/env bash
# run_all.sh — build all test*.cpp with -O2 -g and run each under timeout 15s
# Usage: ./tests/run_all.sh  (run from repo root)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"
BUILD_DIR="$TESTS_DIR/build"
FRAMEWORK_SRCS="$REPO_ROOT/MapReduceJob.cpp $REPO_ROOT/MapContext.cpp $REPO_ROOT/ReduceContext.cpp"

mkdir -p "$BUILD_DIR"

# Collect all test source files
mapfile -t TEST_SRCS < <(find "$TESTS_DIR" -maxdepth 1 -name 'test*.cpp' | sort)

if [[ ${#TEST_SRCS[@]} -eq 0 ]]; then
    echo "No test*.cpp files found under $TESTS_DIR"
    exit 0
fi

printf "%-40s  %s\n" "TEST" "RESULT"
printf "%-40s  %s\n" "----" "------"

PASS=0; FAIL=0; TIMEOUT=0; BUILD_FAIL=0

for src in "${TEST_SRCS[@]}"; do
    name="$(basename "$src" .cpp)"
    bin="$BUILD_DIR/$name"
    expected="$TESTS_DIR/${name}.txt"

    # Build
    if ! g++ --std=c++20 -O2 -g -I"$REPO_ROOT" -o "$bin" "$src" $FRAMEWORK_SRCS 2>/dev/null; then
        printf "%-40s  BUILD-FAIL\n" "$name"
        (( BUILD_FAIL++ )) || true
        continue
    fi

    # Run
    if [[ -f "$expected" ]]; then
        # Compare output against expected file
        timeout 15s "$bin" > "$BUILD_DIR/${name}.out" 2>&1
        rc=$?
    else
        timeout 15s "$bin" > "$BUILD_DIR/${name}.out" 2>&1
        rc=$?
    fi

    if [[ $rc -eq 124 ]]; then
        printf "%-40s  TIMEOUT\n" "$name"
        (( TIMEOUT++ )) || true
    elif [[ $rc -ne 0 ]]; then
        printf "%-40s  FAIL (exit $rc)\n" "$name"
        (( FAIL++ )) || true
    else
        # If an expected output file exists, diff against it
        if [[ -f "$expected" ]]; then
            if diff -q "$BUILD_DIR/${name}.out" "$expected" > /dev/null 2>&1; then
                printf "%-40s  PASS\n" "$name"
                (( PASS++ )) || true
            else
                printf "%-40s  FAIL (output mismatch)\n" "$name"
                (( FAIL++ )) || true
            fi
        else
            printf "%-40s  PASS\n" "$name"
            (( PASS++ )) || true
        fi
    fi
done

echo ""
echo "Summary: PASS=$PASS  FAIL=$FAIL  TIMEOUT=$TIMEOUT  BUILD-FAIL=$BUILD_FAIL"