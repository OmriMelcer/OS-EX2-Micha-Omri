#!/usr/bin/env bash
# run_helgrind.sh — build all test*.cpp and run each under valgrind helgrind.
# Detects data races, lock-ordering problems, and misuse of pthreads primitives.
# helgrind exits 99 on any detected error.
# Usage: ./tests/run_helgrind.sh  (run from repo root)

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"
BUILD_DIR="$TESTS_DIR/build"
FRAMEWORK_SRCS="$REPO_ROOT/MapReduceJob.cpp $REPO_ROOT/MapContext.cpp $REPO_ROOT/ReduceContext.cpp"

mkdir -p "$BUILD_DIR"

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
    bin="$BUILD_DIR/${name}.hg"

    if ! g++ --std=c++20 -O1 -g -I"$REPO_ROOT" -o "$bin" "$src" $FRAMEWORK_SRCS 2>/dev/null; then
        printf "%-40s  BUILD-FAIL\n" "$name"
        (( BUILD_FAIL++ )) || true
        continue
    fi

    timeout 120s valgrind --tool=helgrind \
        --error-exitcode=99 --quiet "$bin" > "$BUILD_DIR/${name}.hg.out" 2>&1
    rc=$?

    if [[ $rc -eq 124 ]]; then
        printf "%-40s  TIMEOUT\n" "$name"; (( TIMEOUT++ )) || true
    elif [[ $rc -eq 99 ]]; then
        printf "%-40s  RACE/ERROR (see %s)\n" "$name" "$BUILD_DIR/${name}.hg.out"; (( FAIL++ )) || true
    elif [[ $rc -ne 0 ]]; then
        printf "%-40s  FAIL (exit %s)\n" "$name" "$rc"; (( FAIL++ )) || true
    else
        printf "%-40s  CLEAN\n" "$name"; (( PASS++ )) || true
    fi
done

echo ""
echo "Summary: CLEAN=$PASS  FAIL=$FAIL  TIMEOUT=$TIMEOUT  BUILD-FAIL=$BUILD_FAIL"
