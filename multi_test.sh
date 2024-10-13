#!/bin/bash
# Usage: ./multi_test.sh <test_runs>
# Runs `cargo test` the amount of times you specify, useful for tests that check for thread safety. After finishing running
# the tests, gives a report of how many times tests failed and how many times they succeeded.

success_count=0
failure_count=0

for i in $(seq 1 $1); do
  echo "Running cargo test (attempt $i)..."
  if cargo test; then
    echo "Cargo test succeeded."
    success_count=$((success_count + 1))
  else
    echo "Cargo test failed."
    failure_count=$((failure_count + 1))
  fi
done

echo "-------------------------"
echo "Cargo test summary:"
echo "Successes: $success_count"
echo "Failures: $failure_count"