#!/usr/bin/env bash

set -e
cd build

format=false
while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--project)
      project=$2
      shift
      shift
      ;;
    -f|--format)
      format=true
      shift
      ;;
  esac
done

function run() { echo "\$ $@"; $@; }

if [[ $format = true ]]; then
  run make -j8 format
  run make -j8 check-lint
  run make -j8 check-clang-tidy
fi

case $project in
  5)
    run make -j8 grading_lock_manager_1_test
    run make -j8 grading_lock_manager_2_test
    run make -j8 grading_lock_manager_3_test
    run make -j8 grading_lock_manager_detection_test
    run make -j8 grading_rollback_test
    run make -j8 grading_transaction_test
    run ./test/grading_lock_manager_1_test
    run ./test/grading_lock_manager_2_test
    run ./test/grading_lock_manager_3_test
    run ./test/grading_lock_manager_detection_test
    run ./test/grading_rollback_test
    run ./test/grading_transaction_test
  ;;
  4)
    run make -j8 grading_catalog_test
    run make -j8 grading_executor_test
    run ./test/grading_catalog_test
    run ./test/grading_executor_test
  ;;
  3)
    run make -j8 grading_b_plus_tree_checkpoint_2_sequential_test
    run make -j8 grading_b_plus_tree_checkpoint_2_concurrent_test
    run ./test/grading_b_plus_tree_checkpoint_2_sequential_test
    run ./test/grading_b_plus_tree_checkpoint_2_concurrent_test
  ;;
  2)
    run make -j8 grading_b_plus_tree_checkpoint_1_test
    run ./test/grading_b_plus_tree_checkpoint_1_test
  ;;
  1)  
    run make -j8 lru_replacer_test
    run make -j8 grading_clock_replacer_test
    run make -j8 buffer_pool_manager_test
    run make -j8 grading_buffer_pool_manager_test
    run make -j8 grading_buffer_pool_manager_concurrency_test
    run ./test/lru_replacer_test
    run ./test/grading_clock_replacer_test
    run ./test/buffer_pool_manager_test
    run ./test/grading_buffer_pool_manager_test
    run ./test/grading_buffer_pool_manager_concurrency_test
  ;;
  0)
    run make -j8 starter_test
    run ./test/starter_test
  ;;
  *)
    echo "Please input correct project number."
    echo "SYNOPSIS"
    echo "  ./auto-test [-p proj#] [-f]"
    echo ""
    echo "DESCRIPTION: automate test projects"
    echo "  -f, --format"
    echo "      run formats auto-correction and check before testing"
    echo ""
    echo "  -p proj#, --project proj#"
    echo "      test project proj#. Project numbers:"
    echo "        0 - Project#0 C++ Primer"
    echo "        1 - Project#1 Buffer Pool Manager"
    echo "        2 - Project#2 Checkpoint 1"
    echo "        3 - Project#2 Checkpoint 2"
    echo "        4 - Project#3 QueryExecution"
    echo "        5 - Project#4 Concurrency Control"
    exit 1
  ;;
esac
