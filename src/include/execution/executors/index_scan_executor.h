//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/index_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
  // lab 文档中说这些类型都被写死了，可以直接用
  using KeyType = GenericKey<8>;
  using ValueType = RID;
  using KeyComparator = GenericComparator<8>;
  using INDEX_TYPE = BPlusTreeIndex<KeyType, ValueType, KeyComparator>;
  using ITERATOR_TYPE = IndexIterator<KeyType, ValueType, KeyComparator>;

 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  TableMetadata *table_meta_;
  INDEX_TYPE *index_;
  // 智能指针，自动析构
  std::unique_ptr<ITERATOR_TYPE> iter_;
};
}  // namespace bustub
