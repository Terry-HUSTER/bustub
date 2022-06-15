//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include <string>
#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // plan_ 里只存了 index_oid，为了根据 RID 访问 tuple 得反向求出 table_oid
  std::string table_name = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_;
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(table_name);
}

void IndexScanExecutor::Init() {
  auto index_meta = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  // 父类转子类
  index_ = dynamic_cast<INDEX_TYPE *>(index_meta->index_.get());
  iter_ = std::make_unique<ITERATOR_TYPE>(index_->GetBeginIterator());
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  bool ret = true;

  while (true) {
    bool reach_end = *iter_.get() == index_->GetEndIterator();
    if (reach_end) {
      ret = false;
      break;
    }

    *rid = (**iter_).second;
    bool found = table_meta_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    if (!found) {
      LOG_ERROR("not found record rid %s", rid->ToString().c_str());
      abort();
    }
    // LOG_DEBUG("found tuple %s rid %s", tuple->ToString(plan_->OutputSchema()).c_str(), rid->ToString().c_str());
    ++(*iter_);

    bool evaluate = plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
    if (evaluate) {
      // 根据 output schema 过滤 tuple
      std::vector<Value> values;
      auto schema = plan_->OutputSchema();
      for (const auto &col : schema->GetColumns()) {
        values.emplace_back(tuple->GetValue(schema, schema->GetColIdx(col.GetName())));
      }
      *tuple = Tuple(values, schema);
      ret = true;
      break;
    }
  }
  return ret;
}

}  // namespace bustub
