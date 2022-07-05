//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/logger.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple delete_tuple;
  RID delete_rid;
  if (!child_executor_->Next(&delete_tuple, &delete_rid)) {
    return false;
  }

  // 加写锁
  exec_ctx_->GetLockManager()->LockWrite(exec_ctx_->GetTransaction(), delete_rid, WType::DELETE);
  // 老套路，先更新 table
  bool deleted = table_info_->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction());
  if (!deleted) {
    LOG_ERROR("delete tuple from table %s rid %s fail", table_info_->name_.c_str(), delete_rid.ToString().c_str());
    return false;
  }

  // 再更新 index
  for (auto *index : indexes_) {
    Tuple delete_key =
        delete_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(delete_key, delete_rid, exec_ctx_->GetTransaction());
    exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(delete_rid, table_info_->oid_, WType::DELETE, delete_tuple, Tuple{}, index->index_oid_, exec_ctx_->GetCatalog());
  }

  return true;
}

}  // namespace bustub
