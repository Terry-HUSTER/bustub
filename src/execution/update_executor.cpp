//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 由 scan 类型的 child executor 提供要 update 的 RID
  Tuple old_tuple;
  RID update_rid;
  if (!child_executor_->Next(&old_tuple, &update_rid)) {
    return false;
  }

  // 先 update table
  Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
  bool updated = table_info_->table_->UpdateTuple(new_tuple, update_rid, exec_ctx_->GetTransaction());
  if (!updated) {
    LOG_ERROR("update tuple table %s rid %s fail", table_info_->name_.c_str(), update_rid.ToString().c_str());
    return false;
  }

  // B+ Tree 没有 update，只能先删除再插入
  for (auto *index : indexes_) {
    auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(old_key, update_rid, exec_ctx_->GetTransaction());
    auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->InsertEntry(new_key, update_rid, exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub
