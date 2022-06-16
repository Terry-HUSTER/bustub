//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/logger.h"
#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      // unique_ptr 特有的所有权转移方式
      child_executor_(std::move(child_executor)),
      insert_id_(0) {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  // 先对 child executor 做初始化
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  // 有的 test case 会在 plan 创建好后再创建索引，所以 index 要在 init 阶段获取，不能在构造函数中获取
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  RID tmp_rid;

  if (plan_->IsRawInsert()) {
    // case 1: tuple 直接由 plan 提供
    if (insert_id_ >= plan_->RawValues().size()) {
      // 全部插入完毕
      return false;
    }
    // 构造 tuple 需要指定 value 和每列的名字
    tmp_tuple = Tuple(plan_->RawValuesAt(insert_id_), &(table_meta_->schema_));
    insert_id_++;
  } else {
    // case 2: tuple 由 child executor next 提供
    if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      return false;
    }
  }

  // LOG_DEBUG("begin insert");

  // 先插入 table
  // LOG_DEBUG("insert tuple to table %s", table_meta_->name_.c_str());
  bool inserted = table_meta_->table_->InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction());
  if (!inserted) {
    LOG_ERROR("insert tuple to table failed");
    return false;
  }

  // 再插入每个关联的 index，类似 Catalog::CreateIndex
  for (auto *index : indexes_) {
    // key_schema: 类似 table 的 schema，但 index 只有一列 key，所以 schema 里只有一个 key 的元信息
    // key_attrs: key 的特殊标志信息
    // LOG_DEBUG("insert tuple to index %s", index->name_.c_str());
    auto key_tuple = tmp_tuple.KeyFromTuple(table_meta_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->InsertEntry(key_tuple, tmp_rid, exec_ctx_->GetTransaction());
  }

  return true;
}

}  // namespace bustub
