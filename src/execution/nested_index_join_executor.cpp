//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "type/value.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

void NestIndexJoinExecutor::Init() { 
  child_executor_->Init();
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), table_info_->name_);
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  // left tuple 由 child node 提供
  // right tuple 由 plan 的 inner table 提供
  do {
    if (!child_executor_->Next(&left_tuple, &left_rid)) {
      return false;
    }

    // 在 index 中找出 left_tuple 对应的 right_tuple

    // 构造一个在 Index 中定位 right_tuple RID 的 key
    Value key_value = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(),
                                                                      &right_tuple, &table_info_->schema_);
    Tuple probe_key({key_value}, index_->index_->GetKeySchema());
    // 找出对应的 RID
    auto bplustree_index =
        dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_->index_.get());
    std::vector<RID> result_set;
    bplustree_index->ScanKey(probe_key, &result_set, exec_ctx_->GetTransaction());
    if (result_set.empty()) {
      return false;
    }
    // 回表找出对应的 tuple，这里不考虑重复 key 的存在
    bool found = table_info_->table_->GetTuple(result_set[0], &right_tuple, exec_ctx_->GetTransaction());
    if (!found) {
      return false;
    }

  } while (plan_->Predicate() != nullptr &&
           !plan_->Predicate()
                ->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_tuple, &table_info_->schema_)
                .GetAs<bool>());

  
  // 根据隔离级加读锁
  exec_ctx_->GetLockManager()->LockRead(exec_ctx_->GetTransaction(), left_tuple.GetRid());
  exec_ctx_->GetLockManager()->LockRead(exec_ctx_->GetTransaction(), right_tuple.GetRid());

  // 构造 tuple
  std::vector<Value> values;
  for (const auto &col : plan_->OutputSchema()->GetColumns()) {
    auto value =
        col.GetExpr()->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_tuple, &table_info_->schema_);
    values.emplace_back(std::move(value));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
}

}  // namespace bustub
