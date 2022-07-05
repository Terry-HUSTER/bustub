//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();

  // 因为类似 MAX、COUNT 等需要一次性统计所有行并在 next 时一次性返回，所以 Init 中保存下所有行
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    // 根据隔离级加读锁
    exec_ctx_->GetLockManager()->LockRead(exec_ctx_->GetTransaction(), rid);
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  std::vector<Value> group_bys;
  std::vector<Value> aggregates;
  // 过滤出所有符合约束的行
  do {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }

    group_bys = aht_iterator_.Key().group_bys_;
    aggregates = aht_iterator_.Val().aggregates_;

    ++aht_iterator_;
  } while (plan_->GetHaving() != nullptr &&
           !plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>());

  // 构造 tuple
  std::vector<Value> values;
  for (const auto& col : plan_->OutputSchema()->GetColumns()) {
    values.emplace_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
}

}  // namespace bustub
