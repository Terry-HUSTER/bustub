//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "type/value.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID left_rid, right_rid;
  Tuple right_tuple;
  
  // 第一次执行：为 left tuple 赋予初始值
  if (tuple->GetLength() == 0) {
    if (!left_executor_->Next(&left_tuple, &left_rid)) {
        return false;
    }
  }

  // 跳到第一个符合约束的 left_tuple 和 right_tuple
  do {
    // 到头了吗？
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!left_executor_->Next(&left_tuple, &left_rid)) {
        return false;
      }

      // right 到头就挪到下一行去
      right_executor_->Init();
      right_executor_->Next(&right_tuple, &right_rid);
    }
  } while (plan_->Predicate() != nullptr && !plan_->Predicate()
                                                ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                               &right_tuple, right_executor_->GetOutputSchema())
                                                .GetAs<bool>());

  // 构造 tuple
  std::vector<Value> values;
  for (const auto &col : plan_->OutputSchema()->GetColumns()) {
    auto value = col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                               right_executor_->GetOutputSchema());
    values.emplace_back(std::move(value));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
}

}  // namespace bustub
