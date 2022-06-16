//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), skipped(0), emitted(0) {}

void LimitExecutor::Init() {
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) { 
  // 只保留 [OFFSET, OFFSET + LIMIT) 范围内的 tuple
  do {
    if (!child_executor_->Next(tuple, rid) || emitted >= plan_->GetLimit()) {
      return false;
    }
    // 目前只能通过 next 一个一个判断，没办法直接跳到 offset
  } while (skipped++ < plan_->GetOffset());
  ++emitted;
  return true;
 }

}  // namespace bustub
