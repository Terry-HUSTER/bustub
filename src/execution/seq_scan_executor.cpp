//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "catalog/catalog.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)  : AbstractExecutor(exec_ctx), plan_(plan) {
	// LOG_DEBUG("construct seq scan");
	// 构造一个指向 table 第一行的 iter
	table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
	Transaction txn(0);
	table_iter_ = std::make_unique<TableIterator>(table_meta_->table_->Begin(&txn));
}

void SeqScanExecutor::Init() {
	// Init 跟构造函数功能是不是重复了？
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
	// seq scan 中每次 next 都要找到下一个符合约束条件的 tuple
	bool ret = true;

	while (true) {
		// 1. 已经到达末尾，直接返回
		bool reach_end = *table_iter_ == table_meta_->table_->End();
		if (reach_end) {
			ret = false;
			break;
		}
		// LOG_DEBUG("seq scan rid: %s val: %s", rid->ToString().c_str(), tuple->GetValue(plan_->OutputSchema(), 0).ToString().c_str());

		*tuple = **table_iter_;
		*rid = (*table_iter_)->GetRid();
		// 无论找没找到，只要没到末尾，iter++
		(*table_iter_)++;
		
		bool evalute = plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
		// 2. 直到找到第一个符合约束条件的 
		if (evalute) {
			ret = true;
			break;
		}
	}
	return ret;
}
}  // namespace bustub
