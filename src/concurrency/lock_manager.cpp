//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // RU 隔离级，读操作不需要也不可以加锁
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // RR 隔离级，2PL 收缩时不能加锁
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    // RC 隔离级不需要检查 SHRINKING
    // 相比起 RR，RC 不采用 2PL 协议，读完一行就可以立刻释放锁，等再次读取时再加锁。
    // 所以两次读操作间可以被别的事务插入写操作，故存在不可重复读问题。
  }

  // LOG_DEBUG("LOCK SHARED %d\n", txn->GetTransactionId());

  // RID 包含递归锁直接返回 true
  // 因为每个 txn 都是单线程的，所以触发递归锁一定证明前一个锁已经被被解开了
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  latch_.lock();
  auto &lock_req_queue = lock_table_[rid];
  latch_.unlock();
  // 先加锁，再排队
  std::unique_lock<std::mutex> queue_lock(lock_req_queue.mutex_);
  auto lock_mode = LockManager::LockMode::SHARED;
  auto &lock_req = lock_req_queue.request_queue_.emplace_back(txn->GetTransactionId(), lock_mode);
  // 很神奇，直接 CanGrantLock 不行，但包成 lambda 就可以了
  lock_req_queue.cv_.wait(queue_lock, [&lock_req_queue, &lock_mode, &txn] {
    return CanGrantLock(lock_req_queue, lock_mode, txn->GetTransactionId());
  });

  // 加锁成功
  lock_req.granted_ = true;
  // 没加 W 锁，则加 R 锁
  if (!txn->IsExclusiveLocked(rid)) {
    txn->GetSharedLockSet()->emplace(rid);
  }

  // LOG_DEBUG("LOCK txn %d rid %s", txn->GetTransactionId(), rid.ToString().c_str());

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  txn->GetExclusiveLockSet()->emplace(rid);

  // 先只处理 RR
  assert(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ);

  LOG_DEBUG("LOCK EXCLUSIVE %s", rid.ToString().c_str());
  abort();
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  // 先只处理 RR
  assert(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ);

  LOG_DEBUG("LOCK UPGRADE %s", rid.ToString().c_str());
  abort();

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 先只处理 RR
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // 只有 RR 遵循 2PL，需要状态转移
    if (txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  // 这里不加锁会发生诡异的重复删除问题
  latch_.lock();
  auto &lock_req_queue = lock_table_[rid];
  latch_.unlock();

  // 先加锁，再出队
  std::unique_lock<std::mutex> queue_lock(lock_req_queue.mutex_);
  auto iter = std::find_if(lock_req_queue.request_queue_.begin(), lock_req_queue.request_queue_.end(),
                           [&txn](const auto &req_iter) { return txn->GetTransactionId() == req_iter.txn_id_; });
  // if (iter == lock_req_queue.request_queue_.end()) {
  //   std::stringstream ss;
  //   ss << "rid " << rid.ToString() << "txn: " << txn->GetTransactionId() << " lock queue: ";
  //   for (auto &req : lock_req_queue.request_queue_) {
  //     ss << req.txn_id_ << " ";
  //   }
  //   LOG_DEBUG("%s\n", ss.str().c_str());
  // }
  // 理论上必定找得到
  assert(iter != lock_req_queue.request_queue_.end());

  // 删除，顺便取出下个 lock_req
  auto next_iter = lock_req_queue.request_queue_.erase(iter);
  // 如果下个 lock 可以释放，则通知下大伙起来检测下
  if (next_iter != lock_req_queue.request_queue_.end() && !next_iter->granted_ &&
      LockManager::CanGrantLock(lock_req_queue, next_iter->lock_mode_, next_iter->txn_id_)) {
    lock_req_queue.cv_.notify_all();
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // LOG_DEBUG("UNLOCK txn %d rid %s ", txn->GetTransactionId(), rid.ToString().c_str());
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
