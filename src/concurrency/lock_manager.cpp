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
#include "concurrency/transaction_manager.h"

#include <algorithm>
#include <mutex>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 各个隔离级下加锁解锁的时机参阅 Wikipedia 的“事务隔离”页面
  // https://zh.m.wikipedia.org/zh-hans/%E4%BA%8B%E5%8B%99%E9%9A%94%E9%9B%A2
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
    return CanGrantLock(lock_req_queue, lock_mode, txn->GetTransactionId()) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  // 获得锁后发现事务已经崩了，全部木大
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

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
  // 理由同 LockShared
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  // 理由同 LockShared
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  latch_.lock();
  auto &lock_req_queue = lock_table_[rid];
  latch_.unlock();

  std::unique_lock<std::mutex> queue_lock(lock_req_queue.mutex_);
  auto lock_mode = LockManager::LockMode::EXCLUSIVE;
  auto &lock_req = lock_req_queue.request_queue_.emplace_back(txn->GetTransactionId(), lock_mode);
  lock_req_queue.cv_.wait(queue_lock, [&lock_req_queue, &lock_mode, &txn] {
    return CanGrantLock(lock_req_queue, lock_mode, txn->GetTransactionId()) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  // 获得锁后发现事务已经崩了，全部木大
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  // 加锁成功
  lock_req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // upgrade 就是将 R 锁转化为为 W 锁的过程
  // 理由同 LockShared
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  latch_.lock();
  auto &lock_req_queue = lock_table_[rid];
  latch_.unlock();

  std::unique_lock<std::mutex> queue_lock(lock_req_queue.mutex_);
  if (lock_req_queue.upgrading_) {
    // 一次只能并发升级一把锁，有别的锁也在升级就 abort
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  lock_req_queue.upgrading_ = true;
  auto iter = std::find_if(lock_req_queue.request_queue_.begin(), lock_req_queue.request_queue_.end(),
                           [&txn](const auto &lock_req) { return txn->GetTransactionId() == lock_req.txn_id_; });

  // 先把锁升级，然后拿 W 锁
  auto lock_mode = LockMode::EXCLUSIVE;
  iter->lock_mode_ = lock_mode;
  iter->granted_ = false;
  lock_req_queue.cv_.wait(queue_lock, [&lock_req_queue, &lock_mode, &txn] {
    return CanGrantLock(lock_req_queue, lock_mode, txn->GetTransactionId()) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  // 上锁成功后取消升级标记
  iter->granted_ = true;
  lock_req_queue.upgrading_ = false;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
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

void LockManager::LockRead(Transaction *txn, const RID &rid) {
  // LOG_DEBUG("lock read start %d rid %lu", txn->GetTransactionId(), rid.Get());
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      // RU 读不加锁
      break;
    case IsolationLevel::READ_COMMITTED:
      // RC 读完就可以释放锁
      LockShared(txn, rid);
      Unlock(txn, rid);
      break;
    case IsolationLevel::REPEATABLE_READ:
      // RR 遵循 2PL，先加锁，最后一块儿释放锁
      LockShared(txn, rid);
      break;
  }
  // LOG_DEBUG("lock read end %d, rid %lu", txn->GetTransactionId(), rid.Get());
}

void LockManager::LockWrite(Transaction *txn, const RID &rid, WType wtype) {
  // LOG_DEBUG("lock write start %d rid %lu", txn->GetTransactionId(), rid.Get());
  if (txn->IsSharedLocked(rid)) {
    LockUpgrade(txn, rid);
  } else if (!txn->IsExclusiveLocked(rid)) {
    LockExclusive(txn, rid);
  }
  // LOG_DEBUG("lock write end %d rid %lu", txn->GetTransactionId(), rid.Get());
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::set<txn_id_t> visited;
  std::set<txn_id_t> muilt_graph_visited;
  // 可能有多个连通图，所以要每个没走过的节点做根节点测一遍 DFS
  for (const auto &kv : waits_for_) {
    auto start_txn_id = kv.first;
    if (muilt_graph_visited.find(start_txn_id) == muilt_graph_visited.end()) {
      bool found = DFSCheckCycle(start_txn_id, visited, muilt_graph_visited, *txn_id);
      if (found) {
        return true;
      }
    }
  }
  return false;
}

bool LockManager::DFSCheckCycle(txn_id_t txn_id, std::set<txn_id_t> &visited, std::set<txn_id_t> &muilt_graph_visited,
                                txn_id_t &found_txn_id) {
  // LOG_DEBUG("visited txn id %d, wait size: %lu", txn_id, waits_for_[txn_id].size());
  // 沿着最小事务 ID 遍历，但是遇到环时返回最大的事务 ID
  for (auto to_txn_id : waits_for_[txn_id]) {
    if (visited.find(to_txn_id) != visited.end()) {
      // found
      found_txn_id = *std::prev(visited.end());
      // LOG_DEBUG("found cycle, txn: %d", found_txn_id);
      return true;
    } else {
      // not found, dfs
      visited.insert(to_txn_id);
      muilt_graph_visited.insert(to_txn_id);
      if (DFSCheckCycle(to_txn_id, visited, muilt_graph_visited, found_txn_id)) {
        return true;
      }
      visited.erase(to_txn_id);
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &kv : waits_for_) {
    auto from = kv.first;
    for (auto to : kv.second) {
      edges.emplace_back(from, to);
    }
  }
  return edges;
}

void LockManager::RebuildWaitsForGraph() {
  waits_for_.clear();
  for (const auto &it : lock_table_) {
    const auto queue = it.second.request_queue_;
    std::vector<txn_id_t> granted;
    std::vector<txn_id_t> waiting;
    for (const auto &lock_req : queue) {
      const auto txn = TransactionManager::GetTransaction(lock_req.txn_id_);
      // 忽略 abort
      if (txn->GetState() == TransactionState::ABORTED) {
        continue;
      }

      if (lock_req.granted_) {
        granted.push_back(lock_req.txn_id_);
      } else {
        waiting.push_back(lock_req.txn_id_);
      }
    }
    for (auto from : waiting) {
      for (auto to : granted) {
        AddEdge(from, to);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // 每 50ms 一次，取得全局锁，像 java GC 一样 stop the world
      std::unique_lock<std::mutex> l(latch_);
      if (!enable_cycle_detection_) {
        break;
      }

      // 构建等待图
      RebuildWaitsForGraph();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        // 破坏等待环
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        // 让等待的线程都动起来
        for (const auto wait_on_txn_id : waits_for_[txn_id]) {
          auto wait_on_txn = TransactionManager::GetTransaction(wait_on_txn_id);
          std::unordered_set<RID> lock_set;
          lock_set.insert(wait_on_txn->GetSharedLockSet()->begin(), wait_on_txn->GetSharedLockSet()->end());
          lock_set.insert(wait_on_txn->GetExclusiveLockSet()->begin(), wait_on_txn->GetExclusiveLockSet()->end());
          for (auto locked_rid : lock_set) {
            lock_table_[locked_rid].cv_.notify_all();
          }
        }

        RebuildWaitsForGraph();
      }
      continue;
    }
  }
}

}  // namespace bustub
