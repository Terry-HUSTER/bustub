//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : cap_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);

  if (!lis_.empty()) {
    *frame_id = lis_.back();
    lis_.pop_back();
    mp_.erase(*frame_id);
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);

  if (mp_.find(frame_id) != mp_.end()) {
    lis_.erase(mp_[frame_id]);
    mp_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);

  if (mp_.size() < cap_ && mp_.find(frame_id) == mp_.end()) {
    lis_.push_front(frame_id);
    mp_[frame_id] = lis_.begin();
  }
}

size_t LRUReplacer::Size() { return mp_.size(); }

}  // namespace bustub
