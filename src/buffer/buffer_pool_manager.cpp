//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  assert(page_id != INVALID_PAGE_ID);

  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    // found in pool
    frame_id_t frame_id = page_table_[page_id];
    PinPageByFrameId(frame_id);
    return &pages_[frame_id];
  } else {
    // no free frame in pool, find a victim
    frame_id_t frame_id;
    if (!AllocateFrame(&frame_id)) {
      return nullptr;
    }

    ResetPageMetadata(&pages_[frame_id]);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].page_id_ = page_id;
    PinPageByFrameId(frame_id);

    page_table_[page_id] = frame_id;
    return &pages_[frame_id];
  }
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  assert(page_id != INVALID_PAGE_ID);

  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ |= is_dirty;  // use 'or'
  bool ret = UnpinPageByFrameId(frame_id);
  return ret;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);

  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  // force flush regard dirty_flag, a test case will check this.
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame_id;
  if (!AllocateFrame(&frame_id)) {
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();

  pages_[frame_id].ResetMemory();
  ResetPageMetadata(&pages_[frame_id]);
  pages_[frame_id].page_id_ = *page_id;
  PinPageByFrameId(frame_id);

  page_table_[*page_id] = frame_id;
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  assert(page_id != INVALID_PAGE_ID);

  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }

  ResetPageMetadata(&pages_[frame_id]);

  page_table_.erase(page_id);
  disk_manager_->DeallocatePage(page_id);

  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> guard(latch_);

  for (auto &iter : page_table_) {
    disk_manager_->WritePage(iter.first, pages_[iter.second].GetData());
    pages_[iter.second].is_dirty_ = false;
  }
}

bool BufferPoolManager::AllocateFrame(frame_id_t *frame_id) {
  if (free_list_.empty()) {
    if (!replacer_->Victim(frame_id)) {
      return false;
    }
    Page &victim_page = pages_[*frame_id];
    if (victim_page.IsDirty()) {
      disk_manager_->WritePage(victim_page.GetPageId(), victim_page.GetData());
      victim_page.is_dirty_ = false;
    }
    page_table_.erase(victim_page.GetPageId());
  } else {
    *frame_id = free_list_.front();
    free_list_.pop_front();
  }
  ResetPageMetadata(&pages_[*frame_id]);
  return true;
}

bool BufferPoolManager::PinPageByFrameId(frame_id_t frame_id) {
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->Pin(frame_id);
  }
  pages_[frame_id].pin_count_++;
  return true;
}

bool BufferPoolManager::UnpinPageByFrameId(frame_id_t frame_id) {
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

void BufferPoolManager::ResetPageMetadata(Page *page) {
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
}

}  // namespace bustub
