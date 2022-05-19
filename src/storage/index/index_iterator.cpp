/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

// 从来没有写过迭代器，这部分代码照大佬学的
// https://github.com/Sorosliu1029/Database-Systems/blob/master/src/storage/index/index_iterator.cpp

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int idx)
    : buffer_pool_manager_(bpm), page_(page), idx_(idx) {
  leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { buffer_pool_manager_->UnpinPage(page_->GetPageId(), false); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_->GetNextPageId() == INVALID_PAGE_ID && idx_ >= leaf_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(idx_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  // 到达当前 page 尽头，准备迁移到下一个 page
  if (idx_ == leaf_->GetSize() - 1 && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    auto *next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    // LOG_DEBUG("iter goto new page %d", next_page->GetPageId());

    page_ = next_page;
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
    idx_ = 0;
  } else {
    idx_++;
  }
  // LOG_DEBUG("iter page %d idx %d key %ld size %d", leaf_->GetPageId(), idx_, leaf_->GetItem(idx_).first.ToString(), leaf_->GetSize());
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf_->GetPageId() == itr.leaf_->GetPageId() && idx_ == itr.idx_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(leaf_->GetPageId() == itr.leaf_->GetPageId() && idx_ == itr.idx_);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
