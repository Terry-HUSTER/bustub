//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  max_size_ = max_size;
  page_type_ = IndexPageType::LEAF_PAGE;
  size_ = 0;
  next_page_id_ = INVALID_PAGE_ID; // 有 test case 会检查
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  return BiSearch(key, comparator);
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) { return array[index]; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // LOG_DEBUG("insert leaf %d key %ld size %d max %d", page_id_, key.ToString(), size_, max_size_);
  int id = BiSearch(key, comparator);
  // 本项目 B+ Tree 只支持 unique key，若 page 非空 && key 已存在，直接返回
  if (size_ > 0 && comparator(array[id].first, key) == 0) {
    return size_;
  }
  // 集体后移一位
  std::move_backward(array + id, array + size_, array + size_ + 1);
  array[id] = {key, value};
  size_++;
  return size_;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // B+ Tree 一定是满了才会分裂
  // 如果 size_ > max_size_ 肯定是别的地方代码有问题
  assert(size_ == max_size_);
  int moved = max_size_ - GetMinSize();
  std::move(array + GetMinSize(), array + max_size_, recipient->array);
  recipient->size_ += moved;
  size_ -= moved;
  // 别忘更新两个节点的 next page id
  recipient->next_page_id_ = next_page_id_;
  next_page_id_ = recipient->page_id_;
  // LOG_DEBUG("set next page id: %d -> %d -> %d", page_id_, next_page_id_, recipient->next_page_id_);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // split 时的 copy 是 cover 而不是 append
  std::copy(items, items + size, array);
  size_ = size;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int id = BiSearch(key, comparator);
  if (id >= size_ || comparator(array[id].first, key) != 0) {
    return false;
  } else {
    *value = array[id].second;
    return true;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int id = BiSearch(key, comparator);
  if (id >= size_ || comparator(array[id].first, key) != 0) {
    // 目前失败直接退出
    // LOG_DEBUG("delete leaf page %d key %ld fail, id %d found key %ld size %d", page_id_, key.ToString(), id, array[id].first.ToString(), size_);
    // abort();
    return id;
  } else {
    // 集体前移
    std::move(array + id + 1, array + size_, array + id);
    size_--;
    // LOG_DEBUG("delete leaf page %d key %ld id %d size %d succ, after key %ld", page_id_, key.ToString(), id, size_, array[id].first.ToString());
    return id;
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // 顺序关系 recipent => cur => next
  // 把该 leaf 的数据都 append 到 recipient 里
  std::move(array, array + size_, recipient->array + recipient->size_);
  recipient->size_ += size_;
  size_ = 0;
  // 之后会把当前 page 删除掉，所以 recipient.next_page 指向当前 page 的下一个
  recipient->next_page_id_ = next_page_id_;
  // LOG_DEBUG("set next page %d => %d", recipient->GetPageId(), recipient->GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // 踩坑：offset 写错过
  std::move(array, array + 1, recipient->array + recipient->size_);
  // 集体前移一位
  std::move(array + 1, array + size_, array);
  size_--;
  recipient->size_++;
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[size_] = item;
  size_++;
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // 踩坑：先腾出空间
  std::move_backward(recipient->array, recipient->array + recipient->size_, recipient->array + recipient->size_ + 1);
  recipient->array[0] = array[size_ - 1];
  size_--;
  recipient->size_++;
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  // 集体后移一位
  std::move_backward(array, array + size_, array + size_ + 1);
  array[0] = item;
  size_++;
}

/*
 * 二分查找第一个大于等于 key 的元素，返回其下标
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::BiSearch(const KeyType &key, const KeyComparator &comparator) const {
  int l = 0;
  int r = size_;
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator(array[mid].first, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
