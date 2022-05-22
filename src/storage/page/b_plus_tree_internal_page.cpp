//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  max_size_ = max_size;
  page_type_ = IndexPageType::INTERNAL_PAGE;
  size_ = 0;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // value 无序排列，只能顺序查找
  for (int i = 0; i < size_; i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return size_;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int id = BiSearch(key, comparator);
  // Internal 的每个元素的 child.key >= parent.key，所以此处需 -1
  if (id >= size_ || comparator(array[id].first, key) > 0) {
    return array[id - 1].second;
  } else {
    // 只有等于时返回
    return array[id].second;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // 上溢递归到了根节点，根节点也满了，分裂后得到两个子节点和一个包含两个 key 的新根节点
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  size_ = 2;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * 在当前节点分裂，上溢到父节点时用到
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int id = ValueIndex(old_value) + 1;
  // 集体后移一位
  std::move_backward(array + id, array + size_, array + size_ + 1);
  array[id] = {new_key, new_value};
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 只有刚好满的节点才会分裂
  assert(size_ == max_size_);
  int moved = max_size_ - GetMinSize();
  // 名义上 array[0].key 保留，但这里实际上发生分裂时也会拷贝过去
  std::move(array + GetMinSize(), array + size_, recipient->array + recipient->size_);
  recipient->BatchChangeChildParentId(recipient->size_, recipient->size_ + moved, buffer_pool_manager);
  size_ -= moved;
  recipient->size_ += moved;
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // split 时该 leaf node append 所有的 item
  std::copy(items, items + size, array + size_);
  // update every parent_id
  BatchChangeChildParentId(size_, size_ + size, buffer_pool_manager);
  size_ += size;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // 集体前移一位
  // LOG_DEBUG("internal page %d remove key %ld value %d size %d", page_id_, array[index].first.ToString(), (int)array[index].second, size_);
  std::move(array + index + 1, array + size_, array + index);
  size_--;
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // 删除唯一的子节点，然后返回
  assert(size_ == 1);
  size_ = 0;
  return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;
  std::move(array, array + size_, recipient->array + recipient->size_);
  recipient->BatchChangeChildParentId(recipient->size_, recipient->size_ + size_, buffer_pool_manager);
  recipient->size_ += size_;
  size_ = 0;
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  recipient->array[recipient->size_] = {middle_key, array[0].second};
  // 集体前移一位
  std::move(array + 1, array + size_, array);
  recipient->BatchChangeChildParentId(recipient->size_, recipient->size_ + 1, buffer_pool_manager);
  size_--;
  recipient->size_++;
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // 集体后移一位
  std::move_backward(recipient->array, recipient->array + recipient->size_, recipient->array + recipient->size_ + 1);
  //TODO: 这里似乎 middle_key == array[size_-1].first
  recipient->array[0] = {middle_key, array[size_ - 1].second};
  recipient->BatchChangeChildParentId(0, 1, buffer_pool_manager);
  recipient->size_++;
  size_--;
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * 二分查找第一个大于等于 key 的元素，返回其下标
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::BiSearch(const KeyType &key, const KeyComparator &comparator) const {
  int l = 1;  // internal page 保留 0 号 key
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

/*
 * 将 [start, end) 内的 child page 的 parent id 设为当前 page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BatchChangeChildParentId(int start, int end,
                                                              BufferPoolManager *buffer_pool_manager) {
  for (int i = start; i < end; i++) {
    Page *page = buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
