// Copyright(C) 2023 InfiniFlow, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module;
import stl;
import memory_pool;
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <typeinfo>
export module skiplist;

namespace infinity {
const int MAX_LEVEL = 20;

std::random_device rd;
std::mt19937 gen(rd());

export template <typename KeyType, typename ValueType>
class Arena;
class Iterator;

uint64_t encodeValue(uint32_t valOffset, uint32_t valSize) { return uint64_t(valSize) << 32 | uint64_t(valOffset); }

std::pair<uint32_t, uint32_t> decodeVal(uint64_t value) {
    auto valOffset = uint32_t(value);
    auto valSize = uint32_t(value >> 32);
    return {valOffset, valSize};
}

export template <typename KeyType, typename ValueType>
class SkipListNode {
public:
    std::pair<uint32_t, uint32_t> getValueOffset() {
        uint64_t v = value_.load(std::memory_order_relaxed);
        return decodeVal(v);
    }

    const ValueType getValue(Arena<KeyType, ValueType> *arena) {
        auto [valOffset, valSize] = getValueOffset();
        return arena->getVal(valOffset, valSize);
    }

    void setValue(uint64_t valueoffset) { std::atomic_store_explicit(&value_, valueoffset, std::memory_order_relaxed); }

    const KeyType key(Arena<KeyType, ValueType> *arena) { return arena->getKey(keyOffset_, keySize_); }

    uint32_t getNextOffset(int h) { return next_[h].load(std::memory_order_relaxed); }

    bool casNextOffset(int h, uint32_t old, uint32_t val) { return next_[h].compare_exchange_strong(old, val); }

public:
    std::atomic<uint64_t> value_;
    uint32_t keyOffset_;
    uint16_t keySize_;
    std::atomic<uint16_t> height_;
    std::atomic<uint32_t> next_[MAX_LEVEL];
};

export template <typename KeyType, typename ValueType, typename Comparator>
class SkipList {
    typedef SkipListNode<KeyType, ValueType> Node;

public:
    SkipList(Comparator cmp, MemoryPool *arena) : comparator_(cmp), height_(1), ref_(1) {
        arena_ = new Arena<KeyType, ValueType>(10 * 1024 * 1024);
        auto head = newNode(KeyType(), ValueType(), MAX_LEVEL);
        headOffset_ = arena_->getNodeOffset(head);
    }

    ~SkipList() {
        arena_->clear();
        delete arena_;
    }

    bool Insert(const KeyType &key, const ValueType &value) {
        int32_t curh = getHeight();
        uint32_t prev[MAX_LEVEL + 1] = {0};
        uint32_t next[MAX_LEVEL + 1] = {0};
        // update all level's prev and next
        prev[curh] = headOffset_;
        for (int i = int(curh) - 1; i >= 0; i--) {
            auto ret = findSpliceForLevel(key, prev[i + 1], i);
            prev[i] = ret.first;
            next[i] = ret.second;
            if (prev[i] == next[i]) {
                auto valueoffset = arena_->addVal(value);
                auto encVal = encodeValue(valueoffset, sizeof(value));
                auto preNode = arena_->getNode(prev[i]);
                preNode->setValue(encVal);
                return true;
            }
        }
        // create new node
        auto h = RandomLevel();
        auto x = newNode(key, value, h);

        // try update skiplist height_ (CAS)
        curh = getHeight();
        while (h > int(curh)) {
            if (height_.compare_exchange_strong(curh, h)) {
                break;
            }
            curh = getHeight();
        }

        // insert and update skiplist
        for (int i = 0; i < h; i++) {
            while (1) {
                if (arena_->getNode(prev[i]) == nullptr) {
                    assert(i > 1);
                    auto ret = findSpliceForLevel(key, headOffset_, i);
                    prev[i] = ret.first;
                    next[i] = ret.second;
                    assert(prev[i] != next[i]);
                }
                x->next_[i] = next[i];
                auto pnode = arena_->getNode(prev[i]);
                if (pnode->casNextOffset(i, next[i], arena_->getNodeOffset(x))) {
                    break;
                }

                // cas failed
                auto ret = findSpliceForLevel(key, prev[i], i);
                prev[i] = ret.first;
                next[i] = ret.second;

                if (prev[i] == next[i]) {
                    assert(i == 0);
                    auto valueoffset = arena_->addVal(value);
                    auto encVal = encodeValue(valueoffset, sizeof(value));
                    auto preNode = arena_->getNode(prev[i]);
                    preNode->setValue(encVal);
                    return true;
                }
            }
        }
        return true;
    }

    bool Search(const KeyType &key, ValueType &value) {
        auto [n, b] = findNear(key, false, true);
        if (!n) {
            return false;
        }

        auto nextKey = arena_->getKey(n->keyOffset_, n->keySize_);
        if (0 != comparator_(key, nextKey)) {
            return false;
        }

        auto [valOffset, valSize] = n->getValueOffset();
        value = arena_->getVal(valOffset, valSize);
        return true;
    }

    void Clear() {}

    bool Empty() { return findLast() == nullptr; }

    // skiplist iterator
    class Iterator {
    public:
        Iterator(SkipListNode<KeyType, ValueType> *node) : current_(node) {}

        std::pair<KeyType, ValueType> operator*() { return std::make_pair(current_->Key(s->arena_), current_->getValue(s->arena_)); }

        const KeyType Key() { return current_->key(s->arena_); }

        const ValueType Value() { return current_->getValue(s->arena_); }

        bool operator!=(const Iterator &other) const { return current_ != other.current_; }

        Iterator &operator++() {
            if (current_ != nullptr) {
                current_ = s->getNext(current_, 0);
            }
            return *this;
        }

        bool Valid() { return current_ != nullptr; }

    private:
        SkipList *s;
        SkipListNode<KeyType, ValueType> *current_;
    };

    Iterator Begin() { return Iterator(getNext(getHead(), 0)); }

    Iterator Begin(const KeyType &key) {
        auto node = findNear(key, false, true).first;
        return Iterator(node);
    }

    Iterator End() { return Iterator(nullptr); }

    Iterator Prev() {
        auto key = Key(arena_);
        return Iterator(findNear(key, true, false).first);
    }

    Iterator Find(const KeyType &key) { return Iterator(findNear(key, false, true).first); }

    Iterator FindForPrev(const KeyType &key) { return Iterator(findNear(key, true, true).first); }

private:
    Node *newNode(KeyType key, ValueType value, int h) {

        MemNode k = {&key,sizeof(key)};
        MemNode v = {value,sizeof(value)};

        auto nodeOffset = arena_->addNode(h);
        auto keyOffset = arena_->addKey(key);
        auto val = encodeValue(arena_->addVal(value), sizeof(value));

        Node *node = arena_->getNode(nodeOffset);
        node->keyOffset_ = keyOffset;
        node->keySize_ = uint16_t(sizeof(key));
        node->height_ = uint16_t(h);
        node->value_ = val;
        return node;
    }

    int RandomLevel() {
        int new_level = 1;
        std::uniform_real_distribution<> dis(0, INT_MAX);
        while (dis(gen) < INT_MAX / 3 && new_level < MAX_LEVEL) {
            new_level++;
        }
        return new_level;
    }

    Node *getNext(Node *node, int h) { return arena_->getNode(node->getNextOffset(h)); }

    Node *getHead() { return arena_->getNode(headOffset_); }

    int32_t getHeight() { return height_.load(std::memory_order_relaxed); }

    std::pair<Node *, bool> findNear(const KeyType &key, bool less, bool allowEqual) {
        auto x = getHead();
        auto level = int(getHeight() - 1);

        while (1) {
            auto next = getNext(x, level);
            if (next == nullptr) {
                if (level > 0) {
                    level--;
                    continue;
                }

                if (!less) {
                    return std::make_pair(nullptr, false);
                }
                if (x == getHead()) {
                    return std::make_pair(nullptr, false);
                }
                return std::make_pair(x, false);
            }

            auto nextkey = next->key(arena_);
            bool cmp = comparator_(key, nextkey);
            if (cmp > 0) {
                x = next;
                continue;
            } else if (cmp == 0) {
                if (allowEqual) {
                    return std::make_pair(next, true);
                }
                if (!less) {
                    return std::make_pair(getNext(next, 0), false);
                }
                if (level > 0) {
                    level--;
                    continue;
                }

                if (x == getHead())
                    return std::make_pair(nullptr, false);
                return std::make_pair(x, false);

            } else {
                // x.key < key
                if (level > 0) {
                    level--;
                    continue;
                }
                if (!less) {
                    return std::make_pair(next, false);
                }
                if (x == getHead()) {
                    return std::make_pair(nullptr, false);
                }
                return std::make_pair(x, false);
            }
        }
    }

    std::pair<uint32_t, uint32_t> findSpliceForLevel(const KeyType &key, uint32_t before, int level) {
        while (1) {
            auto beforeNode = arena_->getNode(before);
            auto nextOffset = beforeNode->getNextOffset(level);
            auto nextNode = arena_->getNode(nextOffset);

            if (!nextNode) {
                return std::make_pair(before, nextOffset);
            }

            KeyType nextKey = nextNode->key(arena_);
            auto cmp = comparator_(key, nextKey);

            if (cmp == 0) {
                return std::make_pair(nextOffset, nextOffset);
            } else if (cmp < 0) {
                return std::make_pair(before, nextOffset);
            }
            before = nextOffset;
        }
    }

    Node *findLast() {
        auto n = getHead();
        auto level = int(getHeight()) - 1;
        while (1) {
            auto next = getNext(n, level);
            if (next != nullptr) {
                n = next;
                continue;
            }
            if (level == 0) {
                if (n == getHead()) {
                    return nullptr;
                }
                return n;
            }
            level--;
        }
    }

    void IncrRef() { ref_.fetch_add(1); }

    // TODO
    void DecRef() {
        ref_.fetch_sub(1);
        if (ref_ > 0)
            return;
    }

private:
    Comparator const comparator_;
    Arena<KeyType, ValueType> *arena_;
    // cur_height
    std::atomic<int32_t> height_;
    std::atomic<uint32_t> headOffset_;
    std::atomic<int32_t> ref_;

    struct MemNode {
        uint64_t addr_;
        uint64_t len_;
    }
};

static const uint32_t OFFSET_SIZE = sizeof(uint32_t(0));
static const uint32_t NODE_ALIGN = sizeof(uint64_t(0)) - 1;

export template <typename KeyType, typename ValueType>
class Arena {
    typedef SkipListNode<KeyType, ValueType> Node;
    static const uint32_t MAX_NODE_SIZE = sizeof(Node);

public:
    explicit Arena(size_t n) : n_(1) { buf_.resize(n); }

    uint32_t allocate(uint32_t sz) {
        n_.fetch_add(sz);

        if (buf_.size() - n_ < MAX_NODE_SIZE) {
            uint32_t grow = buf_.size();
            if (grow > (1 << 30)) {
                grow = 1 << 30;
            } else if (grow < sz) {
                grow = sz;
            }
            buf_.resize(buf_.size() + grow);
        }
        return n_ - sz;
    }

    uint32_t addNode(int height) {
        size_t len = MAX_NODE_SIZE - ((MAX_LEVEL - height) * OFFSET_SIZE) + NODE_ALIGN;
        uint32_t n = allocate(len);
        uint32_t alignN = (n + NODE_ALIGN) & ~(NODE_ALIGN);
        return alignN;
    }

    uint32_t addVal(const ValueType &v) {
        uint32_t l = sizeof(v);
        uint32_t offset = allocate(l);
        std::memcpy(&buf_[offset], &v, l);
        return offset;
    }

    uint32_t addKey(const KeyType &key) {
        uint32_t l = sizeof(key);
        uint32_t offset = allocate(l);
        std::memcpy(&buf_[offset], &key, l);
        return offset;
    }

    Node *getNode(uint32_t offset) {
        if (offset == 0) {
            return nullptr;
        }
        return reinterpret_cast<Node *>(&buf_[offset]);
    }

    uint32_t getNodeOffset(Node *node) {
        if (node == nullptr) {
            return 0;
        }
        return static_cast<uint32_t>(reinterpret_cast<uintptr_t>(node) - reinterpret_cast<uintptr_t>(&buf_[0]));
    }

    KeyType getKey(uint32_t offset, uint16_t size) {
        // 确保 offset 和 size 合法
        if (offset + size > buf_.size()) {
            return KeyType{};
        }
        if constexpr (std::is_same_v<KeyType, String>) {
            // return static_cast<KeyType>((&buf_[offset]));
            return String(std::string_view(&buf_[offset], size));
        } else {
            KeyType key;
            std::memcpy(&key, &buf_[offset], size);
            return key;
        }
    }

    ValueType getVal(uint32_t offset, uint32_t size) {
        if (offset + size > buf_.size()) {
            return ValueType{};
        }
        if constexpr (std::is_same_v<ValueType, String>) {
            return String(std::string_view(&buf_[offset], size));
            // return static_cast<ValueType>((&buf_[offset]));
        } else {
            ValueType val;
            std::memcpy(&val, &buf_[offset], size);
            return val;
        }
    }

    int64_t size() const { return static_cast<int64_t>(n_.load()); }

    void clear() {
        n_ = 0;
        buf_.resize(0);
    }
    //~Arena() { clear(); }
private:
    std::atomic<uint32_t> n_;
    Vector<char> buf_;
};
} // namespace infinity

// module;

// #include <cassert>
// #include <iostream>
// #include <random>

// export module skiplist;

// import stl;
// import memory_pool;

// namespace infinity {

// const int MAX_LEVEL = 16;

// std::random_device rd;
// std::mt19937 gen(rd());

// template <typename KeyType, typename ValueType>
// struct Node {
//     KeyType key_;
//     ValueType value_;
//     Vector<Atomic<Node<KeyType, ValueType> *>> next_;

//     Node(const KeyType &k, const ValueType &v, int level_) : key_(k), value_(v), next_(level_) {}
// };

// export template <typename KeyType, typename ValueType, class Comparator>
// class SkipList {
// public:
//     SkipList(Comparator cmp, MemoryPool *arena) : level_(1), size_(0), arena_(arena), compare_(cmp) {
//         head_ = (new Node<KeyType, ValueType>(KeyType(), ValueType(), MAX_LEVEL));
//     }

//     ~SkipList() {
//         Clear();
//         delete head_.load();
//     }

//     class Iterator {
//     public:
//         Iterator(Node<KeyType, ValueType> *node) : current_(node) {}

//         Pair<KeyType, ValueType> operator*() const { return MakePair(current_->key_, current_->value_); }

//         const KeyType &Key() { return current_->key_; }

//         const ValueType &Value() { return current_->value_; }

//         bool operator!=(const Iterator &other) const { return current_ != other.current_; }

//         Iterator &operator++() {
//             if (current_ != nullptr) {
//                 current_ = current_->next_[0].load();
//             }
//             return *this;
//         }

//         bool Valid() { return current_ != nullptr; }

//     private:
//         Node<KeyType, ValueType> *current_;
//     };

//     Iterator Begin() { return Iterator(head_.load()->next_[0].load()); }

//     Iterator Begin(const KeyType &key) {
//         Node<KeyType, ValueType> *node = FindGreaterOrEqual(key);
//         return Iterator(node);
//     }

//     Iterator End() { return Iterator(nullptr); }

//     bool Insert(const KeyType &key, const ValueType &value) {
//         Vector<Node<KeyType, ValueType> *> previous(MAX_LEVEL, nullptr);
//         Node<KeyType, ValueType> *current = head_.load();

//         for (int i = level_ - 1; i >= 0; --i) {
//             while (true) {
//                 Node<KeyType, ValueType> *next_node = current->next_[i].load();
//                 if (next_node != nullptr && next_node->key_ < key) {
//                     current = next_node;
//                 } else {
//                     break;
//                 }
//             }

//             previous[i] = current;
//         }

//         Node<KeyType, ValueType> *next_node = current->next_[0].load();

//         if (next_node != nullptr && next_node->key_ == key) {
//             next_node->value_ = value; // Update value_ if key_ already exists
//             return false;
//         }

//         int new_level = RandomLevel();

//         if (new_level > level_) {
//             for (int i = level_; i < new_level; ++i) {
//                 previous[i] = head_.load();
//             }
//             level_ = new_level;
//         }

//         Node<KeyType, ValueType> *new_node = new Node<KeyType, ValueType>(key, value, new_level);
//         for (int i = 0; i < new_level; ++i) {
//             new_node->next_[i] = previous[i]->next_[i].load();
//             previous[i]->next_[i] = new_node;
//         }

//         return true;
//     }

//     bool Search(const KeyType &key, ValueType &value) {
//         Node<KeyType, ValueType> *current = head_.load();

//         for (int i = level_ - 1; i >= 0; --i) {
//             while (true) {
//                 Node<KeyType, ValueType> *next_node = current->next_[i].load();
//                 if (next_node != nullptr && next_node->key_ < key) {
//                     current = next_node;
//                 } else {
//                     break;
//                 }
//             }
//         }

//         Node<KeyType, ValueType> *next_node = current->next_[0].load();

//         if (next_node != nullptr && next_node->key_ == key) {
//             value = next_node->value_;
//             return true;
//         }

//         return false;
//     }

//     Iterator Find(const KeyType &key) {
//         Node<KeyType, ValueType> *x = FindGreaterOrEqual(key);
//         if (x != nullptr && Equal(key, x->key_)) {
//             return Iterator(x);
//         }
//         return Iterator(nullptr);
//     }

//     Node<KeyType, ValueType> *FindGreaterOrEqual(const KeyType &target) {
//         Node<KeyType, ValueType> *x = head_.load();
//         int level = level_ - 1;

//         while (true) {
//             Node<KeyType, ValueType> *next = x->next_[level].load();
//             if (next == nullptr || next->key_ >= target) {
//                 if (level == 0) {
//                     return next;
//                 } else {
//                     // Move one level down
//                     level--;
//                 }
//             } else {
//                 x = next;
//             }
//         }
//         return nullptr;
//     }

//     Node<KeyType, ValueType> *FindLessThan(const KeyType &target) {
//         Node<KeyType, ValueType> *x = head_.load();
//         int level = level_ - 1;

//         while (true) {
//             Node<KeyType, ValueType> *next = x->next_[level].load();
//             if (next == nullptr || next->key_ >= target) {
//                 if (level == 0) {
//                     return x; // Return the node before the node with key >= target
//                 } else {
//                     // Move one level down
//                     level--;
//                 }
//             } else {
//                 x = next;
//             }
//         }
//         return nullptr;
//     }

//     void Clear() {
//         Node<KeyType, ValueType> *current = head_.load()->next_[0].load();
//         while (current != nullptr) {
//             Node<KeyType, ValueType> *next = current->next_[0].load();
//             delete current;
//             current = next;
//         }
//         for (int i = 0; i < level_; ++i) {
//             head_.load()->next_[i] = nullptr;
//         }
//     }

// private:
//     bool Equal(const KeyType &a, const KeyType &b) const { return (compare_(a, b) == 0); }

//     Atomic<int> level_;
//     Atomic<int> size_;
//     Atomic<Node<KeyType, ValueType> *> head_;
//     MemoryPool *arena_{nullptr};
//     Comparator const compare_;

//     // int RandomLevel() {
//     //     int new_level = 1;
//     //     // Why not use (rand() & 1 == 0)
//     //     // 0.5 is the probability
//     //     while (static_cast<double>(rand() / RAND_MAX) < 0.5 && new_level < MAX_LEVEL) {
//     //         new_level++;
//     //     }
//     //     return new_level;
//     // }

//     int RandomLevel() {
//         int new_level = 1;
//         std::uniform_real_distribution<> dis(0, 1);
//         while (dis(gen) < 0.5 && new_level < MAX_LEVEL) {
//             new_level++;
//         }
//         return new_level;
//     }
// };
// } // namespace infinity
