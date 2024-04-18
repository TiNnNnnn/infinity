
import skiplist;
import stl;
import memory_pool;
#include <assert.h>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string.h>
#include <thread>
#include <unordered_map>

using namespace infinity;

std::vector<int> num_list;
void InitRamdomList(int num_elements, int max_value) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(max_value / 2, max_value);

    for (int i = 0; i < num_elements; i++) {
        num_list.push_back(dis(gen));
    }
}

int test_skiplist() {
    MemoryPool *pool = new MemoryPool();
    KeyComparator cmp;

    SkipList<String, String, KeyComparator> skip_list(cmp, pool);

    // 定义要插入的元素数量和范围
    int num_elements = 200000;

    // 记录插入操作的时间
    auto start_insert = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_elements; ++i) {
        String key = std::to_string(num_list[i]);
        skip_list.Insert(key, key); // 插入键值对
    }
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto duration_insert = std::chrono::duration_cast<std::chrono::milliseconds>(end_insert - start_insert);
    std::cout << "skipList插入操作耗时: " << duration_insert.count() << " 毫秒" << std::endl;

    // 记录搜索操作的时间
    auto start_search = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_elements; ++i) {
        String key = std::to_string(num_list[i]);
        String value;
        skip_list.Search(key, value);
        // std::cout<<"key: "<<key<<",value:"<<value<<"size: "<<value.size()<<std::endl;
    }
    auto end_search = std::chrono::high_resolution_clock::now();
    auto duration_search = std::chrono::duration_cast<std::chrono::milliseconds>(end_search - start_search);
    std::cout << "skipList搜索操作耗时: " << duration_search.count() << " 毫秒" << std::endl;

    delete pool;

    return 0;
}

int test_map() {
    std::map<String, String> map;

    // 定义要插入的元素数量和范围
    const int num_elements = 200000;
    // 记录插入操作的时间
    auto start_insert = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_elements; ++i) {
        String key = std::to_string(num_list[i]);
        map[key] = key; // 插入键值对
    }
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto duration_insert = std::chrono::duration_cast<std::chrono::milliseconds>(end_insert - start_insert);
    std::cout << "map 插入操作耗时: " << duration_insert.count() << " 毫秒" << std::endl;

    // 记录搜索操作的时间
    auto start_search = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_elements; ++i) {
        String key = std::to_string(num_list[i]);
        map.find(key);
    }
    auto end_search = std::chrono::high_resolution_clock::now();
    auto duration_search = std::chrono::duration_cast<std::chrono::milliseconds>(end_search - start_search);
    std::cout << "map 搜索操作耗时: " << duration_search.count() << " 毫秒" << std::endl;

    return 0;
}

// 元素数量和范围
const int num_elements = 200000;
const int max_value = 10000000;

void concurrent_skiplist_test(const int &rnum = 3, const int &wnum = 1) {

    // 随机数生成器
    // std::random_device rd;
    // std::mt19937 gen(rd());
    // std::uniform_int_distribution<int> dis(1, max_value);

    KeyComparator cmp;
    SkipList<String, String, KeyComparator> skiplist(cmp, nullptr);

    auto start = std::chrono::high_resolution_clock::now();
    std::thread readers[rnum];

    for (int i = 0; i < rnum; ++i) {
        readers[i] = std::thread([&]() {
            for (int j = 0; j < (num_elements / rnum) + 1; j++) {
                String key = std::to_string(num_list[j] + (num_elements / rnum) * i);
                String value;
                skiplist.Search(key, value);
                assert(key == value)
            }
        });
    }

    std::thread writers[wnum];
    for (int i = 0; i < wnum; ++i) {
        writers[i] = std::thread([&]() {
            for (int i = 0; i < num_elements / wnum; ++i) {
                String key = std::to_string(num_list[j] + (num_elements / wnum) * i);
                skiplist.Insert(key, key);
            }
        });
    }

    for (int i = 0; i < rnum; ++i) {
        readers[i].join();
    }
    for (int i = 0; i < wnum; i++) {
        writers[i].join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_insert = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "skiplist 并发读写操作耗时: " << duration_insert.count() << " 毫秒" << std::endl;
}

// 并发 Map 类
template <typename Key, typename Value>
class ConcurrentMap {
public:
    // 插入元素
    void Insert(const Key &key, const Value &value) {
        std::lock_guard<std::mutex> lock(mutex_);
        map_[key] = value;
    }

    // 查找元素
    bool Search(const Key &key, Value &value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            value = it->second;
            return true;
        }
        return false;
    }

private:
    std::map<Key, Value> map_;
    std::mutex mutex_;
};

void concurrent_map_test(const int &rnum = 3, const int &wnum = 1) {
    // 随机数生成器
    // std::random_device rd;
    // std::mt19937 gen(rd());
    // std::uniform_int_distribution<int> dis(1, max_value);

    ConcurrentMap<String, String> map;

    auto start = std::chrono::high_resolution_clock::now();
    std::thread readers[rnum];

    for (int i = 0; i < rnum; ++i) {
        readers[i] = std::thread([&]() {
            for (int i = 0; i < (num_elements / rnum) + 1; i++) {
                String key = std::to_string(dis(gen));
                String value;
                map.Search(key, value);
            }
        });
    }

    std::thread writers[wnum];
    for (int i = 0; i < wnum; ++i) {
        writers[i] = std::thread([&]() {
            for (int i = 0; i < num_elements / wnum; ++i) {
                String key = std::to_string(dis(gen));
                map.Insert(key, key);
            }
        });
    }

    for (int i = 0; i < rnum; ++i) {
        readers[i].join();
    }
    for (int i = 0; i < wnum; i++) {
        writers[i].join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_insert = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "map 并发读写操作耗时: " << duration_insert.count() << " 毫秒" << std::endl;
}

int main() {
    // InitRamdomList(100000, 10000000);
    InitRamdomList(200000, 100000000);
    concurrent_map_test();
    concurrent_skiplist_test();

    test_skiplist();
    test_map();

    return 0;
}
