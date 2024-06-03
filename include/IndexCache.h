#if !defined(_INDEX_CACHE_H_)
#define _INDEX_CACHE_H_

#include "CacheEntry.h"
#include "HugePageAlloc.h"
#include "Timer.h"
#include "third_party/inlineskiplist.h"

#include <atomic>
#include <vector>

extern bool enter_debug;

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

class IndexCache {

public:
  IndexCache(int cache_size);

  bool add_to_cache(InternalPage *page);
  const CacheEntry *search_from_cache(const Key &k, GlobalAddress *addr);

  void search_range_from_cache(const Key &from, const Key &to,
                               std::vector<InternalPage *> &result);

  bool add_entry(const Key &from, const Key &to, InternalPage *ptr);
  const CacheEntry *find_entry(const Key &k);
  const CacheEntry *find_entry(const Key &from, const Key &to);

  bool invalidate(const CacheEntry *entry);

  const CacheEntry *get_a_random_entry(uint64_t &freq);

  void statistics();

  void bench();

private:
  uint64_t cache_size; // MB;
  std::atomic<int64_t> free_page_cnt;
  std::atomic<int64_t> skiplist_node_cnt;
  int64_t all_page_cnt;
  std::mutex mutex_pool[1000];
  // SkipList
  CacheSkipList *skiplist;
  CacheEntryComparator cmp;
  Allocator alloc;

  void evict_one();
};

inline IndexCache::IndexCache(int cache_size) : cache_size(cache_size) {
  skiplist = new CacheSkipList(cmp, &alloc, 21);
  uint64_t memory_size = define::MB * cache_size;

  all_page_cnt = memory_size / sizeof(InternalPage);  //将cacheSize均等分，形成一个个的区块
  free_page_cnt.store(all_page_cnt);
  skiplist_node_cnt.store(0); //后续在使用过程中，每分配一个叶作为缓存，总叶数量就递增，然后空闲节点数量就递减，如果没有空闲节点则利用驱逐算法，驱逐一个叶，然后再进行缓存
}

// [from, to）
inline bool IndexCache::add_entry(const Key &from, const Key &to,
                                  InternalPage *ptr) {

  // TODO memory leak
  auto buf = skiplist->AllocateKey(sizeof(CacheEntry));
  auto &e = *(CacheEntry *)buf;
  e.from = from;
  // since the range for every node is [lowest, highest), and the the skip list will
  // find the first key that larger than or equal to the searched key. However, if the key is equal to the highest,
  // it is not included in this node, so the "to" should be "to - 1".
  e.to = to - 1; // !IMPORTANT;   
  //这里说明机制是一个内部节点的hdr.low - hdr.high范围内
  e.ptr = ptr;

  return skiplist->InsertConcurrently(buf);
}

inline const CacheEntry *IndexCache::find_entry(const Key &from,
                                                const Key &to){
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.from = from;
  e.to = to - 1;
  //THe cmp is CacheEntryComparator in CacheEntry.h
  iter.Seek((char *)&e);//在seek函数里，利用node_来接收返回值，然后在iter.Valid函数里依靠node_不为空来判断是否为有效
  if (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    // while (val->ptr == nullptr) {
    //   iter.Next();
    //   if (!iter.Valid()) {
    //     return nullptr;
    //   }
    //   val = (const CacheEntry *)iter.key();
    // }
    return val;//这里的val是CacheEntry *
  } else {
    return nullptr;
  }
}

inline const CacheEntry *IndexCache::find_entry(const Key &k) {
  return find_entry(k, k + 1);
}

inline bool IndexCache::add_to_cache(InternalPage *page) {
  auto new_page = (InternalPage *)malloc(kInternalPageSize);//在这一步可以看出来，indexcache是自己独有内存分配器和内存空间
  //IndexCache并不是threadLocal变量，因此其被各个thread共享
  //按照htop命令的检测情况来看，在warm_key插入结束后，CN端内存需求就不再增长，这表明这一阶段是indexcache自带的分配器在起作用
  memcpy(new_page, page, kInternalPageSize);  //是将page_buffer里的内容复制到新创建的InternalPage里，由于采用memcpy，使得原本的内容被覆盖，只需要改index_cache_freq这个统计量就可以
  new_page->index_cache_freq = 0;

  if (this->add_entry(page->hdr.lowest, page->hdr.highest, new_page)) { 
    skiplist_node_cnt.fetch_add(1);  //在这里给其计数
    //The code below make the cache bounded by the cache size.
    auto v = free_page_cnt.fetch_add(-1);
    //free_page_cnt的大小为预设的memory_size/sizeof(Leafpage)，也就是预设的最大cache_size大小约束
    //v如果小于0，表明应有的内存空间已经被用完了，因此需要进行驱逐，而实际上的空间则十分庞大
    if (v <= 0) {
      evict_one();
    }

    return true;
  } else { // conflicted
    auto e = this->find_entry(page->hdr.lowest, page->hdr.highest);
    if (e && e->from == page->hdr.lowest && e->to == page->hdr.highest - 1) {
      auto ptr = e->ptr;
      if (ptr == nullptr &&
          __sync_bool_compare_and_swap(&(e->ptr), 0ull, new_page)) {
        // if (enter_debug) {
        //   page->verbose_debug();
        // }
        auto v = free_page_cnt.fetch_add(-1);
        if (v <= 0) {
          evict_one();
        }
        return true;
      }
    }

    free(new_page);
    return false;
  }
}

inline const CacheEntry *IndexCache::search_from_cache(const Key &k,
                                                       GlobalAddress *addr){
  auto entry = find_entry(k);  //先在缓存类cacheskiplist里进行搜索，搜到了返回cacheentry，搜不到返回nullptr
  //entry是一个cache_entry。其内容包括 from为缓存的level_1节点的hdr.low，to为缓存的hdr.high,internal_page *缓存的是内部节点的副本
  //find_entry在跳表里找到了的话，就是返回的，缓存时的内部节点状态，而若未找到的话，返回的就是nullptr

  // the entry->ptr here should be an atomic variable.
  InternalPage *page = entry ? entry->ptr : nullptr;  //InternalPage是cacheentry的一个成员，如果搜索到了就返回这个，否则page即为空

  if (page && entry->from <= k && entry->to >= k) {// this track will definitely happen
      //from和to应该标记了当前内部节点的范围，上述判断情况，说明page在缓存中被找到，而且key符合条件
      //但是看了后续k < page->records[0].key，又让人怀疑这个到底啥意思。先留着吧，后续再看
      std::unique_lock<std::mutex> lck(mutex_pool[(uint64_t)(page)%1000]);
      //在这里，lck是一个独占锁，针对mutex对象。然后这里是利用mutex_pool来完成利用page_addr进行独占锁定，防止后续其他线程对这个内容的修改
      //这里的并发访问控制逻辑是，当其他线程要对某个page进行修改，但是被映射到mutex_pool的相同位置，进行lck的时候就不能获取锁，从而进入阻塞
      //那么mutex_pool的大小不会对并发造成影响吗？因为不仅是相同page，不同page也可能被映射到同一块mutex_pool的位置？
      //从而保证并发访问的一致性
      if (entry->ptr != page)  //这个是啥情况呢？后续再看看是啥吧 
      //这里是因为，index_cache是被tree共享的，而且又是并发结构，所以这里找到的entry也可能被修改掉，所以在这里再确认下，page没有被修改
      //仍然是找到它时候的状态
      //前者如果在上
          return nullptr;
    // if (enter_debug) {
    //   page->verbose_debug();
    // }

    page->index_cache_freq++;  //命中后就递增

    auto cnt = page->hdr.last_index + 1;  //在作者说明中，last_index在叶子和内部节点初始都是-1，根节点初始为0
    //这个last_index有可能是当前节点最后一个kv的位置。有可能，暂时不确定，等看到了再回过头确认
    //确认，last_index是当前节点最后一个被使用的slots的位置，因为使用了unsorted_node布局
    if (k < page->records[0].key) {
      *addr = page->hdr.leftmost_ptr;//如果要查找的key比这个page的最小key还小，就去到该节点的左节点进行查找
    } else {

      bool find = false;
      for (int i = 1; i < cnt; ++i) {  //i从record_1开始比较，直到最后一个record，因为cnt是最后一个位置的+1，所以i最后能取得一位是cnt-1的record
        if (k < page->records[i].key) {  //最后一个要比较的recored_(cnt-1)，如果小于它，那么就去往它的cnt-2位置的addr指向的内容
          find = true;
          *addr = page->records[i - 1].ptr; //如果在范围内，就返回该记录指向的指针的地址
          break;
        }
      }
      if (!find) {
          // addr is the target leaf node that may contain the key.
        *addr = page->records[cnt - 1].ptr;    //如果一直没找到就去往最后一个record指示的。因为如果找到了，最后能去往的是cnt-2的addr；如果没找到则说明可能是在cnt-1位置处往后的节点了
      }
    }

    compiler_barrier();//确保它的调用点之前和之后的代码不会被重排序或优化
    if (entry->ptr) { // check if it is freed.
      // printf("Cache HIt\n");
      return entry;  //如果entry是存在的，在这个时候就返回它，但是具体要查找的下一个位置已经被addr所记录了
    }
  }

  return nullptr;
}

inline void
IndexCache::search_range_from_cache(const Key &from, const Key &to,
                                    std::vector<InternalPage *> &result) {
  CacheSkipList::Iterator iter(skiplist);

  result.clear();
  CacheEntry e;
  e.from = from;
  e.to = from;
  iter.Seek((char *)&e);

  while (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    if (val->ptr) {
      if (val->from > to) {
        return;
      }
      result.push_back(val->ptr);
    }
    iter.Next();
  }
}

inline bool IndexCache::invalidate(const CacheEntry *entry) {
  auto ptr = entry->ptr;

  if (ptr == nullptr) {
    return false;
  }

  if (__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)) {
      std::unique_lock<std::mutex> lk(mutex_pool[(uint64_t)(ptr)%1000]);
      //TODO: REMOVE ENTRY FROM THE SKIP LIST.
    free(ptr);
//    free_page_cnt.fetch_add(1);
      if (free_page_cnt.fetch_add(1)%100000 == 0){
          printf("Cache is still invalidated\n");
          statistics();
      }

    return true;
  }

  return false;
}

inline const CacheEntry *IndexCache::get_a_random_entry(uint64_t &freq) {
  uint32_t seed = asm_rdtsc();
  GlobalAddress tmp_addr;
retry:
  auto k = rand_r(&seed) % (1000ull * define::MB);
  auto e = this->search_from_cache(k, &tmp_addr);
  if (!e) {
    goto retry;
  }
  auto ptr = e->ptr;
  if (!ptr) {
    goto retry;
  }

  freq = ptr->index_cache_freq;
  if (e->ptr != ptr) {
    goto retry;
  }
  return e;
}

inline void IndexCache::evict_one() {  //index_cache驱逐的方式是随机驱逐

  uint64_t freq1, freq2;
  auto e1 = get_a_random_entry(freq1);
  auto e2 = get_a_random_entry(freq2);

  if (freq1 < freq2) {
    invalidate(e1);
  } else {
    invalidate(e2);
  }
}

inline void IndexCache::statistics() {
  printf("[skiplist node: %ld]  [page cache: %ld]\n", skiplist_node_cnt.load(),
         all_page_cnt - free_page_cnt.load());
}

inline void IndexCache::bench() { //这个似乎是用来进行一些信息统计的

  Timer t;
  t.begin();
  const int loop = 100000;

  for (int i = 0; i < loop; ++i) {
    uint64_t r = rand() % (5 * define::MB);
    this->find_entry(r);
  }

  t.end_print(loop);
}

#endif // _INDEX_CACHE_H_
