#include "Tree.h"
#include "HotBuffer.h"
#include "IndexCache.h"
#include "RdmaBuffer.h"
#include "Timer.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>

bool enter_debug = false;

HotBuffer hot_buf;  //hot_buf似乎没有被使用
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
volatile bool need_stop = false;

thread_local CoroCall Tree::worker[define::kMaxCoro];
thread_local CoroCall Tree::master;
thread_local GlobalAddress path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];

// for coroutine schedule
struct CoroDeadline {
  uint64_t deadline;
  uint16_t coro_id;

  bool operator<(const CoroDeadline &o) const {
    return this->deadline < o.deadline;
  }
};

thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;
thread_local std::priority_queue<CoroDeadline> deadline_queue;

  Tree::Tree(DSM *dsm, uint16_t tree_id) : dsm(dsm), tree_id(tree_id){

  for (int i = 0; i < dsm->getClusterSize(); ++i) {
    local_locks[i] = new LocalLockNode[define::kNumOfLock];//这个i是针对集群内的MN端节点的，每个local_lock[i]都指向一个数组，数组大小为KNumOfLock，每个元素大小为uint64_t
    //其中，KNumOfLock大小为片上内存大小除以uint64的大小，这表明为每个节点分配一个local_lock_table，锁表项大小为KNumOfLock
    //kNumOfLock = kLockChipMemSize / sizeof(uint64_t);
    //这里，ClusterSize为MN的数量，而为每个MN节点分配同样片上内存大小的本地锁表
    for (size_t k = 0; k < define::kNumOfLock; ++k) {
      auto &n = local_locks[i][k];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }//并且对本地锁表进行初始化
  }   //完成本地锁表的注册和初始化工作

  assert(dsm->is_register());  //这里的appID设置为atomic变量，可能是考虑到后续thread_run时，每个thread_id这样的本地线程在取线程号的争用吧
  //至于为什么不能用Thread_connection的ID来赋予，是因为，这里是一个先赋予id号，其后再取用id的过程
  //DSM自身分出多个线程的时候，需要用一个一致的方式来获取id号，从而再获取thread_connection
  print_verbose(); //可视化一些配置信息，打印出来

  index_cache = new IndexCache(define::kIndexCacheSize);//初始化indexCache，稍后查看这个有什么
  //这个也说明，indexcache是和具体的treeIndex来的，而非属于threadConnection
   

  root_ptr_ptr = get_root_ptr_ptr();//这里root_ptr_ptr就是nodeID:0,offset:KChunksize/2的位置
  //指向根节点的指针被设为了MN_0的DSM_Pool的16MB偏移量位置，why??
  //也许是为了能够在算法层面达成一致的操作结果
  
  // try to init tree and install root pointer
  auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer(); 
  //这是thread_local的，代表thread_0的rbuf[0],这个0指的是coro_0，获得了coro_0的第1个(而不是第0个)page_buffer区域
  auto root_addr = dsm->alloc(kLeafPageSize);
  //本端请求远端内存，远端内存按照划分的chunk来进行一块块的内存分配
  //返回的内存其实是旧的cur指针指向的地址，但是新的cur指针将偏移一个KLeafPageSize大小
  //LeafPageSize是一个2KB大小的空间
  //由于DSM-pool的首块chunk已经被分配，当再请求的时候，分配得到的区块是第二块chunk

  auto root_page = new (page_buffer) LeafPage;//是一种operator new的内存分配方法，在侯捷的C++内存分配里有讲过
  //这里是将Page_buffer区域实例化为一个LeafPage，同时root_page是指向这块内存的指针
  root_page->set_consistent();//将首尾版本递增
  dsm->write_sync(page_buffer, root_addr, kLeafPageSize);  
  //将page_buffer实例化为root_page，填充好首位version，然后写入远端，先前分配的root_addr
  //page_buffer此时就是root_leaf_page的内容，而root_addr是一个Global_Address，包含了其位置的指向
  //因此这里最终是将page_buffer的内容写到了由root_addr指示的位置处

  auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
  bool res = dsm->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);  //以cas原子写的方式，将root_addr的value写入root指针的指针
  //所以实际上，这里的root_ptr_ptr就是root_leaf的指针
  //单元测试结束，这里root_ptr_ptr记录的确实是root_leaf的指针，换句话说，root_ptr返回的Global_address是一个uint64_t类型的结构
  //其union结构既可以被解释为offset和nodeID，也可以被解释为val，而写入两个变量nodeID和offset显然更加麻烦，所以采用val一次性写入

  //将source位置，也即cas_buffer区间里的内容，写入由root_ptr_ptr指定的位置
  //这里是将root_addr.val写入，然后由于val与之前的struct共享内存地址，是union，这里我猜测，使用unoin结构体囊括起来
  //是为了后续在写入两个变量的时候，用一个变量名来写，随后如果取出val并赋给另一个gaddr，这个gaddr依旧能够从中解析nodeID和offset

  //但是可以肯定的是，root_addr指的是root的指针
  if (res){
    std::cout << "Tree root pointer value " << root_addr << std::endl;
  }else{
     std::cout << "fail\n";
  }
  //所以关于root_addr和root_ptr_ptr之间的关系，就是，root_addr上记录的是真正的root_inner_node
  //然后以val的形式将root_addr的内容写入到由root_ptr_ptr规定的地址上，以val的形式可以使其以原子写的方式进行修改
  //root_addr的记录位置在root_ptr_ptr，也就是dsm_pool开始的第一个Chunk的中间地址，也就是偏移过来16MB的地址处
}

void Tree::print_verbose() {

  int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  assert(kLeafHdrOffset == kInternalHdrOffset);

  if (dsm->getMyNodeID() == 0) {   
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
  }
}

inline void Tree::before_operation(CoroContext *cxt, int coro_id) {  //这里虽然.h的函数原型也没指定默认coro_id，但是在tree->insert里指定了默认的coro_id为0.那么这里为啥没有并发冲突出现呢？
//因为path_stack是thread_local的，因此避免了多线程之间的并发冲突  
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = GlobalAddress::Null();//这里path_stack是想记录下访问路径，以便后续在进行叶节点分裂时，能记录下访问路径
    //从而便于回访上一层，对上一层进行级联式的SMO操作
  }
}

GlobalAddress Tree::get_root_ptr_ptr(){//这个指向根节点的指针的指针，是啥意思呢？
  GlobalAddress addr;
  addr.nodeID = 0;  //认为在MN集群中，MMN_node0为主节点，存储fine-grained的多树的根节点的指示位置
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;
      //这里是16MB+0*sizeof(GlobalAddress)
      //这里KrootPointerStoreOffset设置为KChunkSize/2，按照多颗树的情况来分，其中chunksize是在MN端进行区分的一个概念

  return addr;
}

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Tree::get_root_ptr(CoroContext *cxt, int coro_id){

  if (g_root_ptr == GlobalAddress::Null()){
    auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    dsm->read_sync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress), cxt);
    //这里再一次说明，root_ptr_ptr并不是指向根节点指针的指针，而是就是指向根节点的指针
    //这里root_ptr_ptr指向的是nodeID为0，offset为KChunkSize/2的地方
    GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;  //所以这里为何不用cas_buffer?但最终这里是浪费了一个leafPage的空间来取一个8B的数字
    std::cout << "Get new root" << root_ptr <<std::endl;
    g_root_ptr = root_ptr;  //因为root_ptr_ptr处记录了一个8B的根节点指针，所以root_ptr取到以后就是根节点的指针，即 0:offset的形式，且该处为根节点位置
    return root_ptr;
  }else{
    return g_root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
}

void Tree::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
  RawMessage m;
  m.type = RpcType::NEW_ROOT;
  m.addr = new_root_addr;
  m.level = root_level;
  if (root_level >= 5) {
        enable_cache = true;
  }
  //TODO: When we seperate the compute from the memory, how can we broad cast the new root
  // or can we wait until the compute node detect an inconsistent.
  for (int i = 0; i < dsm->getClusterSize(); ++i) {
    dsm->rpc_call_dir(m, i);
  }
}

bool Tree::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {

  auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
  auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();
    assert(left != GlobalAddress::Null());
    assert(right != GlobalAddress::Null());
  auto new_root = new (page_buffer) InternalPage(left, k, right, level);

  auto new_root_addr = dsm->alloc(kInternalPageSize);
  // The code below is just for debugging
//    new_root_addr.mark = 3;
  new_root->set_consistent();
  // set local cache for root address
  g_root_ptr = new_root_addr;
  dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize, cxt);
  if (dsm->cas_sync(root_ptr_ptr, old_root, new_root_addr, cas_buffer, cxt)) {
    broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

  return false;
}

void Tree::print_and_check_tree(CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  // SearchResult result;

  GlobalAddress p = root;
  GlobalAddress levels[define::kMaxLevelOfTree];
  int level_cnt = 0;
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  GlobalAddress leaf_head;

next_level:

  dsm->read_sync(page_buffer, p, kLeafPageSize);
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  levels[level_cnt++] = p;
  if (header->level != 0) {
    p = header->leftmost_ptr;
    goto next_level;
  } else {
    leaf_head = p;
  }

next:
  dsm->read_sync(page_buffer, leaf_head, kLeafPageSize);
  auto page = (LeafPage *)page_buffer;
  for (int i = 0; i < kLeafCardinality; ++i) {
    if (page->records[i].value != kValueNull) {
    }
  }
  while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
    leaf_head = page->hdr.sibling_ptr;
    goto next;
  }

  // for (int i = 0; i < level_cnt; ++i) {
  //   dsm->read_sync(page_buffer, levels[i], kLeafPageSize);
  //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //   // std::cout << "addr: " << levels[i] << " ";
  //   // header->debug();
  //   // std::cout << " | ";
  //   while (header->sibling_ptr != GlobalAddress::Null()) {
  //     dsm->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
  //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //     // std::cout << "addr: " << header->sibling_ptr << " ";
  //     // header->debug();
  //     // std::cout << " | ";
  //   }
  //   // std::cout << "\n------------------------------------" << std::endl;
  //   // std::cout << "------------------------------------" << std::endl;
  // }
}

GlobalAddress Tree::query_cache(const Key &k) { return GlobalAddress::Null(); }

inline bool Tree::try_lock_addr(GlobalAddress lock_addr, uint64_t tag,
                                uint64_t *buf, CoroContext *cxt, int coro_id) {
  auto &pattern_cnt = pattern[dsm->getMyThreadID()][lock_addr.nodeID];

  bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  if (hand_over) {
    return true;
  }

  {

    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 3000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << dsm->getMyNodeID() << ", " << dsm->getMyThreadID()
                << " locked by " << (conflict_tag >> 32) << ", "
                << (conflict_tag << 32 >> 32) << std::endl;
      assert(false);
      exit(0);
    }

    bool res = dsm->cas_dm_sync(lock_addr, 0, tag, buf, cxt);
//      std::cout << "lock address " << lock_addr << std::endl;
    pattern_cnt++;
    if (!res) {
      conflict_tag = *buf - 1;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      lock_fail[dsm->getMyThreadID()][0]++;
      goto retry;
    }
  }

  return true;
}

inline void Tree::unlock_addr(GlobalAddress lock_addr, uint64_t tag,
                              uint64_t *buf, CoroContext *cxt, int coro_id,
                              bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  auto cas_buf = dsm->get_rbuf(coro_id).get_cas_buffer();
//    std::cout << "unlock " << lock_addr << std::endl;
  *cas_buf = 0;
  if (async) {
    dsm->write_dm((char *)cas_buf, lock_addr, sizeof(uint64_t), false);
  } else {
    dsm->write_dm_sync((char *)cas_buf, lock_addr, sizeof(uint64_t), cxt);
  }

  releases_local_lock(lock_addr);
}

void Tree::write_page_and_unlock(char *page_buffer, GlobalAddress page_addr,
                                 int page_size, uint64_t *cas_buffer,
                                 GlobalAddress lock_addr, uint64_t tag,
                                 CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    dsm->write_sync(page_buffer, page_addr, page_size, cxt);
    releases_local_lock(lock_addr);
    return;
  }

  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)page_buffer;
  rs[0].dest = page_addr;
  rs[0].size = page_size;
  rs[0].is_lock_mr = false;

  rs[1].source = (uint64_t)dsm->get_rbuf(coro_id).get_cas_buffer();
  rs[1].dest = lock_addr;
  rs[1].size = sizeof(uint64_t);

  rs[1].is_lock_mr = true;

  *(uint64_t *)rs[1].source = 0;
  if (async) {
    dsm->write_batch(rs, 2, false);
  } else {
    dsm->write_batch_sync(rs, 2, cxt);
  }

  releases_local_lock(lock_addr);
}

void Tree::lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                              int page_size, uint64_t *cas_buffer,
                              GlobalAddress lock_addr, uint64_t tag,
                              CoroContext *cxt, int coro_id){

  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);  //详细内容有点复杂，具体想做的事情应该是按照锁表，锁住远端页面
  //同时还要考虑本地获取的锁是否能够被移交

  dsm->read_sync(page_buffer, page_addr, page_size, cxt); //锁住后再进行按照page-addr的读取，放入本地rbuf的page_buffer里
  pattern[dsm->getMyThreadID()][page_addr.nodeID]++;//这个应该是给统计量用的
}

void Tree::lock_bench(const Key &k, CoroContext *cxt, int coro_id) {
  uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = 0;
  lock_addr.offset = lock_index * sizeof(uint64_t);
  auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  // bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer, cxt);
  try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
  unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
}
// You need to make sure it is not the root level
// why there is no lock coupling?
void Tree::insert_internal(const Key &k, GlobalAddress v, CoroContext *cxt,
                           int coro_id, int level) {
  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
    //TODO: ADD support for root invalidate and update.
next:

  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.slibing != GlobalAddress::Null()) {
    p = result.slibing;
    goto next;
  }

  p = result.next_level;
  if (result.level != level + 1) {
    goto next;
  }

  internal_page_store(p, k, v, root, level, cxt, coro_id);
}

void Tree::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id){  
  //这里CoroCintext在Tree.h中被默认初始化为nullptr，且coro_id被默认初始化为0
  assert(dsm->is_register());

  before_operation(cxt, coro_id);  
  //benchmark里每次调用insert都默认coroID为0，因此这里，每次的insert，在操作之前，
  //都会先将path_stack[0][1-maxlevel]清空为NULL
  //这里的max_level设置了上限7，但实际构建如果tree_index很大，7层应该是不够用的，所以后续可能也需要改

//  auto res = hot_buf.set(k);
//
//  if (res == HotResult::OCCUPIED) {
//    hot_filter_count[dsm->getMyThreadID()][0]++;
//    if (cxt == nullptr) {
//      while (!hot_buf.wait(k))
//        ;
//    } else {
//      hot_wait_queue.push(coro_id);
//      (*cxt->yield)(*cxt->master);
//    }
//  }

  if (enable_cache) {
    GlobalAddress cache_addr;
    //The cache here is a skip list mapping from key[from, to) to a cached internal node. when searching the
    // only when the from and to are both equal the entry in the skip list, then there will be cache hit. This is actually
    // a tuple level cache, which may not has a large cache locality.
    auto entry = index_cache->search_from_cache(k, &cache_addr);
    //这里的entry是跳表里记录的 <key_low,key_high>:InternalPage的item，为并发结构。时刻有可能被修改
    //然后这里的entry如果不为nullptr的话，表明是找到了的。所以cache_addr此时存的就是应该要被访问的leaf_node
    if (entry) { // cache hit  表明此时，index_cache是找到了相关entry的
      auto root = get_root_ptr(cxt, coro_id);  //首先拿根节点的位置
      // this will by pass the page search.
      // there will be a potentially bug for concurrent root update
      //cache_addr记录了key可能位于的叶节点位置，或者也可能是内部节点的意思。到这里表明，缓存项被搜到，因此直接利用缓存项的结果来进行查找
      if (leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true)) {    
        //如果在index_cache里找到了leaf_node那么就直接对这个leaf_page进行插入。
        //至于往leafPage里进行插入会发生什么的处理，就要继续再细读了

        //leaf_page_store返回false有可能是因为缓存失效
        //level_0的意思是叶节点;从叶节点到root节点，level依次递增,level最大的就是root节点了
        //这里讨论了待插入的节点找到后，发现要往兄弟节点去的递归操作；以及当插入节点满时进行的节点分裂操作，先写分裂节点再写自己
        //以及级联的SMO传播和传播到根节点的update_new_root的情况




        cache_hit[dsm->getMyThreadID()][0]++;
//          printf("Cache hit\n");
//        if (res == HotResult::SUCC) {
//          hot_buf.clear(k);
//        }

        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry);  //这里前者如果leaf_page_store返回false，是因为缓存失效，不然它会一直执行下去
      //缓存没用了就要被丢弃，所以被invalidate()来进行修改
      
//        invalid_counter[dsm->getMyThreadID()][1]++;
//        if(invalid_counter[dsm->getMyThreadID()][1] % 5000 == 0){
//            printf("Invalidate cache 1\n");
//        }
//        printf("Invalidate cache\n");
    }
    cache_miss[dsm->getMyThreadID()][0]++;  //记录下这次的缓存失效
    //因此缓存失效会在两种情况下统计，index-cache里不存在，和利用index_cache结果进行leaf_page_store失败。
    //不存在误统计的情况。程序打印的cache_hit ratio有效
    //但是为何在缓存充足的情况下，缓存命中率那么高呢？难道是XSTORE在骗人？？？
    //也有可能是因为统计基地的选取，没有选择RTT遍历过程中的，但是0.9+的cache_hit也表明RTT访问并不是那么多的。真奇怪了
  }
  //在缓存失效以及不允许缓存的情况下，需要自己手动从根节点开始search
  auto root = get_root_ptr(cxt, coro_id);
  //这里如果g_root为空，则会从远端root_ptr_ptr处取8B的root_addr并返回，赋给g_root；否则直接返回g_root
//  std::cout << "The root now is " << root << std::endl;
  SearchResult result;
  GlobalAddress p = root;
  // this is root is to help the tree to refresh the root node because the
  // new root broadcast is not usable if physical disaggregated.
  bool isroot = true;
//The page_search will be executed mulitple times if the result is not is_leaf
next:

  if (!page_search(p, k, result, cxt, coro_id, false, isroot)){

    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }
  isroot = false;
//The page_search will be executed mulitple times if the result is not is_leaf
// Maybe it will goes to the sibling pointer or go to the children
  if (!result.is_leaf) {  //这里的if分支表示找到的result不是叶节点，如果是叶节点，直接进行leaf_page_store就可以了
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {//如果存在sibling节点
      p = result.slibing;  //这种情况下要去往sibling节点，并开始搜索叶节点，那么，这又是什么情况呢？
      goto next;
    }

    p = result.next_level;  //如果不是往sibling节点去，就继续去往下一层
//    printf("next level pointer is %p\n", p);
    if (result.level != 1) {  //如果未抵达level_1的话，就继续从下一层的叶节点开始继续搜索迭代，否则就对下一层的节点进行搜索
      goto next;//这是因为，如果result已经在level_1了，result的下一层就是level_0了，就直接是叶节点了，因此直接对叶节点进行插入操作即可
      //简单来说就是result-level为1，那么result.next_level就是0，所以这个情况下p就是leaf_node了
    }
  }
  //p为leaf_node就尝试进行leaf_page_store；否则就重复RTT遍历tree_index的行为
  leaf_page_store(p, k, v, root, 0, cxt, coro_id);//这个是在找到的leaf_page里进行搜索

//  if (res == HotResult::SUCC) {
//    hot_buf.clear(k);
//  }
}

bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
  bool isroot = true;
  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (enable_cache) {
    GlobalAddress cache_addr;
    entry = index_cache->search_from_cache(k, &cache_addr); //如果成功搜索到了，则返回entry是一个有效指针，否则是nullptr，同时有效的情况下cache_addr是有内容的
    if (entry) { // cache hit
      cache_hit[dsm->getMyThreadID()][0]++;
      from_cache = true;
      p = cache_addr;
      isroot = false;
    } else {
      cache_miss[dsm->getMyThreadID()][0]++;
    }
  }

next:
  if (!page_search(p, k, result, cxt, coro_id, from_cache, isroot)) {
    if (from_cache) { // cache stale
      index_cache->invalidate(entry);
      // Comment it during the test.
//        invalid_counter[dsm->getMyThreadID()][0]++;
//        if(invalid_counter[dsm->getMyThreadID()][0] % 5000 == 0){
//            printf("Invalidate cache 0\n");
//        }
        //The cache hit is the real cache hit counting the invalidation in
      cache_hit[dsm->getMyThreadID()][0]--;
      cache_miss[dsm->getMyThreadID()][0]++;
      from_cache = false;

      p = root;
      isroot = true;
    } else {
      std::cout << "SEARCH WARNING search" << std::endl;
      sleep(1);
    }
    goto next;
  }
  else{
      isroot = false;
  }

  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;

      return true;
    }
    if (result.slibing != GlobalAddress::Null()){ //turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found,当为根节点且仅有根节点的时候，且范围超界，会走到这里
  } else {        // internal
    p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                : result.next_level; 
    //page_search里，search结果指示往sibling节点和next_level去都会返回true
    goto next;
  }
}

// TODO: Need Fix
uint64_t Tree::range_query(const Key &from, const Key &to, Value *value_buffer,
                           CoroContext *cxt, int coro_id) {

  const int kParaFetch = 32;
  thread_local std::vector<InternalPage *> result;
  thread_local std::vector<GlobalAddress> leaves;

  result.clear();
  leaves.clear();
  index_cache->search_range_from_cache(from, to, result);

  if (result.empty()) {
    return 0;
  }

  uint64_t counter = 0;
  for (auto page : result) {
    auto cnt = page->hdr.last_index + 1;
    auto addr = page->hdr.leftmost_ptr;

    // [from, to]
    // [lowest, page->records[0].key);
    bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
    if (!no_fetch) {
      leaves.push_back(addr);
    }
    for (int i = 1; i < cnt; ++i) {
      no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
      if (!no_fetch) {
        leaves.push_back(page->records[i - 1].ptr);
      }
    }

    no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
    if (!no_fetch) {
      leaves.push_back(page->records[cnt - 1].ptr);
    }
  }

  // printf("---- %d ----\n", leaves.size());
  // sleep(1);

  int cq_cnt = 0;
  char *range_buffer = (dsm->get_rbuf(coro_id)).get_range_buffer();
  for (size_t i = 0; i < leaves.size(); ++i) {
    if (i > 0 && i % kParaFetch == 0) {
      dsm->poll_rdma_cq(kParaFetch);
      cq_cnt -= kParaFetch;
      for (int k = 0; k < kParaFetch; ++k) {
        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
        for (int i = 0; i < kLeafCardinality; ++i) {
          auto &r = page->records[i];
          if (r.value != kValueNull && r.f_version == r.r_version) {
            if (r.key >= from && r.key <= to) {
              value_buffer[counter++] = r.value;
            }
          }
        }
      }
    }
    dsm->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
              kLeafPageSize, true);
    cq_cnt++;
  }

  if (cq_cnt != 0) {
    dsm->poll_rdma_cq(cq_cnt);
    for (int k = 0; k < cq_cnt; ++k) {
      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
      for (int i = 0; i < kLeafCardinality; ++i) {
        auto &r = page->records[i];
        if (r.value != kValueNull && r.f_version == r.r_version) {
          if (r.key >= from && r.key <= to) {
            value_buffer[counter++] = r.value;
          }
        }
      }
    }
  }

  return counter;
}

// Del needs to be rewritten
void Tree::del(const Key &k, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());
  before_operation(cxt, coro_id);

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;

next:
  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING" << std::endl;
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {

      goto next;
    }
  }

  leaf_page_del(p, k, 0, cxt, coro_id);
}
//Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
// THis funciton will get the page by the page addr and search the pointer for the
// next level if it is not leaf page. If it is a leaf page, just put the value in the
// result
bool Tree::page_search(GlobalAddress page_addr, const Key &k,
                       SearchResult &result, CoroContext *cxt, int coro_id,
                       bool from_cache, bool isroot) {
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //struct_offset字段用于计算特定结构体成员离结构体的偏移量，这里计算的就是hdr成员离LeafPage开头有多长的偏移量
  //因此这里就是将获取得page_buffer中得特定偏移量实例化出header来,也就是将leafPage_size大小的page_buffer，取出其中的Header成员应该在的位置并进行一些操作

  auto &pattern_cnt = pattern[dsm->getMyThreadID()][page_addr.nodeID];
  //这里是thread_ID-MN_nodeID

  int counter = 0;
re_read:
  if (++counter > 100) {
    printf("re read too many times\n");
    sleep(1);
  }
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);//将page_addr的内容读到page_buffer上
  //目前传入得page_addr就是root节点得gaddr
  pattern_cnt++;
  memset(&result, 0, sizeof(result));
  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
  //注意这里不是一个连等，而是，判断当前leaf的header的leftmost_ptr是否为空，如果为空，则is_leaf为true，否则为内部节点
  //换句话说，leaf的leftmost_ptr为空，则其为叶节点，否则其有一个leftmost_ptr
  //然后在Hearder的构造函数里，leftmost_ptr首先被置为Null
  //这是因为在internal_page_store里，内部节点的left_most_ptr被设置为其左兄弟的record[m]的ptr，而又因为其做兄弟范围不会超过split_key
  //split_key也即，(record[m])，因而左兄弟链接到此不需要再调整
  //如果是最左侧的内部节点咋办？？
  //在internal_page的构造函数中需要传入left节点来给内部节点构造left_most_ptr；同时，update_new_root的时候会让new_root的left_most_ptr指向分裂节点
  //这样，当root不再是leaf的时候，所有最左节点都会继承这个特性，从而使得全部的内部节点都具有leaf_most_ptr


  
  result.level = header->level;
  if(!result.is_leaf)
      assert(result.level !=0);
  path_stack[coro_id][result.level] = page_addr;
  //当初始进来的时候，记录下的是path_stack[0][0]=page_addr,而page_addr则是root_ptr(这个是当前从只有root来看的情况)

  // std::cout << "level " << (int)result.level << " " << page_addr <<
  // std::endl;

  if (result.is_leaf){
    auto page = (LeafPage *)page_buffer;
    if (!page->check_consistent()){
      goto re_read;
    }

    if (from_cache &&  //from_cache为false，或者from_cache为1且key在叶节点范围内的，能够跳出去
        (k < page->hdr.lowest || k >= page->hdr.highest)){ // cache is stale
      return false;
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {  //一般情况下应该不会发生这种
      assert(false);
      return false;   
    }
    leaf_page_search(page, k, result);  //当只有一个root节点，且同为leaf的时候，直接进入leaf_page_sear阶段
    //leaf_page_search是从page_buffer的records数组里搜，是否该叶节点已经有一个kv等于要插入的kv了，如果是的话，就记入results.val里
  } else {

    assert(result.level != 0);
    assert(!from_cache);
    auto page = (InternalPage *)page_buffer;
//      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

    if (!page->check_consistent()) {
      goto re_read;
    }

    if (result.level == 1 && enable_cache) {  //
        // add the pointer of this internal page into cache. Why it make sense?
//        printf("Add content to cache\n");
      index_cache->add_to_cache(page);
      // if (enter_debug) {
      //   printf("add %lud [%lud %lud]\n", k, page->hdr.lowest,
      //          page->hdr.highest);
      // }
    }

    if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
    // TODO: if this is the root node then we need to refresh the new root.
    //这里应该是一个需要完善的点，因为如果发生在根节点超界同时又修改了根节点缓存，那么是需要更新根节点的，但是由于在根节点之初期，hdr.highest就是整个key空间的最大值
    //所以在本项目中应该不会走到这一条路
        if (isroot){
            // invalidate the root.
            g_root_ptr = GlobalAddress::Null();
        }
      result.slibing = page->hdr.sibling_ptr;  //内部节点超界，返回true会使得在后续跳转进入sibling节点继续查
      return true;
    }
    if (k < page->hdr.lowest) {  //这种情况下会用print_and_check_tree()进行自检？why?
      //一个节点首先是将其split_key作为anchor加入到上层节点，无论这个节点怎么分裂，从split_key到其的映射都不会变的，其指向的node的hdr.low就是split_key
      //如果出现这种情况，表明可能是出了一些错误
      //所以一般是不会出现这种情况的
      printf("key %ld error in level %d\n", k, page->hdr.level);
      sleep(10);
      print_and_check_tree();
      assert(false);
      return false;
    }
    // this function will add the children pointer to the result.
    internal_page_search(page, k, result);
  }

  return true;
}
// internal page serach will return the global point for the next level
void Tree::internal_page_search(InternalPage *page, const Key &k,
                                SearchResult &result) {

  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);
// if the record front verison is not equal to the rear version, what to do.
    // If we pile up the index sequentially by mulitple threads the bugs will happen
    // when muli9tple thread trying to modify the same page, because the reread for
    // inconsistent record below is not well implemented.

    //TODO (potential bug) what will happen if the last record version is not consistent?

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) { // this only happen when the lowest is 0
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
    result.next_level = page->hdr.leftmost_ptr;
//      result.upper_key = page->records[0].key;
      assert(result.next_level != GlobalAddress::Null());
//      assert(page->hdr.lowest == 0);//this actually should not happen
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
//        printf("next level key is %lu \n", page->records[i - 1].key);
      result.next_level = page->records[i - 1].ptr;
        assert(result.next_level != GlobalAddress::Null());
        assert(page->records[i - 1].key <= k);
        result.upper_key = page->records[i - 1].key;
      return;
    }
  }
//    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);

    result.next_level = page->records[cnt - 1].ptr;
    assert(result.next_level != GlobalAddress::Null());
    assert(page->records[cnt - 1].key <= k);
//    result.upper_key = page->records[cnt - 1].key;
}

void Tree::leaf_page_search(LeafPage *page, const Key &k,
                            SearchResult &result) {

  for (int i = 0; i < kLeafCardinality; ++i) {
    auto &r = page->records[i];
    // if the record front verison is not equal to the rear version, what to do.
    // If we pile up the index sequentially by mulitple threads the bugs will happen
    // when muli9tple thread trying to modify the same page, because the reread for
    // inconsistent record below is not well implemented.
    if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
      result.val = r.value;
        memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
//      result.value_padding = r.value_padding;
      break;
    }
  }
}//就是在leafPage里进行顺序搜索

void Tree::internal_page_store(GlobalAddress page_addr, const Key &k,
                               GlobalAddress v, GlobalAddress root, int level,
                               CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  auto &rbuf = dsm->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);

    return;
  }
  assert(k >= page->hdr.lowest);

  auto cnt = page->hdr.last_index + 1;
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  bool is_update = false;
  uint16_t insert_index = 0;
  //TODO: Make it a binary search.
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;
      // assert(false);
      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }  //以上在内部节点插入的阶段，搜索时判断是否有内部节点内的key等于了已有k，如果是的话直接更新sibling_ptr，否则的话就找到该插入的位置
  assert(cnt != kInternalCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  assert(page->records[page->hdr.last_index].key != 0);

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kInternalCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  // THe internal node is different from leaf nodes because it has the
  // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
  // the internal node split pick the middle key as split key and it
  // will not existed in either of the splited node
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kInternalPageSize);
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;
      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);
      for (int i = m + 1; i < cnt; ++i) { // move
          sibling->records[i - m - 1].key = page->records[i].key;
          sibling->records[i - m - 1].ptr = page->records[i].ptr;
      }
      page->hdr.last_index -= (cnt - m); // this is correct.
      assert(page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m - 1);
      assert(sibling->hdr.last_index == cnt - m - 1 - 1);
      sibling->hdr.leftmost_ptr = page->records[m].ptr;
      sibling->hdr.lowest = page->records[m].key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = page->records[m].key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    //the code below is just for debugging.
//    sibling_addr.mark = 2;

    dsm->write_sync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
      assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

  }
//  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());


    page->set_consistent();
  write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return;

  if (root == page_addr) { // update root

    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);  //进行级联式的节点分裂
  } else {
      insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
    assert(false);
  }
}

bool Tree::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level,
                           CoroContext *cxt, int coro_id, bool from_cache){ //from_cache默认是false
                          //level在使用过程中都是0
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
      //如果有多个page_addr被映射到同一个位置上怎么办?这样的hash冲突如何解决？

  GlobalAddress lock_addr;


    char padding[VALUE_PADDING];
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);  //得到远程锁表上的lock位置
  //这里的pn-chip memory是根据页面地址做city_hash映射，然后计算锁表位置来得到的，这里难道不会遇到hash冲突的问题吗？
#endif

  auto &rbuf = dsm->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  auto tag = dsm->getThreadTag();
  assert(tag != 0); //因为tag始终比thread_id的低字节位大1，所以tag!=对从0开始编号的thread都是成立的

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,

                     lock_addr, tag, cxt, coro_id);  //利用CAS_buffer将lock_addr远端内存上的lock原子翻转，同时将page_addr上的内容读到page_buffer上来

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());  //检查有效性

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)){ // cache is stale 
      //这里是因为hdr.highest取不到，所以采用>=highest
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);//这里说明，cache失效是根据读到的内容来决定的，读到的内容表明缓存失效了

    // if (enter_debug) {
    //   printf("cache {%lu} %lu [%lu %lu]\n", page_addr.val, k,
    //   page->hdr.lowest,
    //          page->hdr.highest);
    // }

    return false; 
  }

  // if (enter_debug) {
  //   printf("{%lu} %lu [%lu %lu]\n", page_addr.val, k, page->hdr.lowest,
  //          page->hdr.highest);
  // }

  if (k >= page->hdr.highest){  //这里应该是from_cache为false的情况

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                          coro_id);//如果是超界了，那么会尝试往兄弟节点进行page_store，这个过程是递归的，表明，非from_cache使得在越过右侧界限的情况下，会递归执行page_store的尝试

    return true;
  }
  assert(k >= page->hdr.lowest);

  int cnt = 0;
  int empty_index = -1;  
  char *update_addr = nullptr;
    // It is problematic to just check whether the value is empty, because it is possible
    // that the buffer is not initialized as 0
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {  //叶节点插入采用的是unsorted_leafNode的设计，因此节点分裂需要先对内容进行排序；至于采用unsorted_leafNode的原因应该是采用了细粒度版本机制，希望减少回写的带宽需求
      cnt++;
      if (r.key == k) {  //这里是如果已经有一个key下有value，就执行更新操作
        r.value = v;
        // ADD MORE weight for write.
        memcpy(r.value_padding, padding, VALUE_PADDING);

        r.f_version++;
        r.r_version = r.f_version;
        update_addr = (char *)&r;
        break;
      }
    } else if (empty_index == -1) {
      empty_index = i;  //这里的empty_index为，找到了一个空位置
    }
  }

  assert(cnt != kLeafCardinality);
  //普通情况下cnt是到不了叶节点最大槽位的，后续插入新kv，会再次递增cnt，然后cnt与最大槽位数相等，则触发split

  if (update_addr == nullptr){ // insert new item
    if (empty_index == -1) {
      printf("%d cnt\n", cnt);
      assert(false);
    }

    auto &r = page->records[empty_index];//根据找到的第一个empth位置插入KV
    r.key = k;
    r.value = v;
    memcpy(r.value_padding, padding, VALUE_PADDING);
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;//在这里，插入新键值对后要再次递增cnt
  }

  bool need_split = (cnt == kLeafCardinality);//如果键值对位置已满，则需要进入分裂状态，否则就不需要分裂
  //插入新键值对到达最大的槽位数量，就引发分裂

  if (!need_split) {//不需要分裂就写回，并解锁
    assert(update_addr);
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - (char *)page)),
        sizeof(LeafEntry), cas_buffer, lock_addr, tag, cxt, coro_id, false);
        //这里是细粒度的写回，因为这里只有leafEntry级别的写回
    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  } //如果需要分裂，就先对leaf内的KV进行排序，这里需要排序，也可以看出原因，因为插入操作中寻找空节点时，并不会保证数据排序


  Key split_key;
  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kLeafPageSize);  //如果需要分裂，则当前节点的兄弟节点需要重新申请一个Page，并且会返回这个PageSize的全局地址
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);

      for (int i = m; i < cnt; ++i) { // move
          sibling->records[i - m].key = page->records[i].key;
          sibling->records[i - m].value = page->records[i].value;
          page->records[i].key = 0;
          page->records[i].value = kValueNull;
      }
      //We don't care about the last index in the leaf nodes actually,
      // because we iterate all the slots to find an entry.
      page->hdr.last_index -= (cnt - m);
//      assert(page_addr == root || page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m);
//      assert(sibling->hdr.last_index == cnt -m -1);
      sibling->hdr.lowest = split_key;// the lowest for leaf node is the lowest that this node contain
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = split_key;  //分裂过后更新hdr里的元数据信息

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;  //这是因为，现在加入的sibling_node位于旧node和旧node的sibling_ptr之间
      page->hdr.sibling_ptr = sibling_addr;  //所以这里是让sibling的sibling等于旧node的sibling，让旧node的sibling等于这个分裂得到的sibling节点
    sibling->set_consistent();
    dsm->write_sync(sibling_buf, sibling_addr, kLeafPageSize, cxt);  //将sibling节点写入远端
  }

  page->set_consistent();
    // why need split make the write and locking async?
  write_page_and_unlock(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);  //将分裂节点写入远端后，再将自己也写回远端

  if (!need_split)//由于上述write_page_and_unlock无论分裂与否都要发生，这里是分裂节点情况的衍生讨论
    return true;  //如果不需要分裂，那么返回true，否则继续往下执行
  // note: there will be a bug for the concurrent root update. because the root is not guaranteed to be the same
  // when split pop up to the root node. Causing two nodes.
  if (root == page_addr) { // update root
  //这里表示，如果是分裂的节点，正好是root节点的话，则需要进行root节点的更新与传播，但是这里的bug就是，root节点无法并发更新
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return true;
    }
  }
//以上是利用传入的参数root判断是否是根节点在分裂
//那么这个逻辑就是，分裂完毕后，在内部形成这个插入的新节点，所以才会有internel_page_store，进行逐级完善的SMO的操作
  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {

    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
    assert(from_cache);
    //If the program comes here, then it could be dangerous
    insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
  }

  return true;
}

// Need BIG FIX
void Tree::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id){
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  auto tag = dsm->getThreadTag();
  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

  auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
  }

  auto cnt = page->hdr.last_index + 1;

  int del_index = -1;
  for (int i = 0; i < cnt; ++i) {
    if (page->records[i].key == k) { // find and update
      del_index = i;
      break;
    }
  }

  if (del_index != -1) { // remove and shift
    for (int i = del_index + 1; i < cnt; ++i) {
      page->records[i - 1].key = page->records[i].key;
      page->records[i - 1].value = page->records[i].value;
    }

    page->hdr.last_index--;

    page->set_consistent();
    dsm->write_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  }
  this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, false);
}

void Tree::run_coroutine(CoroFunc func, int id, int coro_cnt) {

  using namespace std::placeholders;

  assert(coro_cnt <= define::kMaxCoro);
  for (int i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, dsm, id);
    worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, i));
  }

  master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

  master();
}

void Tree::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id){
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (true) {

    auto r = gen->next();

    coro_timer.begin();
    if (r.is_search) {
      Value v;
      this->search(r.k, v, &ctx, coro_id);
    } else {
      this->insert(r.k, r.v, &ctx, coro_id);
    }
    auto us_10 = coro_timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][us_10]++;
  }
}

void Tree::coro_master(CoroYield &yield, int coro_cnt) {

  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }

  while (true) {

    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      yield(worker[next_coro_id]);
    }

    if (!hot_wait_queue.empty()) {
      next_coro_id = hot_wait_queue.front();
      hot_wait_queue.pop();
      yield(worker[next_coro_id]);
    }

    if (!deadline_queue.empty()) {
      auto now = timer.get_time_ns();
      auto task = deadline_queue.top();
      if (now > task.deadline) {
        deadline_queue.pop();
        yield(worker[task.coro_id]);
      }
    }
  }
}

// Local Locks
inline bool Tree::acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  bool is_local_locked = false;

  uint64_t lock_val = node.ticket_lock.fetch_add(1);

  uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
  uint32_t current = lock_val >> 32;// current is the former 32 bit in ticket lock

  // printf("%ud %ud\n", ticket, current);
  while (ticket != current) { // lock failed
    is_local_locked = true;

    if (cxt != nullptr) {
      hot_wait_queue.push(coro_id);
      (*cxt->yield)(*cxt->master);
    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  if (is_local_locked) {
    hierarchy_lock[dsm->getMyThreadID()][0]++;
  }

  node.hand_time++;

  return node.hand_over;
}

inline bool Tree::can_hand_over(GlobalAddress lock_addr) {

  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
// only when unlocking, it need to check whether it can handover to the next, so that it do not need to UNLOCK the global lock.
// It is possible that the handover is set as false but this server is still holding the lock.
  uint32_t ticket = lock_val << 32 >> 32;//
  uint32_t current = lock_val >> 32;
// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime; // check the limit
  }
  if (!node.hand_over) {
    node.hand_time = 0;// clear the handtime.
  } else {
    handover_count[dsm->getMyThreadID()][0]++;
  }

  return node.hand_over;
}

inline void Tree::releases_local_lock(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}

void Tree::index_cache_statistics() {
  index_cache->statistics();
  index_cache->bench();
}

void Tree::clear_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}







