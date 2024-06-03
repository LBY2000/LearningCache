
#include "DSM.h"
#include <atomic>
#include <city.h>
#include <functional>
#include <iostream>
#include <mutex>
#include "PLR.h"
#include "Models.h"
#include <vector>
#include <algorithm>
#include "Tree.h"

#define LOAD_KEYS 16000000
#define BACK_KEYS 100000
extern GlobalAddress g_root_ptr;

class LC{
    
   // private:
    public:
       int test;
       DSM *dsm;
      // GlobalAddress root_ptr_ptr;
       uint64_t LC_id;
       Models models;
       std::map<int,int> total_times;  //temp_var
       
       
       friend class DSM;
       friend class Models;
       friend class PLR;
       friend class Tree;
    
IndexCache *index_cache;  
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];

      // uint64_t kKeySpace;
       IndexCache *index_cache_lc;
       std::vector<uint64_t> exist_keys;
       std::vector<uint64_t> nonexist_keys;  //for_bulk_loding
       LC(DSM *dsm);
       ~LC();
       GlobalAddress get_root_ptr_ptr();
       void load_data();
       void normal_data();
       //这里写其他数据集的生成接口
       void append_model(double slope, double intercept,
                     const typename std::vector<uint64_t>::const_iterator &keys_begin,
                     const typename std::vector<uint64_t>::const_iterator &vals_begin, 
                     size_t size);
       bool search(uint64_t &key,uint64_t &val); //仅仅使用down层模型的读
       bool insert(uint64_t key,uint64_t val); //仅仅使用down层模型的写
       bool try_lock(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroContext *ctx = nullptr);  //对传入的地址后的8字节进行cas
       void try_unlock(char *buffer, GlobalAddress gaddr, size_t size,bool signal, CoroContext *ctx=nullptr); //对传入地址后的内容用buffer上的内容替换，用于解锁
       bool model_search(uint64_t &key,uint64_t &val);  //model级别的search但是只搜索了front
       bool model_insert(uint64_t key,uint64_t val);    //front和backup都搜然后进行插入操作
       bool upper_search(uint64_t &key,uint64_t &val);  //利用upper层和down层的搜索
       bool model_batch_search(uint64_t key,uint64_t &val); //model级别的batch查找，包括查找,是完整的LC的search逻辑
       double local_double_predict(uint64_t key); //测试位置预测，用来进行垂直粒度计算用的
       bool test_search(uint64_t &key, uint64_t &val, CoroContext *cxt = nullptr,     //用于tree的和model的联合batch测试,相关代码逻辑是要修改以支持batch操作的
                     int coro_id = 0);  //test_search是moti里，LI+delta-Tree的性能对比测试
        bool test_hash_search(uint64_t &key, uint64_t &val, CoroContext *cxt = nullptr,     
                     int coro_id = 0);  //用于LI+Hash结构的search逻辑
       void print_page_num();



       //下面是耦合的tree结构
         GlobalAddress get_root_ptr(CoroContext *cxt, int coro_id);  //OK
         void tree_insert(const Key &k, const Value &v, CoroContext *cxt = nullptr,  //OK
              int coro_id = 0);
         bool tree_search(const Key &k, Value &v, CoroContext *cxt = nullptr,     //OK
                     int coro_id = 0);
         void tree_del(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);  

         uint64_t tree_range_query(const Key &from, const Key &to, Value *buffer,
                              CoroContext *cxt = nullptr, int coro_id = 0);

         void print_and_check_tree(CoroContext *cxt = nullptr, int coro_id = 0);              //OK



         void lock_bench(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);     //doesn't need
 
         GlobalAddress query_cache(const Key &k);                                        //doesn't need
         void index_cache_statistics();                                                  //doesn't need
         uint64_t tree_id;
         GlobalAddress root_ptr_ptr; // the address which stores root pointer;

         // static thread_local int coro_id;
         static thread_local CoroCall worker[define::kMaxCoro];
         static thread_local CoroCall master;

         LocalLockNode *local_locks[MAX_MACHINE];

         
         void print_verbose();  //OK                                                

         void before_operation(CoroContext *cxt, int coro_id);   //OK, 但是这里修改了path_stack的命名为path_stack_lc

         

         void coro_worker(CoroYield &yield, RequstGen *gen, int coro_id);    //doesn't need
         void coro_master(CoroYield &yield, int coro_cnt);                   //doesn't need  
         void broadcast_new_root(GlobalAddress new_root_addr, int root_level);  //OK
         bool update_new_root(GlobalAddress left, const Key &k, GlobalAddress right,   //OK
                              int level, GlobalAddress old_root, CoroContext *cxt,
                              int coro_id);
         void insert_internal(const Key &k, GlobalAddress v, CoroContext *cxt,         //OK
                              int coro_id, int level);

         bool try_lock_addr(GlobalAddress lock_addr, uint64_t tag, uint64_t *buf,    //OK
                              CoroContext *cxt, int coro_id);
         void unlock_addr(GlobalAddress lock_addr, uint64_t tag, uint64_t *buf,   //OK
                           CoroContext *cxt, int coro_id, bool async);
         void write_page_and_unlock(char *page_buffer, GlobalAddress page_addr,    //OK
                                    int page_size, uint64_t *cas_buffer,
                                    GlobalAddress lock_addr, uint64_t tag,
                                    CoroContext *cxt, int coro_id, bool async);
         void lock_and_read_page(char *page_buffer, GlobalAddress page_addr,    //OK
                                 int page_size, uint64_t *cas_buffer,
                                 GlobalAddress lock_addr, uint64_t tag,
                                 CoroContext *cxt, int coro_id);
         bool page_search(GlobalAddress page_addr, const Key &k, SearchResult &result,  //OK
                           CoroContext *cxt, int coro_id, bool from_cache = false, bool isroot = false);
         void internal_page_search(InternalPage *page, const Key &k,      //OK
                                    SearchResult &result);
         void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);  //OK
         void internal_page_store(GlobalAddress page_addr, const Key &k,  //OK
                                    GlobalAddress value, GlobalAddress root, int level,
                                    CoroContext *cxt, int coro_id);
         bool leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v,  //OK
                              GlobalAddress root, int level, CoroContext *cxt,
                              int coro_id, bool from_cache = false);
         void leaf_page_del(GlobalAddress page_addr, const Key &k, int level,  //OK
                              CoroContext *cxt, int coro_id);

         bool acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,    //OK
                                 int coro_id);
         bool can_hand_over(GlobalAddress lock_addr);   //OK
         void releases_local_lock(GlobalAddress lock_addr);  //OK





};

inline size_t murmur2 ( const void * key, size_t len, size_t seed=0xc70f6907UL)
{
	// 'm' and 'r' are mixing constants generated offline.
	// They're not really 'magic', they just happen to work well.

	const unsigned int m = 0x5bd1e995;
	const int r = 24;

	// Initialize the hash to a 'random' value

	unsigned int h = seed ^ len;

	// Mix 4 bytes at a time into the hash

	const unsigned char * data = (const unsigned char *)key;

	while(len >= 4)
	{
		unsigned int k = *(unsigned int *)data;

		k *= m;
		k ^= k >> r;
		k *= m;
		
		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}
	
	// Handle the last few bytes of the input array

	switch(len)
	{
	case 3: h ^= data[2] << 16;
	case 2: h ^= data[1] << 8;
	case 1: h ^= data[0];
	        h *= m;
	};

	// Do a few final mixes of the hash to ensure the last few
	// bytes are well-incorporated.

	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}






