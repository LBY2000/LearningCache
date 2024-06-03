#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <bitset>
#include <limits>

#include "Debug.h"
#include "HugePageAlloc.h"
#include "Rdma.h"

#include "WRLock.h"

// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC
typedef unsigned char      myuint8_t;
#define LATENCY_WINDOWS 1000000

#define STRUCT_OFFSET(type, field)                     \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define MAX_MACHINE 8

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96 // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 128

// { app thread
#define MAX_APP_THREAD 26
//26
#define APP_MESSAGE_NR 96

// }

// { dir thread
#define NR_DIRECTORY 1

#define DIR_MESSAGE_NR 128

//#define kInternalPageSize 2*1024

//#define kLeafPageSize 2*1024

#define kInternalPageSize 512

#define kLeafPageSize 512

#define KEY_PADDING 0
//#define KEY_PADDING 12
#define VALUE_PADDING 0
//#define VALUE_PADDING 392
// }

#define BUCKET_SLOTS 8

void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

#include <boost/coroutine/all.hpp>

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  int coro_id;
};

namespace define {

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t kChunkSize = 32 * MB;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = 256 * 1024;

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough concurrency.
// if you wan to use 16-bit locks, call *cas_dm_mask*
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);

// level of tree
//constexpr uint64_t kMaxLevelOfTree = 7;
constexpr uint64_t kMaxLevelOfTree = 15;

constexpr uint16_t kMaxCoro = 8;
constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;
//The upper limit for cache is 1TB
//constexpr int kIndexCacheSize = 1024*1024; // MB
constexpr int kIndexCacheSize = 1000; // MB
//constexpr int kIndexCacheSize = 10000; // MB
} // namespace define

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}
struct Key_buff {
    char buffer[20];
};
struct Value_buff {
    char buffer[400];
};
// For Tree
using Key = uint64_t;
using Value = uint64_t;
//using Key = Key_buff;
//using Value = Value_buff;
constexpr Key kKeyMin = std::numeric_limits<Key>::min();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
constexpr Value kValueNull = 0;
//constexpr Value kValueNull = {};
//constexpr uint32_t kInternalPageSize = 1024;
//constexpr uint32_t kLeafPageSize = 1024;

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }



class Entry{
   public:
    uint64_t key;
    uint64_t val;
    Entry(){
        key=0;
        val=0;
    };
};
class LeafBucket{
    friend class LC;

    public:
      uint64_t bucket_lock;
      myuint8_t f_version;
      Entry entry[BUCKET_SLOTS];
      myuint8_t r_version;
      LeafBucket(){
        f_version=0;
        r_version=0;
        bucket_lock=0;
      }
      void set_consistent(){
        f_version++;
        r_version=f_version;
      }
      bool check_consistent(){
        bool succ=true;
        succ = succ && (r_version == f_version);
        return succ;
      }

};
class LeafNode{
    public:
       uint64_t stash_loc;
       LeafBucket front_buckets[128];
       LeafBucket backup_buckets[32];
       Entry stash[256];
};


#endif /* __COMMON_H__ */

