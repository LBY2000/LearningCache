#if !defined(_LOCAL_ALLOC_H_)
#define _LOCAL_ALLOC_H_

#include "Common.h"
#include "GlobalAddress.h"

#include <vector>

// for fine-grained shared memory alloc
// not thread safe
// now it is a simple log-structure alloctor
// TODO: slab-based alloctor
class LocalAllocator {  //目前似乎只是一个简单的影子分配器

public:
  LocalAllocator() {
    head = GlobalAddress::Null();
    cur = GlobalAddress::Null();
  }

  GlobalAddress malloc(size_t size, bool &need_chunck, bool align = false){

    if (align) {
    }

    GlobalAddress res = cur;  //第一次malloc的适合log_heads是空，因此返回NULL以及need_chunk为true
    if (log_heads.empty() ||
        (cur.offset + size > head.offset + define::kChunkSize)){  //要么是当前拥有的chunk为空，要么是当前要求分配的空间分配后大于了当前chunk的边界
        //两者都会要求进行chunk分配
      need_chunck = true;
    }else{
      need_chunck = false;
      cur.offset += size;
    }

    // assert(res.addr + size <= 40 * define::GB);

    return res;
  }

  void set_chunck(GlobalAddress &addr) {
    log_heads.push_back(addr);//将新分配的chunk的全局地址加入到localAllocator里，表明本地获得了这块远端内存
    //而local_allocator是thread_local的，这意味着每个thread都拥有自己的独立的local_allocator和独立的获取的远端内存chunk
    head = cur = addr;
  }

  void free(const GlobalAddress &addr) {
    // TODO
  }

private:
  GlobalAddress head;
  GlobalAddress cur;
  std::vector<GlobalAddress> log_heads;
};

#endif // _LOCAL_ALLOC_H_
