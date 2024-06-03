#if !defined(_GLOBAL_ALLOCATOR_H_)
#define _GLOBAL_ALLOCATOR_H_

#include "Common.h"
#include "Debug.h"
#include "GlobalAddress.h"

#include <cstring>



// global allocator for coarse-grained (chunck level) alloc 
// used by home agent
// bitmap based
class GlobalAllocator {

public:
  GlobalAllocator(const GlobalAddress &start, size_t size)
      : start(start), size(size) {
    bitmap_len = size / define::kChunkSize;  //按照chunk大小划分区块
    bitmap = new bool[bitmap_len];           //形成bitmap
    memset(bitmap, 0, bitmap_len);
     
    // null ptr
    bitmap[0] = true;  //首一块是被使用的，再次分配并不是从第0块的chunk进行，换句话说，CN可以自由利用这块首0 chunk
    bitmap_tail = 1;   //下一个分配块是第一块
    //在初始阶段，远端内存的布局上，首块chunk是已经被使用的了，只能从bitmap_tail来继续分配，因而首块chunk可以去存元数据
  }

  ~GlobalAllocator() {
      printf("The remote memory usage is %lu MB", bitmap_tail*define::kChunkSize);
      delete[] bitmap;
  }

  GlobalAddress alloc_chunck() {
      static size_t counter = 0;
      if(counter++ % 32 == 0){  //每隔32个chunk划分就形成一次提示
          printf("The remote memory usage is %lu MB\n", bitmap_tail*32);
      }

    GlobalAddress res = start;
    if (bitmap_tail >= bitmap_len){
      assert(false);              //当tail已经大于等于bitmap长度，表明已经分配完毕
      Debug::notifyError("shared memory space run out");
    }

    if (bitmap[bitmap_tail] == false){  //否则的话，如果bitmap_tail为false，则表明可以分配
      bitmap[bitmap_tail] = true;
      res.offset += bitmap_tail * define::kChunkSize;  //这里是因为start是没有变化过的，始终是dsm_pool的起始地址
      //当前的bitmap_tail*chunkSize刚好是可以分配的首地址

      bitmap_tail++;
    }else{
      assert(false);   //否则表明不可被分配
      Debug::notifyError("TODO");  //这里的TODO应该向下轮询
    }

    return res;
  }

  void free_chunk(const GlobalAddress &addr) {  //将该地址的块设置为false，表明可以被再次分配
    bitmap[(addr.offset - start.offset) / define::kChunkSize] = false;
  }

private:
  GlobalAddress start;
  size_t size;

  bool *bitmap;
  size_t bitmap_len;
  size_t bitmap_tail;
};

#endif
