  #ifndef __CONFIG_H__
  #define __CONFIG_H__

  #include "Common.h"

  class CacheConfig {
  public:
    uint32_t cacheSize;
      // THis cache size is irrelevant to the index cache size, this is the size for local RDMA buffer
    CacheConfig(uint32_t cacheSize = 1024) : cacheSize(cacheSize) {}
  };

  class DSMConfig {
  public:
    CacheConfig cacheConfig;
    uint32_t MemoryNodeNum;
    uint32_t ComputeNodeNum;
    uint64_t dsmSize; // G

    DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
              uint32_t machineNR = 2, uint64_t dsmSize =40)// THe dsm memory size initialize here.
        : cacheConfig(cacheConfig), MemoryNodeNum(machineNR), dsmSize(dsmSize) {}
  };

  #endif /* __CONFIG_H__ */
