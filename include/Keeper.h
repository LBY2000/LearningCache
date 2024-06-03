#ifndef __KEEPER__H__
#define __KEEPER__H__

#include <assert.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <thread>

#include <libmemcached/memcached.h>

#include "Config.h"
#include "Debug.h"
#include "Rdma.h"
enum NodeType {Compute, Memory};
struct ExPerThread {
    uint16_t lid;
    uint8_t gid[16];

    uint32_t rKey;

    uint32_t lock_rkey; //for directory on-chip memory
} __attribute__((packed));

struct ExchangeMeta {//C和M均会在本地填上各自要填的内容，后续被对端拿走这些元数据后，将会被写入remoteConnection里
    NodeType node_type;//M,C
    uint64_t dsmBase; //M端会填
    uint64_t cacheBase; //C端会填
    uint64_t lockBase; //M

    ExPerThread appTh[MAX_APP_THREAD]; //C，这个是C端各个线程的rdma环境，在这里会填入C端各个线程的lid,rkey和gid,而似乎
    //在C端的cache_Pool的作用只是用作message的缓冲
    ExPerThread dirTh[NR_DIRECTORY]; //M，这个是M端rdma环境
    //似乎这里的lid和gid均只与rdma context有关
    //在创建qp的初期，都会传入本机的rdma_context环境，因此在这里每个dirTh和appTH获得了其lid和gid


    uint32_t appUdQpn[MAX_APP_THREAD];//C端会填，填入的是本端thTH的message_qpn
    uint32_t dirUdQpn[NR_DIRECTORY]; //M端填入的是本侧dirTH对应的message_qp的qpn

    uint32_t appRcQpn2dir[MAX_APP_THREAD][NR_DIRECTORY];//C端的connect_node阶段会将其填入，主要是填入本机thTH的qp的qpn

    uint32_t dirRcQpn2app[NR_DIRECTORY][MAX_APP_THREAD]; //M端在connect_node(k)节点将MN端线程qp，而非是message_qp的qpn填入
    //M端填入的信息是，针对某个node的qp的qpn，即为当前MN_th[i]-CN_th[j]的qpn映射

}__attribute__((packed));
class Keeper {

private:


protected:
    static const char *COMPUTE_NUM_KEY;
    static const char *MEMORY_NUM_KEY;
    uint32_t ComputemaxServer;
    uint32_t MemorymaxServer;
    uint16_t curServer;
    uint16_t myNodeID;//keeper有一个myNodeID，DSM也有。本质上DSM通过keeper->getID返回当前节点的myNodeID
    std::string myIP;
    uint16_t myPort;

    memcached_st *memc;
  bool connectMemcached();
  bool disconnectMemcached();

  virtual void serverEnter() = 0;  //这些函数应该适配MN和CN端，因此Keeper类并没有写具体逻辑，而是采用虚函数的形式
  virtual void serverConnect() = 0;
  virtual bool connectNode(uint16_t remoteID) = 0;


public:
  Keeper(uint32_t ComputemaxServer = 12, uint32_t MemorymaxServer = 12);
  ~Keeper();

  uint16_t getMyNodeID() const { return this->myNodeID; }  //这里的myNodeID指的是加入集群的节点分配的ID，例如MN0,MN1之类的
  uint16_t getComputeServerNR() const { return this->ComputemaxServer; }
  uint16_t getMyPort() const { return this->myPort; }

  std::string getMyIP() const { return this->myIP; }


  void memSet(const char *key, uint32_t klen, const char *val, uint32_t vlen);
  //blocking function.
  char *memGet(const char *key, uint32_t klen, size_t *v_size = nullptr);
  uint64_t memFetchAndAdd(const char *key, uint32_t klen);
};

#endif
