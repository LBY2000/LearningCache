#ifndef __LINEAR_KEEPER__H__
#define __LINEAR_KEEPER__H__

#include <vector>

#include "Keeper.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;
struct RemoteConnection;



class DSMKeeper : public Keeper {

private:
  static const char *OK;
  static const char *ServerPrefix;

  ThreadConnection **thCon;
  DirectoryConnection **dirCon;
  RemoteConnection *remoteCon;

  ExchangeMeta localMeta;

  std::vector<std::string> serverList;

    std::string setKey(uint16_t remoteID) {
        return std::to_string(getMyNodeID()) + "C" + "-" + std::to_string(remoteID) + "M";
    }

    std::string getKey(uint16_t remoteID) {
        return std::to_string(remoteID) + "M" + "-" + std::to_string(getMyNodeID()) + "C";
    }

  void initLocalMeta_Compute();
  void initLocalMeta_Memory();
  void connectMySelf();
  void initRouteRule();

  void setDataToRemote(uint16_t remoteID);
  void setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta);

protected:
  virtual bool connectNode(uint16_t remoteID) override;
  virtual void serverConnect() override;
  virtual void serverEnter() override;
public:
  DSMKeeper(ThreadConnection **thCon, DirectoryConnection **dirCon, RemoteConnection *remoteCon,
            uint32_t maxMemoryServer = 12, uint32_t maxComputeServer = 12)
      : Keeper(maxComputeServer, maxMemoryServer), thCon(thCon), dirCon(dirCon),
        remoteCon(remoteCon) {

      initLocalMeta_Compute();//在这里填充的是当前节点需要对外进行交换的元数据节点

    if (!connectMemcached()) {
      return;
    }

  }
  void initialization(){
      serverEnter();//将自己加入集群push到节点里，并且获得其在集群内的唯一节点号

      serverConnect();//在这里将自己的元数据上传，然后拉取远端元数据，并填充remoteConnection状态，以及转换本地appTh的qp的状态为RTS
//      connectMySelf();

      initRouteRule();//将自己的IP上传到memcached服务器但是感觉好像作用不算很大
  }

  ~DSMKeeper() { disconnectMemcached(); }
  void barrier(const std::string &barrierKey);
  uint64_t sum(const std::string &sum_key, uint64_t value);
};

#endif
