//
// Created by ruihong on 5/21/22.
//

#ifndef SHERMAN_DSMCONTAINER_H
#define SHERMAN_DSMCONTAINER_H

#include <vector>
#include "DirectoryConnection.h"
#include "Connection.h"
#include "Keeper.h"
#include "Directory.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;  //实际上并没有被使用
struct RemoteConnection;



class DSMContainer : public Keeper{
private:
    static const char *OK;  //就是字符串"OK"
    static const char *ServerPrefix;  //这里是"SPre"
    uint64_t baseAddr;
    ThreadConnection **thCon_;
    DirectoryConnection **dirCon_;
    RemoteConnection *remoteCon;
//    RemoteConnection *remoteInfo;
    ExchangeMeta localMeta;
    DSMConfig* config_inner;//实际没使用
    std::vector<std::string> serverList;//实际没使用
    Directory *dirAgent[NR_DIRECTORY];

    std::string setKey(uint16_t remoteID) {
        return std::to_string(getMyNodeID()) + "M" + "-" + std::to_string(remoteID) + "C";  //在DSMContainer里写下 xM-xC的映射，与之对应的是
    }                                                                                       //在DSMKeeper里写入xC-XM的映射

    std::string getKey(uint16_t remoteID) {
        return std::to_string(remoteID) + "C" + "-" + std::to_string(getMyNodeID()) + "M";
    }
    void initRDMAConnection_Compute();
    void initLocalMeta();
    void connectMySelf();
    void initRouteRule();

    void setDataToRemote(uint16_t remoteID);
    void setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta);

protected:
    virtual bool connectNode(uint16_t remoteID) override;
    virtual void serverConnect() override;
    virtual void serverEnter() override;  //这几个是Keeper中定义的要被重写的虚函数
public:
    DSMContainer(ThreadConnection **thCon, DirectoryConnection **dirCon, const DSMConfig &conf,
                 uint32_t ComputeMaxServer = 12, uint32_t MemoryMaxServer = 12)
    : Keeper(ComputeMaxServer, MemoryMaxServer), thCon_(thCon), dirCon_(dirCon){ //这里CN和MN的max_server被限定为输入参数的数量

        remoteCon = new RemoteConnection[conf.ComputeNodeNum];  //自身的在这里分配，但记住，是ComputeNodeNum，说明有多少CN就有多少RemoteConnection
        //先按照1这个computeNodeNum来理解吧  
        //这个后续使用的node_id看起来像是在区分不同Memory_Node
        //在这里MN端保留的remoteConnection数量和CN_Num一样；而CN端保留的remoteConnection数量和MN_Num一样
        //remoteConnection是end2end的connection连接，对CN来说，看到的endpoint数量确实是MemoryNode的数量，对MN来说，看到的endpoint数量确实也是CN数量


        baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);
        assert(baseAddr != 0);
//        baseAddr = reinterpret_cast<uint64_t>(malloc(1024 * 1024 * 1024));
        for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
             i += 2 * define::MB){
            *(char *)i = 0;  //在这里进行一系列初始化的目的是什么？
        }   //每个2MB块的首1字节内容有那么重要吗?

        memset((char *)baseAddr, 0, define::kChunkSize); //首先将第一个KChunkSize设为0

        for (int i = 0; i < NR_DIRECTORY; ++i) {
            // the connection below just create the queue pair for DSM. also initialize the DSM memory region
            // in this machine.
            dirCon_[i] = 
                    new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
                                            conf.ComputeNodeNum, remoteCon);   //这里的MachineNR指的是ComputeNodeNum
        }//两端的dirTH或者是appTH的注册都只是完成内存注册以及各自qp的生成
//        for (int i = 0; i < NR_DIRECTORY; ++i) {
//            dirAgent[i] =
//            new Directory(dirCon[i], remoteCon, conf.ComputeNodeNum, i, myNodeID);
//        }
        initLocalMeta();  //将本地所有dir_connection的rdma元数据信息封装起来 
        //此时封装的是本端地址，gid等信息，以及dirTH对应的mesaage的qpn信息
        //在connecNode阶段才会将自己的qpn写入ExchangeMeta里

        if (!connectMemcached()) {  //利用Keeper里的memc连接配置中的memcached服务器
            return;
        }
    }   
    void initialization(){
        serverEnter();  //enter MN，将当前memory_node数量写入服务器，之后递增吧并形成从0开始计数的myNodeID
        //相当于每个MN节点执行一次，就会递增memcached服务器上的MN_server数量，然后获取本机的NodeID
        for (int i = 0; i < NR_DIRECTORY; ++i) {  //为每个dir_connection[i]开启一个异步执行的线程
            dirAgent[i] = new Directory(dirCon_[i], remoteCon, MemorymaxServer, i, myNodeID);   //形成异步执行的线程,但是第三个参数实际上似乎并没有被使用
                                                   //这里的myNodeID指的是当前container的nodeID，如果是一台服务器那就是server0
        }//形成初始分配内存的地址，然后启动代理的异步MN_Threads,从而不断监听来自网络的message        
        serverConnect();  //在这里，填补initlocalmetadata的最后空白，并且将 xM - xC : "localmeta"写入memcaced服务器，并且
        //写完xM-xC的，还会读取xC-xM的，并且完善自己的qp信息，并转换qp状态，同时会依靠此信息初始哈remoteconnection的内容，从而完成
        //从xM到xC的RDMA建链过程

        initRouteRule();  //这一步是往memcached服务器写入 serverx : serverx_IP的过程
        //这个是在集群中的CN和MN数量都到达最高的时候会出现的
    }  //其后主函数main陷入死循环，维护directory线程的执行，而directory线程主要是异步接收消息并进行处理

    ~DSMContainer() { disconnectMemcached(); }
    void barrier(const std::string &barrierKey);
    uint64_t sum(const std::string &sum_key, uint64_t value);

};


#endif //SHERMAN_DSMCONTAINER_H
