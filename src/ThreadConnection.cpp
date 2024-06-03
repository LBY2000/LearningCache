#include "ThreadConnection.h"

#include "Connection.h"

ThreadConnection::ThreadConnection(uint16_t threadID, void *cachePool,
                                   uint64_t cacheSize, uint32_t machineNR,
                                   RemoteConnection *remoteInfo)
    : threadID(threadID), remoteInfo(remoteInfo){
  createContext(&ctx);

  cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);  //这个是threadconnection与dirconnection之间的cq
  // rpc_cq = cq;
  rpc_cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);  //这个是rawmessageconnection之间的cq，而对于MN一侧来说，其dirTH的qp与message的qp是共享同一个cq的

  message = new RawMessageConnection(ctx, rpc_cq, APP_MESSAGE_NR);  //Thread一端的Message大小为96

  this->cachePool = cachePool;
  //This create Memory Region will register the same memory muliptle times.
  cacheMR = createMemoryRegion((uint64_t)cachePool, cacheSize, &ctx);
  cacheLKey = cacheMR->lkey;

  // dir, RC
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    data[i] = new ibv_qp *[machineNR];
    for (size_t k = 0; k < machineNR; ++k) {
      createQueuePair(&data[i][k], IBV_QPT_RC, cq, &ctx);  //machineNR在CN端指的是MN endpoints的数量，在MN端指的是CN endpoints的数量
    }//所以对于每个MN端的threadconnection来说，其保有针对远端节点上每个MN线程dirTH的qp
  }
}

void ThreadConnection::sendMessage2Dir(RawMessage *m, uint16_t node_id,
                                       uint16_t dir_id) {  //在这里message的发送都需要传入remoteConnection的信息作为指示
  
  message->sendRawMessage(m, remoteInfo[node_id].dirMessageQPN[dir_id],
                          remoteInfo[node_id].appToDirAh[threadID][dir_id]);
}
