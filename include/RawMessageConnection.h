#ifndef __RAWMESSAGECONNECTION_H__
#define __RAWMESSAGECONNECTION_H__

#include "AbstractMessageConnection.h"
#include "GlobalAddress.h"

#include <thread>

enum RpcType : uint8_t {
  MALLOC,
  FREE,
  NEW_ROOT,
  NOP,
};

struct RawMessage {
  RpcType type;
  
  uint16_t node_id;  //这里的node_id不知道是什么意思，但是看起来像是对MN端线程end_point的封装
  uint16_t app_id;  //app_id可能是CN端threads的id号

  GlobalAddress addr; // for malloc
  int level; //可能是与Tree_index相关的一个东西
}__attribute__((packed));

class RawMessageConnection : public AbstractMessageConnection {

public:
  RawMessageConnection(RdmaContext &ctx, ibv_cq *cq, uint32_t messageNR);

  void initSend();
  void sendRawMessage(RawMessage *m, uint32_t remoteQPN, ibv_ah *ah);
};

#endif /* __RAWMESSAGECONNECTION_H__ */
