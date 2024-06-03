#include "RawMessageConnection.h"

#include <cassert>

RawMessageConnection::RawMessageConnection(RdmaContext &ctx, ibv_cq *cq,
                                           uint32_t messageNR)
    : AbstractMessageConnection(IBV_QPT_UD, 0, 40, ctx, cq, messageNR) {}
 //这里IBV_QPT_UD指的是qp类型，0是send_padding的长度，40是recv_padding的长度
void RawMessageConnection::initSend() {}//send_init似乎没有做

void RawMessageConnection::sendRawMessage(RawMessage *m, uint32_t remoteQPN,
                                          ibv_ah *ah) {

  if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0) {//send_counter初始化是0，SIGNAL_BATCH为31
    ibv_wc wc;//为何说31呢？31只是和dirth这一侧相对应
    pollWithCQ(send_cq, 1, &wc);//如果sendCounter & SIGNAL_BATCH为1的话，会给wr额外添加一个属性，猜测这里是为了从远端接收一些反馈
  }//似乎pollWithCQ并没有做什么特别的事情

  rdmaSend(message, (uint64_t)m - sendPadding, sizeof(RawMessage) + sendPadding,
           messageLkey, ah, remoteQPN, (sendCounter & SIGNAL_BATCH) == 0);
           //m是一个指向rawmessage的指针，因此m记录的是其地址，因而m本身可以作为发送地址信息
           //但是不是很理解为什么源地址要减去sendPadding，不会溢出吗？也许是在message_pool中，减去一个padding也没事
           //RAWMessage的大小为24字节，其实远远低于设定的96字节大小的MESSAGE_SIZE

  ++sendCounter;
}
