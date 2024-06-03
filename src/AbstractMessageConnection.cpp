#include "AbstractMessageConnection.h"

AbstractMessageConnection::AbstractMessageConnection(  //首先注册messagePool并且划分前半段为recvPool，后半段为sendPool
    ibv_qp_type type, uint16_t sendPadding, uint16_t recvPadding,
    RdmaContext &ctx, ibv_cq *cq, uint32_t messageNR)
    : messageNR(messageNR), curMessage(0), curSend(0), sendCounter(0),
      sendPadding(sendPadding), recvPadding(recvPadding) {  //所以为啥只初始化recv_pool，不初始sendPool呢？

  assert(messageNR % kBatchCount == 0);  //这里messageNR在dirTH一侧为128，kBatchCount为4

  send_cq = ibv_create_cq(ctx.ctx, 128, NULL, NULL, 0);  //创建send_cq，128是send_cq的cqe数量

  createQueuePair(&message, type, send_cq, cq, &ctx);  //一个是send_cq，一个是recv_cq，并且创建与send_cq和recv_cq绑定的qp，
  //其中send_cq属于Ab_connection自己
  //recv_cq则是传入的，因此可能与ThreadConnection以及Dirconnection有关
  modifyUDtoRTS(message, &ctx);   //在此处转换的message的qp的状态                     
  //在这里的转换让qp进入可以传输数据的状态
  messagePool = hugePageAlloc(2 * messageNR * MESSAGE_SIZE);  //相当于预先分配可以接收2*messageNR个message的空间
  messageMR = createMemoryRegion((uint64_t)messagePool,
                                 2 * messageNR * MESSAGE_SIZE, &ctx);
  sendPool = (char *)messagePool + messageNR * MESSAGE_SIZE;  //相当于message_pool的后一半空间给了sendPool
  messageLkey = messageMR->lkey;
}

void AbstractMessageConnection::initRecv() {  //以Kbatch划分，将MessagePool的前半段划分为一个个的用于接收recv_request的区间
  subNR = messageNR / kBatchCount;  //看起来像是以KBatchCount为基础，将messageNR分割，这样整个messageNR就被氛围KBatchCount个
  //对于thread_connection，subNR的大小为96/4=24
  //对于dir_connection，subNR的大小为128/4=32
  for (int i = 0; i < kBatchCount; ++i) {
    recvs[i] = new ibv_recv_wr[subNR];  //实际recvs为 recvs[kBatchCount][subNR]，其为ibv_recv_wr
    recv_sgl[i] = new ibv_sge[subNR];   //实际recv_sgl为 recv_sgl[kBatchCount][subNR]，其为ibv_sge
  }

  for (int k = 0; k < kBatchCount; ++k) {
    for (size_t i = 0; i < subNR; ++i) {
      auto &s = recv_sgl[k][i];
      memset(&s, 0, sizeof(s));

      s.addr = (uint64_t)messagePool + (k * subNR + i) * MESSAGE_SIZE;  //依靠ibv_sge，将接收到的消息放到messagePool上
      s.length = MESSAGE_SIZE;
      s.lkey = messageLkey;

      auto &r = recvs[k][i];
      memset(&r, 0, sizeof(r));

      r.sg_list = &s;  //将sg_list绑定到ibv_recv_wr上
      r.num_sge = 1;
      r.next = (i == subNR - 1) ? NULL : &recvs[k][i + 1];  //将ibv_recv_wr串起来
      //一个batch里32/24个子ibv_recv_wr，这里是将一个batch里的32/24个wr串在了一起，不同batch之间则没有
      //关于wr与ibv_sge之间的关系可以查看链接里的示意图： https://zhuanlan.zhihu.com/p/55142568
    }
  }

  struct ibv_recv_wr *bad;
  for (int i = 0; i < kBatchCount; ++i) {
    if (ibv_post_recv(message, &recvs[i][0], &bad)) {  //由于先前将recvs[i][0]-recvs[i][subNR-1]穿在一起，因此会整批次提交
      Debug::notifyError("Receive failed.");
    }
  }
}  //具体来看就是，将每个message_size大小的空间放到每个ibv_sge上，然后为每个单独的ibv_sge，绑定到一个ibv_recv_wr上
//其后将每个batch开头的的ibv_recv_wr放到recv_qp上

char *AbstractMessageConnection::getMessage() {  //从当前recv_pool中，依据当前处理的curMessage位置，提取出message
  struct ibv_recv_wr *bad;
  char *m = (char *)messagePool + curMessage * MESSAGE_SIZE + recvPadding;//为何要再加上padding呢？
  //这里相当于默认message_size为96B，而padding再recv里有40字节，那么每次实际上是取用了message_size大小内的后56字节内容

  ADD_ROUND(curMessage, messageNR); //按照取余messageNR来递增curMessage

  if (curMessage % subNR == 0) {  //如果当前message已经到头了，就会将下一个批次的ibv_recv_wr提交到recv_qp上
    if (ibv_post_recv(
            message,
            &recvs[(curMessage / subNR - 1 + kBatchCount) % kBatchCount][0],
            &bad)) {
      Debug::notifyError("Receive failed.");//在get_message里将recv_wr提前放到message_pool上
    }
  }

  return m;
}

char *AbstractMessageConnection::getSendPool() {  //根据当前发送的message数量，获得sendPool的起始地址
  char *s = (char *)sendPool + curSend * MESSAGE_SIZE + sendPadding;    //在send_pool阶段，就已经是会额外偏移一个sendPadding的量了，因此后续
  //后续在获取send_message的时候，先减去一个sendPadding再加上一个sendPadding好像也没啥

  ADD_ROUND(curSend, messageNR);  //对于getSendPool的行为就没有预先将send_wr放到message_pool上

  return s;
}
