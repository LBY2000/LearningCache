#include "Directory.h"
#include "Common.h"

#include "Connection.h"

//#include <gperftools/profiler.h>

GlobalAddress g_root_ptr = GlobalAddress::Null();
int g_root_level = -1;
bool enable_cache ;

Directory::Directory(DirectoryConnection *dCon, RemoteConnection *remoteInfo,
                     uint32_t ComputemachineNR, uint16_t dirID, uint16_t nodeID)
    : dCon(dCon), remoteInfo(remoteInfo), ComputeMachineNR(ComputemachineNR), dirID(dirID),
      nodeID(nodeID), dirTh(nullptr) {

  { // chunck alloctor
    GlobalAddress dsm_start;
    uint64_t per_directory_dsm_size = dCon->dsmSize / NR_DIRECTORY;
    dsm_start.nodeID = nodeID;
    dsm_start.offset = per_directory_dsm_size * dirID;
    chunckAlloc = new GlobalAllocator(dsm_start, per_directory_dsm_size);
  }

  dirTh = new std::thread(&Directory::dirThread, this);
}

Directory::~Directory() { delete chunckAlloc; }

void Directory::dirThread() {

  bindCore(23 - dirID);
  Debug::notifyInfo("dir %d launch!\n", dirID);

  while (true) {
    struct ibv_wc wc;
    pollWithCQ(dCon->cq, 1, &wc);

    switch (int(wc.opcode)) {
    case IBV_WC_RECV: // control message
    {

      auto *m = (RawMessage *)dCon->message->getMessage();

      process_message(m);

      break;
    }
    case IBV_WC_RDMA_WRITE: {
      break;
    }
    case IBV_WC_RECV_RDMA_WITH_IMM: {

      break;
    }
    default:
      assert(false);
    }
  }
}

void Directory::process_message(const RawMessage *m) {
  RawMessage *send = nullptr;
  switch (m->type) {
  case RpcType::MALLOC: {

    send = (RawMessage *)dCon->message->getSendPool();

    send->addr = chunckAlloc->alloc_chunck();
    break;
  }

  case RpcType::NEW_ROOT: {

    if (g_root_level < m->level) {
      g_root_ptr = m->addr;
      g_root_level = m->level;
      // can not set as 1. if set as 1 then the root node will be cached and no page search
      // will go through it, then it will trigger a bug in function insert internal.
      if (g_root_level >= 4) {
        enable_cache = true;
      }
    }

    break;
  }

  default:
    assert(false);
  }

  if (send) {
    dCon->sendMessage2App(send, m->node_id, m->app_id);
  }
}