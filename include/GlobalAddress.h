#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"


class GlobalAddress {
public:

union {
  struct {
  uint64_t nodeID: 16;
  uint64_t offset : 48;
  };
  uint64_t val;  //val在这里是干啥的？是不是说，与地址，以及返回值共用一个空间，表明根据这个地址，查找的value
                 //回答，经过单元测试，猜测使用union是为了，紧凑内存地址，写入nodeID和offset两个变量，但实际上利用val进行原子传输和读取
  //单元测试表明，与上述struct共享内存地址的val
};
//    uint8_t mark;

 operator uint64_t() {
  return val;
}

  static GlobalAddress Null() {
    static GlobalAddress zero{0, 0};  //经过单元测试，得出，这里的0和0是用来初始化nodeID和offset的
    return zero;
  };
} __attribute__((packed));

static_assert(sizeof(GlobalAddress) == sizeof(uint64_t), "XXX");

inline GlobalAddress GADD(const GlobalAddress &addr, int off) {
  auto ret = addr;
  ret.offset += off;
  return ret;
}

inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const GlobalAddress &obj) {
  os << "[" << (int)obj.nodeID << ", " << obj.offset << "]";
  return os;
}

#endif /* __GLOBALADDRESS_H__ */
