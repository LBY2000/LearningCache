#include <math.h>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <stdint.h>
#include <iostream>
#include <vector>
#include "DSM.h"
#include "Common.h"



class SubModel{
    friend class LC;
    public:
      uint64_t anchor_key; //单个子model内最大的key值
      double slope,intercept;
      GlobalAddress leaf_ptr;
};

class Upper_Model{  //目前只考虑静态的情况
    friend class LC;
    public:
       uint64_t anchor_key;
       double slope,intercept;
       std::vector<SubModel> down;

};

class Top_Model{
    friend class LC;
    public:
      uint64_t anchor_key;
      double slope,intercept;
      int offset;
};


class Models{  //submodel的集合体
    friend class LC;
    public:
    std::vector<SubModel> down;
    std::vector<Upper_Model> up;
    std::vector<Top_Model> top;
    uint64_t upper_epsilon;
    uint64_t down_epsilon;
    Models(){
        this->upper_epsilon=4;
        this->down_epsilon=8;
    }
    void bulkload_train(std::vector<uint64_t> &keys,std::vector<uint64_t> &vals,DSM *dsm);
    void bulkload_train_up();
    void bulkload_train_top();
    void append_model(double slope, double intercept,
                     const typename std::vector<uint64_t>::const_iterator &keys_begin,
                     const typename std::vector<uint64_t>::const_iterator &vals_begin, 
                     size_t size,DSM *dsm);
};





