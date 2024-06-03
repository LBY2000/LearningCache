//这个版本的是实现了依靠扩大后的sibling节点进行远端读和远端写
//目前开发版本从此替换


#include "LC.h"
#include "HotBuffer.h"
#include "IndexCache.h"
#include "RdmaBuffer.h"
#include "Timer.h"


#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>


LC::LC(DSM *dsm):dsm(dsm),LC_id(0){
        assert(dsm->is_register());
        auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer(); 
        auto root_addr = dsm->alloc(kLeafPageSize);
        auto root_page = new (page_buffer) LeafPage;
        auto &r = root_page->records[3];
        r.value=15;
        root_page->set_consistent();
        dsm->write_sync(page_buffer, root_addr, kLeafPageSize); 
        auto page_buffer_read = (dsm->get_rbuf(1)).get_page_buffer(); 
        dsm->read_sync(page_buffer_read, root_addr, kLeafPageSize, nullptr);
        auto read_page=(LeafPage *)page_buffer_read;
        auto &r2 = read_page->records[3];
        std::cout<<r2.value<<std::endl;  
        GlobalAddress root_ptr_ptr=get_root_ptr_ptr();


        if(dsm->getMyNodeID()==0){  //由集群节点0发起bulk_loading操作
           //进行bulk_loading操作, 本端加载一堆内容，然后排序，重训练，将排序后的KV写到远端DSM里，之后分配得到Index的lowwer_bound内容并写入
           //注意lower_bound会划分好Leaf_node的区域，然后将这些地址作为指针跳转项，一并写入
           //之后分配Upper_bound，并按照相同办法写入
           uint64_t epsilon=16;
           PLR *plr=new PLR(epsilon-1);
           this->load_data();
           std::sort(exist_keys.begin(), exist_keys.end());
           exist_keys.erase(std::unique(exist_keys.begin(), exist_keys.end()), exist_keys.end());
           std::sort(exist_keys.begin(), exist_keys.end());
           uint64_t p=exist_keys[0];
           size_t pos=0;
           plr->add_point(p,pos);
           auto k_iter = exist_keys.begin();
           auto v_iter = exist_keys.begin();

           for(int i=1; i<exist_keys.size(); i++) {
            uint64_t next_p = exist_keys[i];
            if (next_p == p){
                //LOG(5)<<"DUPLICATE keys";
                exit(0);
            }
            p = next_p;
            pos++;
            
            if(!plr->add_point(p, pos)||i==exist_keys.size()-1){   //如果add_point失败，则执行下面的操作            
                auto cs = plr->get_segment();
                std::pair<long double, long double>  cs_param= cs.get_slope_intercept(); 
                append_model(cs_param.first, cs_param.second, k_iter, v_iter, pos);    //待写append_model的变化代码
                k_iter += pos;
                v_iter += pos;
                pos=0;
                plr = new PLR(epsilon-1);
                plr->add_point(p, pos);
            }
        }
            models.train_up();
            std::cout<<"used down models:"<<models.down.size()<<std::endl;
            std::cout<<"used up models:"<<models.up.size()<<std::endl;
 


            
            //以上为bulk_loading部分

        }
        dsm->barrier("bulk_loading");
        printf("bulk_loading over.But lack of push to remote.\n");

        if(dsm->getMyNodeID()!=0){  //这里是非节点0的节点发起拉取同步的操作
            //节点拉取bulk_loading的index









        }
        dsm->barrier("LI_sync"); //至此完成bulk_loading阶段
        std::cout<<"everything ok"<<std::endl;
        uint64_t key_test = exist_keys[1000];
        std::cout<<"the key we test is:"<<key_test<<std::endl;
        int loc_test;
        for(int i=0;i<models.up.size();i++){
            if(models.up[i].anchor_key>key_test){
                loc_test=(int)(models.up[i].slope*(double)(key_test)+models.up[i].intercept);
                break;
            }
        }
        for(int i=loc_test-8;i<=loc_test+8;i++){
            if(models.down[i].anchor_key>key_test){
                char *tb=(dsm->get_rbuf(0)).get_sibling_buffer();
                Huge_Leaf_3 *read_buffer=new(tb)Huge_Leaf_3;
                dsm->read_sync((char *)read_buffer, models.down[i].leaf_ptr, sizeof(Huge_Leaf_3), nullptr);
                int loc=(int)(models.down[i].slope*(double)(key_test)+models.down[i].intercept);
                for(int j=0;j<8;j++){
                    if(read_buffer->records[loc].keys[j]==key_test){
                        std::cout<<"we find it and the val is:"<<read_buffer->records[loc].vals[j]<<std::endl;
                    }
                }
            }
        }

        
        
        
        
        //先进行初次的排序
        
        //去掉随机生成的重复KV
        
       //利用subModel类来进行管理，将其变成一个个的子model进行管理
       //后续将训练完成的叶先写到远端，并记录下位置，其后构建level1的index，并训练level2,将level2写入root_ptr_ptr
       


}

GlobalAddress LC::get_root_ptr_ptr(){
  GlobalAddress addr;
  addr.nodeID = 0;  
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * LC_id;
      //这里是16MB+0*sizeof(GlobalAddress)
      //这里KrootPointerStoreOffset设置为KChunkSize/2，按照多颗树的情况来分，其中chunksize是在MN端进行区分的一个概念

  return addr;
}

void LC::load_data(){  //在这里切换不同负载，现在只考虑一种生成负载
    normal_data();
    std::cout << "==== LOAD normal ====="<<std::endl;
};

void LC::normal_data() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<double> rand_normal(4, 2);

    exist_keys.reserve(LOAD_KEYS);
    for(size_t i=0; i<LOAD_KEYS; i++) {
        uint64_t a = rand_normal(gen)*1000000000000;
        if(a<=0) {  //在生成端就把0去掉
            i--;
            continue;
        }
        exist_keys.push_back(a);
    }
    nonexist_keys.reserve(BACK_KEYS);
    for(size_t i=0; i<BACK_KEYS; i++) {
        uint64_t a = rand_normal(gen)*1000000000000;
        if(a<0) {
            i--;
            continue;
        }
        nonexist_keys.push_back(a);
    }
}

void LC::append_model(double slope, double intercept,
                     const typename std::vector<uint64_t>::const_iterator &keys_begin,
                     const typename std::vector<uint64_t>::const_iterator &vals_begin, 
                     size_t size){
       SubModel sub;
       sub.slope=slope;
       sub.intercept=intercept;
       sub.leaf_size=1024;
       auto k=*(keys_begin+size-1);
       sub.anchor_key=k;
       char *tb=(dsm->get_rbuf(0)).get_sibling_buffer();
       Huge_Leaf_3 *leaf=new (tb)Huge_Leaf_3;
       for(int i=0;i<1024;i++){ //默认无0这个key
           for(int j=0;j<8;j++){
             leaf->records[i].keys[j]=0;  //因为是无符号数，所以不能用负数来作为初始值
             leaf->records[i].vals[j]=0;
           }
       }
       for(int i=0;i<size;i++){
          auto temp_k=*(keys_begin+i);
          int pre_loc=(int)(slope*(double)(temp_k)+intercept);
          for(int j=0;j<8;j++){
            if(leaf->records[pre_loc].keys[j]==0){
                leaf->records[pre_loc].keys[j]=temp_k;
                leaf->records[pre_loc].vals[j]=temp_k+3;
                break;
            }}}
       auto leaf_addr = dsm->alloc(sizeof(Huge_Leaf_3));
       sub.leaf_ptr=leaf_addr;
       dsm->write_sync((char *)leaf, leaf_addr, sizeof(Huge_Leaf_3)); 
       models.down.push_back(sub);
  //待写结构调整部分
   return;
}






















