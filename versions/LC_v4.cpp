//这一版需要在submodel里设置1024长度的huge_page和2048长度的huge_page+512 uint64长度的stash

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
#include <unordered_map>
#include <map>
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
           std::cout<<"used "<<exist_keys.size()<<" keys. and epsilon is:"<<epsilon<<std::endl;
           uint64_t p=exist_keys[0];
           size_t pos=0;
           plr->add_point(p,pos);
           auto k_iter = exist_keys.begin();
           auto v_iter = exist_keys.begin();
          // std::cout<<sizeof(PLR)<<std::endl;
           for(int i=1; i<exist_keys.size(); i++) {
            uint64_t next_p = exist_keys[i];
            if (next_p == p){
                //LOG(5)<<"DUPLICATE keys";
                exit(0);
            }
            p = next_p;
            pos++;
            
            if(!plr->add_point(p, pos)||i==exist_keys.size()-1){   //如果add_point失败，则执行下面的操作     
              //  std::cout<<"i="<<i<<std::endl;       
                auto cs = plr->get_segment();
                std::pair<long double, long double>  cs_param= cs.get_slope_intercept(); 

                append_model(cs_param.first, cs_param.second, k_iter, v_iter, pos);    //待写
                
                k_iter += pos;
                v_iter += pos;
                pos=0;
                delete plr;
                PLR *plr = new PLR(epsilon-1);
              //  std::cout<<"here we start new PLR model"<<std::endl;
                plr->add_point(p, pos);
               // std::cout<<"i="<<i<<std::endl;
               // std::cout<<"used "<<exist_keys.size()<<" keys."<<std::endl;
            }
        }
           // models.train_up();
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
        std::cout<<"below is lower models prediction collision:"<<std::endl;
        std::map<int,int>::iterator it;
        for(it=total_times.begin();it!=total_times.end();it++){
            std::cout<<it->first<<" collisons accurs: "<<it->second<<std::endl;
        }
        int stash_num=0;
        for(int i=0;i<models.down.size();i++){
            std::cout<<"now the stash loc is: "<<models.down[i].leaf_backup->stash_loc<<std::endl;
        }
        for(int i=0;i<nonexist_keys.size();i++){
            uint64_t key_test=nonexist_keys[i];
           int loc_test;
           for(int i=0;i<models.up.size();i++){
            if(models.up[i].anchor_key>key_test){
                loc_test=(int)(models.up[i].slope*(double)(key_test)+models.up[i].intercept);
               //  std::cout<<"loc_test is :"<<loc_test<<std::endl;
                break;
            }
           }
          
        //   int p=loc_test-16>=0?loc_test-16:0;
         //  int end=loc_test+16<models.down.size()?loc_test+16:models.down.size();
        //   for(;p<end;p++){
            for(int p=loc_test-8;p<=loc_test+8;p++){
            bool is_insert=false;
            if(models.down[p].anchor_key>key_test){
                std::cout<<"hello??"<<std::endl;
                int loc=(int)(models.down[p].slope*(double)(key_test)+models.down[p].intercept);
                for(int j=0;j<8;j++){
                    if(models.down[p].leaf_test->records[loc].keys[j]==0){
                          models.down[p].leaf_test->records[loc].keys[j]=nonexist_keys[i];
                          is_insert=true;
                          break;
                    }
                }
                if(!is_insert){
                    for(int j=0;j<8;j++){
                        if(models.down[p].leaf_backup->records[(int)((1.5)*(models.down[p].slope*(double)(key_test)+models.down[p].intercept))].keys[j]==0){
                            models.down[p].leaf_backup->records[(int)((1.5)*(models.down[p].slope*(double)(key_test)+models.down[p].intercept))].keys[j]=nonexist_keys[i];
                            is_insert=true;
                            break;
                        }
                    }
                }
                
                if(!is_insert){
                    if(models.down[p].leaf_backup->stash_loc>=256){
                        std::cout<<"now it is: "<<models.down[p].leaf_backup->stash_loc<<std::endl;
                        is_insert=false;
                        
                    }else if(models.down[p].leaf_backup->stash_loc<256){
                    models.down[p].leaf_backup->stash_loc++;
                   // std::cout<<"here"<<std::endl;
                    std::cout<<"now it is: "<<models.down[p].leaf_backup->stash_loc<<std::endl;
                    models.down[p].leaf_backup->stash[models.down[p].leaf_backup->stash_loc]=nonexist_keys[i];
                 //   std::cout<<"or here"<<std::endl;
                    is_insert=true;
                    }
          
                }
                if(!is_insert){
                    stash_num++;
                }
            }
            if(is_insert){
                break;
            }
        }

 

        }
        std::cout<<"total there are "<<stash_num<<" keys will be insert in stash"<<std::endl;
        
        
        
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
    std::uniform_int_distribution<uint64_t> rand_uniform(10000, 1000000);
    exist_keys.reserve(LOAD_KEYS);
    for(size_t i=0; i<LOAD_KEYS; i++) {
        uint64_t a = rand_normal(gen)*1000000000000;  
       // uint64_t a = rand_uniform(gen);    //切换均匀分布数据集和正态分布数据集
        if(a<=0) {  //在生成端就把0去掉
            i--;
            continue;
        }
        exist_keys.push_back(a);
    }
    nonexist_keys.reserve(BACK_KEYS);
    for(size_t i=0; i<BACK_KEYS; i++) {
        uint64_t a = rand_normal(gen)*1000000000000;
    //uint64_t a = rand_uniform(gen);
        if(a<=0) {
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
       
      // sub.leaf_test=new Huge_Leaf_3;
       //sub.leaf_backup=new Huge_Leaf_4;

       //char *tb=(dsm->get_rbuf(0)).get_sibling_buffer();

       std::unordered_map<int,int> m;
       std::map<int,int> m_times;
       for(int i=0;i<size;i++){
      //  std::cout<<"here we begin insert."<<std::endl;
          auto temp_k=*(keys_begin+i);
          int pre_loc=(int)(slope*(double)(temp_k)+intercept);
          bool inserted=false;
          for(int j=0;j<8;j++){
             if(sub.leaf_test->records[pre_loc].keys[j]!=0){
                sub.leaf_test->records[pre_loc].keys[j]=temp_k;
                sub.leaf_test->records[pre_loc].vals[j]=temp_k;
                inserted=true;
                break;
             }
          }
        //  std::cout<<"here we test leaf_test."<<std::endl;
          if(!inserted){
            for(int j=0;j<8;j++){
                
                if(sub.leaf_backup->records[pre_loc].keys[j]==0){
                    sub.leaf_backup->records[pre_loc].keys[j]=temp_k;
                    sub.leaf_backup->records[pre_loc].vals[j]=temp_k;
                    inserted=true;
                    break;
                }
            }
          }
        //  std::cout<<"here we begin leaf_backup."<<std::endl;
        /*
          if(!inserted){

                sub.leaf_backup->stash[sub.leaf_backup->stash_loc]=temp_k;
                sub.leaf_backup->stash_loc++;
                std::cout<<"no it is: "<<sub.leaf_backup->stash_loc<<std::endl;
                if(sub.leaf_backup->stash_loc==256&&sub.leaf_backup->stash_loc==128){
                  exit(0);
                }
                inserted=true;
          }
*/

          m[pre_loc]++;
       }
       std::unordered_map<int,int>::iterator it;
       std::map<int,int>::iterator it_times;
       for(it=m.begin();it!=m.end();it++){
          m_times[it->second]++;
       }
       for(it_times=m_times.begin();it_times!=m_times.end();it_times++){
          //std::cout<<it_times->first<<" colision accurs "<<it_times->second<<" times"<<std::endl;
          if(total_times[it_times->first]==0){
            total_times[it_times->first]=it_times->second;
          }else{
            total_times[it_times->first]+=it_times->second;}
       }
     //  std::cout<<"new model avove."<<std::endl;
       
       
       models.down.push_back(sub);
     //  std::cout<<"submodel's stash loc is"<<sub.leaf_backup->stash_loc<<std::endl;

  //待写
   return;
}






