//此版本包含了简易的LC demo以及tree_test以及Hash_test的源码，还有单向model_serach而不是batch_search的模块


#include "LC.h"
#include "HotBuffer.h"
#include "IndexCache.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "Tree.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <ctime>

uint64_t kkKeySpace = 1024*1024*1024;
inline Key lc_to_key(uint64_t k){  //city_hash形成随机key编码
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kkKeySpace;
}

int num_less=0,num_greater=0,num_mid=0;
#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

//计算结构体内的成员在结构体中的偏移量
thread_local GlobalAddress path_stack_lc[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];
thread_local std::queue<uint16_t> hot_wait_queue_lc;
extern bool enable_cache;

LC::LC(DSM *dsm):dsm(dsm),LC_id(0){     //在这里还需要补充tree_index的构造函数中要做的事情
/*
        struct timespec start, end;
        uint64_t elapsed;
        uint64_t ke=19;
        clock_gettime(CLOCK_MONOTONIC, &start);  
        murmur2((char *)&ke, sizeof(ke));
        clock_gettime(CLOCK_MONOTONIC, &end);
        elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
        std::cout <<"hash 耗时 "<< elapsed << "   nsec\t" <<  std::endl;
*/
    //    clock_gettime(CLOCK_MONOTONIC, &end);
     //   elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	 //   std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(3900000/(elapsed/1000.0))) << "\tOps/sec\tReadOps" << std::endl;


        

        std::cout<<"asda"<<std::endl;
        LeafPage l;
        l.debug();
        
        
        assert(dsm->is_register());
        char *temp=(dsm->get_rbuf(0)).get_entry_buffer();
        LeafNode *lf=new(temp)LeafNode;
        lf->front_buckets[117].entry[0].key=117;
        lf->front_buckets[117].entry[0].val=120;
        lf->front_buckets[110].entry[0].key=110;
        lf->front_buckets[110].entry[0].val=113;
        auto test_addr = dsm->alloc(sizeof(LeafNode));
        dsm->write_sync(temp, test_addr, sizeof(LeafNode));




        auto page_buffer_read2 = (dsm->get_rbuf(0)).get_page_buffer(); 
        auto page_buffer_read3 = (dsm->get_rbuf(0)).get_page_buffer(); 
        GlobalAddress page_loc2=GADD(test_addr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*110);
        GlobalAddress page_loc3=GADD(test_addr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*117);
        GlobalAddress loc_addr=GADD(test_addr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*117);
        auto loc_loc = (dsm->get_rbuf(0)).get_cas_buffer();
        bool cas_right=this->try_lock(loc_addr, 0, 1, loc_loc);
        *loc_loc=0;
        // dsm->LI_write_dm((char *)loc_loc, loc_addr, sizeof(uint64_t), false);
        this->try_unlock((char *)loc_loc,loc_addr,sizeof(uint64_t),false);


        auto page_buffer_read = (dsm->get_rbuf(0)).get_page_buffer(); 
        dsm->read_sync(page_buffer_read, GADD(test_addr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*117), sizeof(LeafBucket), nullptr);
        auto read_page=(LeafBucket *)page_buffer_read;
        auto &r1 = read_page->entry[0];
        std::cout<<"test1 key is:"<<r1.key<<std::endl;
        std::cout<<"test1 val is:"<<r1.val<<std::endl;   
        std::cout<<"loc num is:"<<read_page->bucket_lock<<std::endl;




        auto page_buffer_read4 = (dsm->get_rbuf(0)).get_page_buffer(); 
        auto page_buffer_read5 = (dsm->get_rbuf(0)).get_page_buffer(); 
        GlobalAddress page_loc4=GADD(test_addr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*111);
        GlobalAddress page_loc5=GADD(test_addr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*118);




        RdmaOpRegion rs[2];
        rs[0].source = (uint64_t)page_buffer_read2;
        rs[0].dest = (uint64_t)page_loc2;
        rs[0].size = sizeof(LeafBucket);
        rs[0].is_lock_mr = false;
        rs[1].source = (uint64_t)page_buffer_read3;
        rs[1].dest = (uint64_t)page_loc3;
        rs[1].size = sizeof(LeafBucket);
        rs[1].is_lock_mr = false;
        std::cout<<"tag1"<<std::endl;
        dsm->read_batch(rs, 2,true);

        std::cout<<"tag2"<<std::endl;
        auto read_page2=(LeafBucket *)page_buffer_read2;
        auto &r2 = read_page2->entry[0];
        std::cout<<"test2 key is:"<<r2.key<<std::endl;
        std::cout<<"test2 val is:"<<r2.val<<std::endl;

        auto read_page3=(LeafBucket *)page_buffer_read3;
        auto &r3 = read_page3->entry[0];
        std::cout<<"test3 key is:"<<r3.key<<std::endl;
        std::cout<<"test3 val is:"<<r3.val<<std::endl;

         
      //  dsm->read_batch(rs, 2,true);
        




        
        std::cout << "==== origin test over ====="<<std::endl;
        
        if(dsm->getMyNodeID()==0){  //由集群节点0发起bulk_loading操作
           //进行bulk_loading操作, 本端加载一堆内容，然后排序，重训练，将排序后的KV写到远端DSM里，之后分配得到Index的lowwer_bound内容并写入
           //注意lower_bound会划分好Leaf_node的区域，然后将这些地址作为指针跳转项，一并写入
           //之后分配Upper_bound，并按照相同办法写入

            this->load_data();
            std::sort(exist_keys.begin(), exist_keys.end());
            exist_keys.erase(std::unique(exist_keys.begin(), exist_keys.end()), exist_keys.end());  //去重
            nonexist_keys.erase(std::unique(nonexist_keys.begin(), nonexist_keys.end()), nonexist_keys.end());
            std::sort(exist_keys.begin(), exist_keys.end());
            this->models.bulkload_train(exist_keys,exist_keys,this->dsm);
            //以上为bulk_loading部分

        }


        dsm->barrier("bulk_loading");
        printf("bulk_loading over.But lack of push IndexStructure to remote.\n");

        if(dsm->getMyNodeID()!=0){  //这里是非节点0的节点发起拉取同步的操作
            //节点拉取bulk_loading的index




        }
        dsm->barrier("LI_sync"); //至此完成bulk_loading阶段
        std::cout<<"LI_sync over, and everything ok"<<std::endl;
        uint64_t key_test = exist_keys[45];
        std::cout<<"the key we test is:"<<key_test<<std::endl;
        uint64_t val_test;
        if(this->search(key_test,val_test)){
           std::cout<<"we find it and the val is:"<<val_test<<std::endl;
        }
        val_test=1;
        std::cout<<"below is upper_search:"<<std::endl;
        if(this->upper_search(key_test,val_test)){
           std::cout<<"we find it and the val is:"<<val_test<<std::endl;
        }else{
           std::cout<<"upper search failed."<<std::endl;
        }
        std::cout<<"below is model_search:"<<std::endl;
        if(this->model_search(key_test,val_test)){
           std::cout<<"we find it and the val is:"<<val_test<<std::endl;
        }else{
           std::cout<<"model search failed."<<std::endl;
        }
        uint64_t key_insert=key_test+1234,val_insert=6666;
        uint64_t val_insert_test;
        if(this->insert(key_insert,val_insert)){
            std::cout<<"succeed to insert the val."<<std::endl;
            if(this->search(key_insert,val_insert_test)){
            std::cout<<"we find it and the val is:"<<val_insert_test<<std::endl;
        }
        }

        key_insert=13213;
        val_insert=88888;
        if(this->insert(key_insert,val_insert)){
            std::cout<<"succeed to insert the val."<<std::endl;
            if(this->search(key_insert,val_insert_test)){
            std::cout<<"we find it and the val is:"<<val_insert_test<<std::endl;
        }
        }else{
            std::cout<<"we failed to insert."<<std::endl;
        }
        int suc=0,non_suc=0;

        for(int i=0;i<nonexist_keys.size();i++){
            if(this->model_insert(nonexist_keys[i],val_insert)){
                suc++;
            }else{
                non_suc++;
            }

        }
        std::cout<<"the succeed inserted num is:"<<suc<<std::endl;
        std::cout<<"the non succeed inserted num is:"<<non_suc<<std::endl; 
        std::cout<<"now start localpredict-test: "<<std::endl;
      //  int i=30000;
     //   for(;i<30020;i++){
     //       double loc=this->local_double_predict(exist_keys[i]);
    //        std::cout<<loc<<std::endl;
    //    }
     //   clock_gettime(CLOCK_MONOTONIC, &start);  吞吐测试
     //   for(int i=1001;i<=3901000;i++){
    //        this->model_search(exist_keys[i],val_test);
    //    }
    //    clock_gettime(CLOCK_MONOTONIC, &end);
     //   elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	 //   std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(3900000/(elapsed/1000.0))) << "\tOps/sec\tReadOps" << std::endl;




        
        //先进行初次的排序
        //去掉随机生成的重复KV
       //利用subModel类来进行管理，将其变成一个个的子model进行管理
       //后续将训练完成的叶先写到远端，并记录下位置，其后构建level1的index，并训练level2,将level2写入root_ptr_ptr





       //下面的是给tree用的初始化代码
       
          for (int i = 0; i < dsm->getClusterSize(); ++i) {
            local_locks[i] = new LocalLockNode[define::kNumOfLock];
            for (size_t k = 0; k < define::kNumOfLock; ++k) {
                auto &n = local_locks[i][k];
                n.ticket_lock.store(0);
                n.hand_over = false;
                n.hand_time = 0;
            }
        }
        assert(dsm->is_register());
        index_cache = new IndexCache(define::kIndexCacheSize);
        std::cout<<"这里index cacahe的all_page是:"<<index_cache->all_page_cnt<<std::endl;
        
        root_ptr_ptr = get_root_ptr_ptr();
        auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer(); 
        auto root_addr = dsm->alloc(kLeafPageSize);
        auto root_page = new (page_buffer) LeafPage;
        root_page->set_consistent();
        dsm->write_sync(page_buffer, root_addr, kLeafPageSize); 
        auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
        bool res = dsm->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
        if (res){
            std::cout << "Tree root pointer value " << root_addr << std::endl;
        }else{
            std::cout << "fail\n";
        }
         if (dsm->getMyNodeID() == 0) {
            for (uint64_t i = 1; i < 10240; ++i) {
            this->tree_insert(lc_to_key(i),i);
            }
        } 


}

GlobalAddress LC::get_root_ptr_ptr(){
  GlobalAddress addr;
  addr.nodeID = 0;  
  addr.offset =define::kRootPointerStoreOffest + sizeof(GlobalAddress) * LC_id;
      //这里是MB+0*sizeof(GlobalAddress)
      //这里KrootPointerStoreOffset设置为KChunkSize/2，按照多颗树的情况来分，其中chunksize是在MN端进行区分的一个概念

  return addr;
}

void LC::load_data(){  //在这里切换不同负载，现在只考虑一种生成负载
    normal_data();
    std::cout << "==== LOAD normal ====="<<std::endl;
};

void LC::normal_data(){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<double> rand_normal(0, 1);

    exist_keys.reserve(LOAD_KEYS);
    for(size_t i=0; i<LOAD_KEYS; i++) {
        double b=rand_normal(gen);
        if(b<0){
            b*=-1;
        }
        uint64_t a = b*1000000000000;
        if(a<=0) {  //在生成端就把0去掉
            i--;
            continue;
        }
        exist_keys.push_back(a);
    }
    nonexist_keys.reserve(BACK_KEYS);
    for(size_t i=0; i<BACK_KEYS; i++){
        double b=rand_normal(gen);
        if(b<0){
            b*=-1;
        }
        uint64_t a = b*1000000000000;
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
    // sub.leaf_size=1024;
       auto k=*(keys_begin+size-1);
       sub.anchor_key=k;
    //   char *tb=(dsm->get_rbuf(0)).get_sibling_buffer();
       char *tb=(dsm->get_rbuf(0)).get_entry_buffer();
       LeafNode *leaf=new (tb)LeafNode;

       for(int i=0;i<size;i++){
          auto temp_k=*(keys_begin+i);
          int pre_loc=(int)(slope*(double)(temp_k)+intercept);

          for(int j=0;j<BUCKET_SLOTS;j++){
            if(leaf->front_buckets[pre_loc].entry[j].key==0){
                leaf->front_buckets[pre_loc].entry[j].key=temp_k;
                leaf->front_buckets[pre_loc].entry[j].val=temp_k+3;
                break;
            }}}
       auto leaf_addr = dsm->alloc(sizeof(LeafNode));
       sub.leaf_ptr=leaf_addr;
       dsm->write_sync((char *)leaf, leaf_addr, sizeof(LeafNode)); 
       models.down.push_back(sub);
  //待写结构调整部分
   return;
}

bool LC::search(uint64_t &key,uint64_t &find_val){  //down_level_search
        for(int i=0;i<models.down.size();i++){
            if(models.down[i].anchor_key>key){
        //  std::cout<<"we find anchor key is:"<<models.down[i].anchor_key<<std::endl;
            int loc=(int)(models.down[i].slope*(double)(key)+models.down[i].intercept);
            char *tb=(dsm->get_rbuf(0)).get_page_buffer();
            LeafBucket *read_buffer=new(tb)LeafBucket;
            GlobalAddress page_loc=GADD(models.down[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc);
            dsm->read_sync((char *)read_buffer, page_loc, sizeof(LeafBucket), nullptr);
            for(int j=0;j<BUCKET_SLOTS;j++){
                if(read_buffer->entry[j].key==key){
                    find_val=read_buffer->entry[j].val;
                    return true;
                }
            }
            break;
        }
    }
    return false;
}
bool LC::insert(uint64_t key,uint64_t val){
    int repeat=0;
     for(int i=0;i<models.down.size();i++){
        if(models.down[i].anchor_key>key){

           int loc=(int)(models.down[i].slope*(double)(key)+models.down[i].intercept);
           if(loc<0){
              loc=0;
           }

           GlobalAddress loc_addr=GADD(models.down[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc);
           auto loc_loc = (dsm->get_rbuf(0)).get_cas_buffer();
           while(1){
              bool cas_right=dsm->LI_cas_dm_sync(loc_addr, 0, 1, loc_loc);
              if(cas_right){
                break;
              }else{
              //  std::cout<<"cas wrong"<<std::endl;
                repeat++;
                if(repeat>5){
                    return false;
                }
                continue;
              }
           }
           char *read_buffer=(dsm->get_rbuf(0)).get_page_buffer();
           dsm->read_sync(read_buffer, loc_addr, sizeof(LeafBucket), nullptr);
           auto page = (LeafBucket *)read_buffer;
      //     assert(page->check_consistent());
           for(int j=0;j<BUCKET_SLOTS;j++){
                if(page->entry[j].key==key){
                  //  std::cout<<"key already exist."<<std::endl;
                    *loc_loc=0;
                    dsm->LI_write_dm((char *)loc_loc, loc_addr, sizeof(uint64_t), false);
                    return false;
                }else if(page->entry[j].key==0){
                    page->entry[j].key=key;
                    page->entry[j].val=val;
                    page->set_consistent();
                    break;
                }else{
                    if(j==15){
                  //      std::cout<<"front bucket is full."<<std::endl;
                        *loc_loc=0;
                        dsm->LI_write_dm((char *)loc_loc, loc_addr, sizeof(uint64_t), false);
                        return false;
                    }
                }
            }
            dsm->write_sync(read_buffer, loc_addr, sizeof(LeafBucket));  //将buckets写入远端


//以下为解锁
           *loc_loc=0;
           dsm->LI_write_dm((char *)loc_loc, loc_addr, sizeof(uint64_t), false);
           return true;
           
        }
     }



}
bool LC::try_lock(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroContext *ctx){
    return dsm->LI_cas_dm_sync(gaddr, 0, 1, rdma_buffer);

}
void LC::try_unlock(char *buffer, GlobalAddress gaddr, size_t size,bool signal, CoroContext *ctx){
     dsm->LI_write_dm(buffer, gaddr, size, signal);
};

bool LC::model_search(uint64_t &key,uint64_t &val){  //将这里的model_search改为read_batch的版本

    int loc_up=-100,loc_up_pre,loc_down,loc_pre;
    for(int i=0;i<models.top.size();i++){
       if(models.top[i].anchor_key>key){
          loc_up= (int)(models.top[i].slope*(double)(key)+models.top[i].intercept);
          if(loc_up<0){
            loc_up=0;
          }
          loc_up+=models.top[i].offset; 
          break;
       } 
    }

    for(int i=loc_up-4;i<=loc_up+4;i++){
        if(i<0){
            continue;
        }
        if(models.up[i].anchor_key>key){
            loc_down=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            loc_up_pre=i;
            if(loc_down<0){
                loc_down=0;
            }
            break;
        }
    }
    for(int i=loc_down-4;i<=loc_down+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up_pre].down)[i].anchor_key>key){
            loc_pre=(int)((models.up[loc_up_pre].down)[i].slope*(double)(key)+(models.up[loc_up_pre].down)[i].intercept);
            //待写，read_batch的过程，批量读过来然后检查
             
            char *read_buffer=(dsm->get_rbuf(0)).get_page_buffer();
            GlobalAddress page_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc_pre);
            dsm->read_sync((char *)read_buffer, page_loc, sizeof(LeafBucket), nullptr);
            for(int j=0;j<BUCKET_SLOTS;j++){
                if(((LeafBucket*)read_buffer)->entry[j].key==key){
                    val=((LeafBucket*)read_buffer)->entry[j].val;
                    return true;
                }else if(((LeafBucket*)read_buffer)->entry[j].key==0){
                    val=0;
                    return false;
                }
            }
            break;
        }
    } 
    

    return false;
};
bool LC::model_insert(uint64_t key,uint64_t val){   //同理也将这里的model_insert改为带backup版本的
//待写   
//新版本idea是根据后续的小数点决定是存front还是存backup??
     //这一版将写完
     int repeat=0;
     int loc_up=-100,loc_up_pre,loc_down,loc_pre;
    for(int i=0;i<models.top.size();i++){
       if(models.top[i].anchor_key>key){
          loc_up= (int)(models.top[i].slope*(double)(key)+models.top[i].intercept);
          if(loc_up<0){
            loc_up=0;
          }
          loc_up+=models.top[i].offset; 
          break;
       } 
    }
    for(int i=loc_up-4;i<=loc_up+4;i++){
        if(i<0){
            continue;
        }
        if(models.up[i].anchor_key>key){
            loc_down=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            loc_up_pre=i;
            if(loc_down<0){
                loc_down=0;
            }
            break;
        }
    }
    for(int i=loc_down-4;i<=loc_down+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up_pre].down)[i].anchor_key>key){
            
            //后续整个的加锁和解锁过程就在这里完成
    

            loc_pre=(int)((models.up[loc_up_pre].down)[i].slope*(double)(key)+(models.up[loc_up_pre].down)[i].intercept);
            
            GlobalAddress front_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc_pre);
            GlobalAddress back_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,backup_buckets)+sizeof(LeafBucket)*(loc_pre/4));
            auto lock_f = (dsm->get_rbuf(0)).get_cas_buffer();
            auto lock_r = (dsm->get_rbuf(0)).get_cas_buffer();
            //这里在计算完地址后应该先对远端进行写锁
            while(1){
              bool cas_right=dsm->LI_cas_dm_sync(front_loc, 0, 1, lock_f);
              if(cas_right){
                break;
              }else{
              //  std::cout<<"cas wrong"<<std::endl;
                repeat++;
                if(repeat>4){
                    return false;
                }
                continue;
              }
           }
           while(1){
              bool cas_right=dsm->LI_cas_dm_sync(back_loc, 0, 1, lock_r);
              if(cas_right){
                break;
              }else{
              //  std::cout<<"cas wrong"<<std::endl;
                repeat++;
                if(repeat>6){
                    return false;
                }
                continue;
              }
           }
           //接下来发起read_batch;
           auto page_buffer_front = (dsm->get_rbuf(0)).get_page_buffer(); 
           auto page_buffer_back = (dsm->get_rbuf(0)).get_page_buffer();
           RdmaOpRegion rs[2];
           rs[0].source = (uint64_t)page_buffer_front;
           rs[0].dest = (uint64_t)front_loc;
           rs[0].size = sizeof(LeafBucket);
           rs[0].is_lock_mr = false;
           rs[1].source = (uint64_t)page_buffer_back;
           rs[1].dest = (uint64_t)back_loc;
           rs[1].size = sizeof(LeafBucket);
           rs[1].is_lock_mr = false;
           
           dsm->read_batch(rs, 2,true);
      
           if((!((LeafBucket *)page_buffer_front)->check_consistent())&&(!((LeafBucket *)page_buffer_back)->check_consistent())){
               return false;
           }
           bool is_full=false;
           for(int i=0;i<BUCKET_SLOTS;i++){
               if(((LeafBucket *)page_buffer_front)->entry[i].key==key){
                  *lock_f=0;
                  dsm->LI_write_dm((char *)lock_f, front_loc, sizeof(uint64_t), false);
                  dsm->LI_write_dm((char *)lock_f, back_loc, sizeof(uint64_t), false);
                  return false;

               }else if(((LeafBucket *)page_buffer_front)->entry[i].key==0){
                  ((LeafBucket *)page_buffer_front)->entry[i].key=key;
                  ((LeafBucket *)page_buffer_front)->entry[i].val=val;
                  ((LeafBucket *)page_buffer_front)->set_consistent();
                  dsm->write_sync(page_buffer_front, front_loc, sizeof(LeafBucket));
                  break;
               }else{
                  if(i==15){
                     is_full=true;
                  }
               }
           }
            if(is_full){
                for(int i=0;i<BUCKET_SLOTS;i++){
                    if(((LeafBucket *)page_buffer_back)->entry[i].key==key){
                        *lock_r=0;
                        dsm->LI_write_dm((char *)lock_r, front_loc, sizeof(uint64_t), false);
                        dsm->LI_write_dm((char *)lock_r, back_loc, sizeof(uint64_t), false);
                        return false;

                    }else if(((LeafBucket *)page_buffer_back)->entry[i].key==0){
                        ((LeafBucket *)page_buffer_back)->entry[i].key=key;
                        ((LeafBucket *)page_buffer_back)->entry[i].val=val;
                        ((LeafBucket *)page_buffer_back)->set_consistent();
                        is_full=false;
                        dsm->write_sync(page_buffer_back, back_loc, sizeof(LeafBucket));
                        break;
                    }else{
                        if(i==15){
                            is_full=true;
                        }
                    }
                }
            }
            //正常给远端解锁
            *lock_r=0;
            dsm->LI_write_dm((char *)lock_r, front_loc, sizeof(uint64_t), false);
            dsm->LI_write_dm((char *)lock_r, back_loc, sizeof(uint64_t), false);

            if(is_full){
               return false;
            }else{
               return true; 
            }
                
        }
    } 



     return false;
}//
bool LC::upper_search(uint64_t &key,uint64_t &val){
//待写
    int loc_up,loc_up_pre,loc_down,loc_pre;
    for(int i=0;i<models.up.size();i++){
       // std::cout<<models.up[i].anchor_key<<std::endl;
        if(models.up[i].anchor_key>key){
            loc_up=i;
            loc_up_pre=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            std::cout<<"upper loc in upper search is "<<i<<std::endl;
            break;
        }
    }
  //  std::cout<<"predict upper loc: "<<loc_up<<std::endl;
 //   std::cout<<"predicted anchor "<<(models.up[loc_up].down)[loc_up_pre].anchor_key<<std::endl;
 //   std::cout<<"previous anchor "<<(models.up[loc_up-1].down)[loc_up_pre].anchor_key<<std::endl;
    for(int i=loc_up_pre-4;i<=loc_up_pre+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up].down)[i].anchor_key>key){
            loc_pre=(int)((models.up[loc_up].down)[i].slope*(double)(key)+(models.up[loc_up].down)[i].intercept);
            char *tb=(dsm->get_rbuf(0)).get_page_buffer();
            LeafBucket *read_buffer=new(tb)LeafBucket;
            GlobalAddress page_loc=GADD((models.up[loc_up].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc_pre);
            dsm->read_sync((char *)read_buffer, page_loc, sizeof(LeafBucket), nullptr);
            for(int j=0;j<BUCKET_SLOTS;j++){
                if(read_buffer->entry[j].key==key){
                    val=read_buffer->entry[j].val;
                    return true;
                }
            }
            break;
        }
    } 

    return false;
};


bool LC::model_batch_search(uint64_t key,uint64_t &val){  //将这里的model_search改为read_batch的版本

    int loc_up=-100,loc_up_pre,loc_down,loc_pre;
    for(int i=0;i<models.top.size();i++){
       if(models.top[i].anchor_key>key){
          loc_up= (int)(models.top[i].slope*(double)(key)+models.top[i].intercept);
          if(loc_up<0){
            loc_up=0;
          }
          loc_up+=models.top[i].offset; 
          break;
       } 
    }

    for(int i=loc_up-4;i<=loc_up+4;i++){
        if(i<0){
            continue;
        }
        if(models.up[i].anchor_key>key){
            loc_down=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            loc_up_pre=i;
            if(loc_down<0){
                loc_down=0;
            }
            break;
        }
    }
    for(int i=loc_down-4;i<=loc_down+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up_pre].down)[i].anchor_key>key){
            loc_pre=(int)((models.up[loc_up_pre].down)[i].slope*(double)(key)+(models.up[loc_up_pre].down)[i].intercept);
            //待写，read_batch的过程，批量读过来然后检查
             
            char *front_buffer=(dsm->get_rbuf(0)).get_page_buffer();
            char *back_buffer=(dsm->get_rbuf(0)).get_page_buffer();
            GlobalAddress front_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc_pre);
            GlobalAddress back_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,backup_buckets)+sizeof(LeafBucket)*(loc_pre/4));

            RdmaOpRegion rs[2];
            rs[0].source = (uint64_t)front_buffer;
            rs[0].dest = (uint64_t)front_loc;
            rs[0].size = sizeof(LeafBucket);
            rs[0].is_lock_mr = false;
            rs[1].source = (uint64_t)back_buffer;
            rs[1].dest = (uint64_t)back_loc;
            rs[1].size = sizeof(LeafBucket);
            rs[1].is_lock_mr = false;
            dsm->read_batch(rs, 2,true);
         //   return true;
            if((!((LeafBucket *)front_buffer)->check_consistent())&&(!((LeafBucket *)back_buffer)->check_consistent())){
               return false;
           }
            

           
           for(int i=0;i<BUCKET_SLOTS;i++){
               if(((LeafBucket *)front_buffer)->entry[i].key==key){
                  val=((LeafBucket *)front_buffer)->entry[i].val;
                  return true;

               }else if(((LeafBucket *)front_buffer)->entry[i].key==0){
                  return false;
               }
           }

            for(int i=0;i<BUCKET_SLOTS;i++){
                if(((LeafBucket *)back_buffer)->entry[i].key==key){
                    val=((LeafBucket *)back_buffer)->entry[i].val;
                    return true;

                }else if(((LeafBucket *)back_buffer)->entry[i].key==0){
                    
                    return false;
                }
            }
            break;
        }
    } 
    

    return false;
};

double LC::local_double_predict(uint64_t key){
    int loc_up=-100,loc_up_pre,loc_down;
    double loc_pre;
    for(int i=0;i<models.top.size();i++){
       if(models.top[i].anchor_key>key){
          loc_up= (int)(models.top[i].slope*(double)(key)+models.top[i].intercept);
          if(loc_up<0){
            loc_up=0;
          }
          loc_up+=models.top[i].offset; 
          break;
       } 
    }
    for(int i=loc_up-4;i<=loc_up+4;i++){
        if(i<0){
            continue;
        }
        if(models.up[i].anchor_key>key){
            loc_down=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            loc_up_pre=i;
            if(loc_down<0){
                loc_down=0;
            }
            break;
        }
    }
    for(int i=loc_down-4;i<=loc_down+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up_pre].down)[i].anchor_key>key){
            loc_pre=(models.up[loc_up_pre].down)[i].slope*(double)(key)+(models.up[loc_up_pre].down)[i].intercept;
            return loc_pre;
        }
    }



    return 0.0;
}













//以下是tree_index_operations的扩展
GlobalAddress LC::get_root_ptr(CoroContext *cxt, int coro_id){

  if (g_root_ptr == GlobalAddress::Null()){
    auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    dsm->read_sync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress), cxt);
    //这里再一次说明，root_ptr_ptr并不是指向根节点指针的指针，而是就是指向根节点的指针
    //这里root_ptr_ptr指向的是nodeID为0，offset为KChunkSize/2的地方
    GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;  //所以这里为何不用cas_buffer?但最终这里是浪费了一个leafPage的空间来取一个8B的数字
    std::cout << "Get new root" << root_ptr <<std::endl;
    g_root_ptr = root_ptr;  //因为root_ptr_ptr处记录了一个8B的根节点指针，所以root_ptr取到以后就是根节点的指针，即 0:offset的形式，且该处为根节点位置
    return root_ptr;
  }else{
    return g_root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
}


void LC::print_and_check_tree(CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  // SearchResult result;

  GlobalAddress p = root;
  GlobalAddress levels[define::kMaxLevelOfTree];
  int level_cnt = 0;
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  GlobalAddress leaf_head;

next_level:

  dsm->read_sync(page_buffer, p, kLeafPageSize);
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  levels[level_cnt++] = p;
  if (header->level != 0) {
    p = header->leftmost_ptr;
    goto next_level;
  } else {
    leaf_head = p;
  }

next:
  dsm->read_sync(page_buffer, leaf_head, kLeafPageSize);
  auto page = (LeafPage *)page_buffer;
  for (int i = 0; i < kLeafCardinality; ++i) {
    if (page->records[i].value != kValueNull) {
    }
  }
  while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
    leaf_head = page->hdr.sibling_ptr;
    goto next;
  }

}

void LC::print_verbose() {

  int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  assert(kLeafHdrOffset == kInternalHdrOffset);

  if (dsm->getMyNodeID() == 0) {   
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
  }
}


inline void LC::before_operation(CoroContext *cxt, int coro_id) {  
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack_lc[coro_id][i] = GlobalAddress::Null();
  }
}

void LC::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
  RawMessage m;
  m.type = RpcType::NEW_ROOT;
  m.addr = new_root_addr;
  m.level = root_level;
  if (root_level >= 5) {
        enable_cache = true;
  }
  //TODO: When we seperate the compute from the memory, how can we broad cast the new root
  // or can we wait until the compute node detect an inconsistent.
  for (int i = 0; i < dsm->getClusterSize(); ++i) {
    dsm->rpc_call_dir(m, i);
  }
}


bool LC::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {

  auto page_buffer = dsm->get_rbuf(coro_id).get_entry_buffer();
  auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();
    assert(left != GlobalAddress::Null());
    assert(right != GlobalAddress::Null());
  auto new_root = new (page_buffer) InternalPage(left, k, right, level);

  auto new_root_addr = dsm->alloc(kInternalPageSize);
  // The code below is just for debugging
//    new_root_addr.mark = 3;
  new_root->set_consistent();
  // set local cache for root address
  g_root_ptr = new_root_addr;
  dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize, cxt);
  if (dsm->cas_sync(root_ptr_ptr, old_root, new_root_addr, cas_buffer, cxt)) {
    broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

  return false;
}

void LC::insert_internal(const Key &k, GlobalAddress v, CoroContext *cxt,
                           int coro_id, int level){
  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
    //TODO: ADD support for root invalidate and update.
next:

  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.slibing != GlobalAddress::Null()) {
    p = result.slibing;
    goto next;
  }

  p = result.next_level;
  if (result.level != level + 1) {
    goto next;
  }

  internal_page_store(p, k, v, root, level, cxt, coro_id);
}





inline bool LC::try_lock_addr(GlobalAddress lock_addr, uint64_t tag,
                                uint64_t *buf, CoroContext *cxt, int coro_id){
  auto &pattern_cnt = pattern[dsm->getMyThreadID()][lock_addr.nodeID];

  bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  if (hand_over) {
    return true;
  }

  {

    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 3000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << dsm->getMyNodeID() << ", " << dsm->getMyThreadID()
                << " locked by " << (conflict_tag >> 32) << ", "
                << (conflict_tag << 32 >> 32) << std::endl;
      assert(false);
      exit(0);
    }

    bool res = dsm->cas_dm_sync(lock_addr, 0, tag, buf, cxt);
//      std::cout << "lock address " << lock_addr << std::endl;
    pattern_cnt++;
    if (!res) {
      conflict_tag = *buf - 1;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      lock_fail[dsm->getMyThreadID()][0]++;
      goto retry;
    }
  }

  return true;
}

inline void LC::unlock_addr(GlobalAddress lock_addr, uint64_t tag,
                              uint64_t *buf, CoroContext *cxt, int coro_id,
                              bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  auto cas_buf = dsm->get_rbuf(coro_id).get_cas_buffer();
//    std::cout << "unlock " << lock_addr << std::endl;
  *cas_buf = 0;
  if (async) {
    dsm->write_dm((char *)cas_buf, lock_addr, sizeof(uint64_t), false);
  } else {
    dsm->write_dm_sync((char *)cas_buf, lock_addr, sizeof(uint64_t), cxt);
  }

  releases_local_lock(lock_addr);
}




void LC::write_page_and_unlock(char *page_buffer, GlobalAddress page_addr,
                                 int page_size, uint64_t *cas_buffer,
                                 GlobalAddress lock_addr, uint64_t tag,
                                 CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other){
    dsm->write_sync(page_buffer, page_addr, page_size, cxt);
    releases_local_lock(lock_addr);
    return;
  }

  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)page_buffer;
  rs[0].dest = page_addr;
  rs[0].size = page_size;
  rs[0].is_lock_mr = false;

  rs[1].source = (uint64_t)dsm->get_rbuf(coro_id).get_cas_buffer();
  rs[1].dest = lock_addr;
  rs[1].size = sizeof(uint64_t);

  rs[1].is_lock_mr = true;

  *(uint64_t *)rs[1].source = 0;
  if (async) {
    dsm->write_batch(rs, 2, false);
  } else {
    dsm->write_batch_sync(rs, 2, cxt);
  }

  releases_local_lock(lock_addr);
}



void LC::lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                              int page_size, uint64_t *cas_buffer,
                              GlobalAddress lock_addr, uint64_t tag,
                              CoroContext *cxt, int coro_id){

  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);  //详细内容有点复杂，具体想做的事情应该是按照锁表，锁住远端页面
  //同时还要考虑本地获取的锁是否能够被移交

  dsm->read_sync(page_buffer, page_addr, page_size, cxt); //锁住后再进行按照page-addr的读取，放入本地rbuf的page_buffer里
  pattern[dsm->getMyThreadID()][page_addr.nodeID]++;//这个应该是给统计量用的
}





bool LC::page_search(GlobalAddress page_addr, const Key &k,
                       SearchResult &result, CoroContext *cxt, int coro_id,
                       bool from_cache, bool isroot) {
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));

  auto &pattern_cnt = pattern[dsm->getMyThreadID()][page_addr.nodeID];
  //这里是thread_ID-MN_nodeID

  int counter = 0;
re_read:
  if (++counter > 100) {
    printf("re read too many times\n");
    sleep(1);
  }
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);//将page_addr的内容读到page_buffer上
  //目前传入得page_addr就是root节点得gaddr
  pattern_cnt++;
  memset(&result, 0, sizeof(result));
  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();

  
  result.level = header->level;
  if(!result.is_leaf)
      assert(result.level !=0);
  path_stack_lc[coro_id][result.level] = page_addr;
 

  if (result.is_leaf){
    auto page = (LeafPage *)page_buffer;
    if (!page->check_consistent()){
      goto re_read;
    }

    if (from_cache &&  //from_cache为false，或者from_cache为1且key在叶节点范围内的，能够跳出去
        (k < page->hdr.lowest || k >= page->hdr.highest)){ // cache is stale
      return false;
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {  //一般情况下应该不会发生这种
      assert(false);
      return false;   
    }
    leaf_page_search(page, k, result);  
  } else {

    assert(result.level != 0);
    assert(!from_cache);
    auto page = (InternalPage *)page_buffer;
//      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

    if (!page->check_consistent()) {
      goto re_read;
    }

    if (result.level == 1 && enable_cache) {  
      index_cache->add_to_cache(page);

    }

    if (k >= page->hdr.highest) { 
        if (isroot){

            g_root_ptr = GlobalAddress::Null();
        }
      result.slibing = page->hdr.sibling_ptr;  //内部节点超界，返回true会使得在后续跳转进入sibling节点继续查
      return true;
    }
    if (k < page->hdr.lowest) {  
      printf("key %ld error in level %d\n", k, page->hdr.level);
      sleep(10);
      print_and_check_tree();
      assert(false);
      return false;
    }
    // this function will add the children pointer to the result.
    internal_page_search(page, k, result);
  }

  return true;
}

void LC::internal_page_search(InternalPage *page, const Key &k,
                                SearchResult &result) {

  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);
// if the record front verison is not equal to the rear version, what to do.
    // If we pile up the index sequentially by mulitple threads the bugs will happen
    // when muli9tple thread trying to modify the same page, because the reread for
    // inconsistent record below is not well implemented.

    //TODO (potential bug) what will happen if the last record version is not consistent?

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) { // this only happen when the lowest is 0
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
    result.next_level = page->hdr.leftmost_ptr;
//      result.upper_key = page->records[0].key;
      assert(result.next_level != GlobalAddress::Null());
//      assert(page->hdr.lowest == 0);//this actually should not happen
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
//        printf("next level key is %lu \n", page->records[i - 1].key);
      result.next_level = page->records[i - 1].ptr;
        assert(result.next_level != GlobalAddress::Null());
        assert(page->records[i - 1].key <= k);
        result.upper_key = page->records[i - 1].key;
      return;
    }
  }
//    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);

    result.next_level = page->records[cnt - 1].ptr;
    assert(result.next_level != GlobalAddress::Null());
    assert(page->records[cnt - 1].key <= k);
//    result.upper_key = page->records[cnt - 1].key;
}


void LC::leaf_page_search(LeafPage *page, const Key &k,
                            SearchResult &result) {

  for (int i = 0; i < kLeafCardinality; ++i) {
    auto &r = page->records[i];
    // if the record front verison is not equal to the rear version, what to do.
    // If we pile up the index sequentially by mulitple threads the bugs will happen
    // when muli9tple thread trying to modify the same page, because the reread for
    // inconsistent record below is not well implemented.
    if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
      result.val = r.value;
        memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
//      result.value_padding = r.value_padding;
      break;
    }
  }
}



void LC::internal_page_store(GlobalAddress page_addr, const Key &k,
                               GlobalAddress v, GlobalAddress root, int level,
                               CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  auto &rbuf = dsm->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_entry_buffer();

  auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);

    return;
  }
  assert(k >= page->hdr.lowest);

  auto cnt = page->hdr.last_index + 1;
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  bool is_update = false;
  uint16_t insert_index = 0;
  //TODO: Make it a binary search.
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;
      // assert(false);
      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }  //以上在内部节点插入的阶段，搜索时判断是否有内部节点内的key等于了已有k，如果是的话直接更新sibling_ptr，否则的话就找到该插入的位置
  assert(cnt != kInternalCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  assert(page->records[page->hdr.last_index].key != 0);

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kInternalCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  // THe internal node is different from leaf nodes because it has the
  // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
  // the internal node split pick the middle key as split key and it
  // will not existed in either of the splited node
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kInternalPageSize);
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;
      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);
      for (int i = m + 1; i < cnt; ++i) { // move
          sibling->records[i - m - 1].key = page->records[i].key;
          sibling->records[i - m - 1].ptr = page->records[i].ptr;
      }
      page->hdr.last_index -= (cnt - m); // this is correct.
      assert(page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m - 1);
      assert(sibling->hdr.last_index == cnt - m - 1 - 1);
      sibling->hdr.leftmost_ptr = page->records[m].ptr;
      sibling->hdr.lowest = page->records[m].key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = page->records[m].key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    //the code below is just for debugging.
//    sibling_addr.mark = 2;

    dsm->write_sync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
      assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

  }
//  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());


    page->set_consistent();
  write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return;

  if (root == page_addr) { // update root

    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return;
    }
  }

  auto up_level = path_stack_lc[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);  //进行级联式的节点分裂
  } else {
      insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
    assert(false);
  }
}

bool LC::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level,
                           CoroContext *cxt, int coro_id, bool from_cache){ //from_cache默认是false
                          //level在使用过程中都是0
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
      //如果有多个page_addr被映射到同一个位置上怎么办?这样的hash冲突如何解决？

  GlobalAddress lock_addr;


    char padding[VALUE_PADDING];
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);  //得到远程锁表上的lock位置
  //这里的pn-chip memory是根据页面地址做city_hash映射，然后计算锁表位置来得到的，这里难道不会遇到hash冲突的问题吗？
#endif

  auto &rbuf = dsm->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_entry_buffer();

  auto tag = dsm->getThreadTag();
  assert(tag != 0); //因为tag始终比thread_id的低字节位大1，所以tag!=对从0开始编号的thread都是成立的

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,

                     lock_addr, tag, cxt, coro_id);  //利用CAS_buffer将lock_addr远端内存上的lock原子翻转，同时将page_addr上的内容读到page_buffer上来

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());  //检查有效性

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)){ // cache is stale 
      //这里是因为hdr.highest取不到，所以采用>=highest
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);//这里说明，cache失效是根据读到的内容来决定的，读到的内容表明缓存失效了

    // if (enter_debug) {
    //   printf("cache {%lu} %lu [%lu %lu]\n", page_addr.val, k,
    //   page->hdr.lowest,
    //          page->hdr.highest);
    // }

    return false; 
  }

  // if (enter_debug) {
  //   printf("{%lu} %lu [%lu %lu]\n", page_addr.val, k, page->hdr.lowest,
  //          page->hdr.highest);
  // }

  if (k >= page->hdr.highest){  //这里应该是from_cache为false的情况

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                          coro_id);//如果是超界了，那么会尝试往兄弟节点进行page_store，这个过程是递归的，表明，非from_cache使得在越过右侧界限的情况下，会递归执行page_store的尝试

    return true;
  }
  assert(k >= page->hdr.lowest);

  int cnt = 0;
  int empty_index = -1;  
  char *update_addr = nullptr;
    // It is problematic to just check whether the value is empty, because it is possible
    // that the buffer is not initialized as 0
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {  //叶节点插入采用的是unsorted_leafNode的设计，因此节点分裂需要先对内容进行排序；至于采用unsorted_leafNode的原因应该是采用了细粒度版本机制，希望减少回写的带宽需求
      cnt++;
      if (r.key == k) {  //这里是如果已经有一个key下有value，就执行更新操作
        r.value = v;
        // ADD MORE weight for write.
        memcpy(r.value_padding, padding, VALUE_PADDING);

        r.f_version++;
        r.r_version = r.f_version;
        update_addr = (char *)&r;
        break;
      }
    } else if (empty_index == -1) {
      empty_index = i;  //这里的empty_index为，找到了一个空位置
    }
  }

  assert(cnt != kLeafCardinality);
  //普通情况下cnt是到不了叶节点最大槽位的，后续插入新kv，会再次递增cnt，然后cnt与最大槽位数相等，则触发split

  if (update_addr == nullptr){ // insert new item
    if (empty_index == -1) {
      printf("%d cnt\n", cnt);
      assert(false);
    }

    auto &r = page->records[empty_index];//根据找到的第一个empth位置插入KV
    r.key = k;
    r.value = v;
    memcpy(r.value_padding, padding, VALUE_PADDING);
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;//在这里，插入新键值对后要再次递增cnt
  }

  bool need_split = (cnt == kLeafCardinality);//如果键值对位置已满，则需要进入分裂状态，否则就不需要分裂
  //插入新键值对到达最大的槽位数量，就引发分裂

  if (!need_split) {//不需要分裂就写回，并解锁
    assert(update_addr);
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - (char *)page)),
        sizeof(LeafEntry), cas_buffer, lock_addr, tag, cxt, coro_id, false);
        //这里是细粒度的写回，因为这里只有leafEntry级别的写回
    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  } //如果需要分裂，就先对leaf内的KV进行排序，这里需要排序，也可以看出原因，因为插入操作中寻找空节点时，并不会保证数据排序


  Key split_key;
  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kLeafPageSize);  //如果需要分裂，则当前节点的兄弟节点需要重新申请一个Page，并且会返回这个PageSize的全局地址
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);

      for (int i = m; i < cnt; ++i) { // move
          sibling->records[i - m].key = page->records[i].key;
          sibling->records[i - m].value = page->records[i].value;
          page->records[i].key = 0;
          page->records[i].value = kValueNull;
      }
      //We don't care about the last index in the leaf nodes actually,
      // because we iterate all the slots to find an entry.
      page->hdr.last_index -= (cnt - m);
//      assert(page_addr == root || page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m);
//      assert(sibling->hdr.last_index == cnt -m -1);
      sibling->hdr.lowest = split_key;// the lowest for leaf node is the lowest that this node contain
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = split_key;  //分裂过后更新hdr里的元数据信息

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;  //这是因为，现在加入的sibling_node位于旧node和旧node的sibling_ptr之间
      page->hdr.sibling_ptr = sibling_addr;  //所以这里是让sibling的sibling等于旧node的sibling，让旧node的sibling等于这个分裂得到的sibling节点
    sibling->set_consistent();
    dsm->write_sync(sibling_buf, sibling_addr, kLeafPageSize, cxt);  //将sibling节点写入远端
  }

  page->set_consistent();
    // why need split make the write and locking async?
  write_page_and_unlock(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);  //将分裂节点写入远端后，再将自己也写回远端

  if (!need_split)//由于上述write_page_and_unlock无论分裂与否都要发生，这里是分裂节点情况的衍生讨论
    return true;  //如果不需要分裂，那么返回true，否则继续往下执行
  // note: there will be a bug for the concurrent root update. because the root is not guaranteed to be the same
  // when split pop up to the root node. Causing two nodes.
  if (root == page_addr) { // update root
  //这里表示，如果是分裂的节点，正好是root节点的话，则需要进行root节点的更新与传播，但是这里的bug就是，root节点无法并发更新
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return true;
    }
  }
//以上是利用传入的参数root判断是否是根节点在分裂
//那么这个逻辑就是，分裂完毕后，在内部形成这个插入的新节点，所以才会有internel_page_store，进行逐级完善的SMO的操作
  auto up_level = path_stack_lc[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {

    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
    assert(from_cache);
    //If the program comes here, then it could be dangerous
    insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
  }

  return true;
}



void LC::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id){
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  auto tag = dsm->getThreadTag();
  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

  auto page_buffer = dsm->get_rbuf(coro_id).get_entry_buffer();
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
  }

  auto cnt = page->hdr.last_index + 1;

  int del_index = -1;
  for (int i = 0; i < cnt; ++i) {
    if (page->records[i].key == k) { // find and update
      del_index = i;
      break;
    }
  }

  if (del_index != -1) { // remove and shift
    for (int i = del_index + 1; i < cnt; ++i) {
      page->records[i - 1].key = page->records[i].key;
      page->records[i - 1].value = page->records[i].value;
    }

    page->hdr.last_index--;

    page->set_consistent();
    dsm->write_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  }
  this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, false);
}


inline bool LC::acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  bool is_local_locked = false;

  uint64_t lock_val = node.ticket_lock.fetch_add(1);

  uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
  uint32_t current = lock_val >> 32;// current is the former 32 bit in ticket lock

  // printf("%ud %ud\n", ticket, current);
  while (ticket != current) { // lock failed
    is_local_locked = true;

    if (cxt != nullptr) {
      hot_wait_queue_lc.push(coro_id);
      (*cxt->yield)(*cxt->master);
    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  if (is_local_locked) {
    hierarchy_lock[dsm->getMyThreadID()][0]++;
  }

  node.hand_time++;

  return node.hand_over;
}


inline bool LC::can_hand_over(GlobalAddress lock_addr) {

  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
// only when unlocking, it need to check whether it can handover to the next, so that it do not need to UNLOCK the global lock.
// It is possible that the handover is set as false but this server is still holding the lock.
  uint32_t ticket = lock_val << 32 >> 32;//
  uint32_t current = lock_val >> 32;
// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime; // check the limit
  }
  if (!node.hand_over) {
    node.hand_time = 0;// clear the handtime.
  } else {
    handover_count[dsm->getMyThreadID()][0]++;
  }

  return node.hand_over;
}
inline void LC::releases_local_lock(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}



void LC::tree_insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id){  
  //这里CoroCintext在Tree.h中被默认初始化为nullptr，且coro_id被默认初始化为0
  assert(dsm->is_register());

  before_operation(cxt, coro_id);  

  if (enable_cache) {
    GlobalAddress cache_addr;
    auto entry = index_cache->search_from_cache(k, &cache_addr);
    if (entry) { 
      auto root = get_root_ptr(cxt, coro_id);  
      if (leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true)) {    
        cache_hit[dsm->getMyThreadID()][0]++;
        return;
      }
      index_cache->invalidate(entry);  
    }
    cache_miss[dsm->getMyThreadID()][0]++; 
  }

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;
  GlobalAddress p = root;
  bool isroot = true;

next:

  if (!page_search(p, k, result, cxt, coro_id, false, isroot)){

    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }
  isroot = false;

  if (!result.is_leaf) {  
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {//如果存在sibling节点
      p = result.slibing;  //这种情况下要去往sibling节点，并开始搜索叶节点，那么，这又是什么情况呢？
      goto next;
    }

    p = result.next_level;  
    if (result.level != 1) {  
      goto next;
    }
  }

  leaf_page_store(p, k, v, root, 0, cxt, coro_id);
}



bool LC::tree_search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
  bool isroot = true;
  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (enable_cache) {
    GlobalAddress cache_addr;
    entry = index_cache->search_from_cache(k, &cache_addr); //如果成功搜索到了，则返回entry是一个有效指针，否则是nullptr，同时有效的情况下cache_addr是有内容的
    if (entry) { // cache hit
      cache_hit[dsm->getMyThreadID()][0]++;
      from_cache = true;
      p = cache_addr;
      isroot = false;
    } else {
      cache_miss[dsm->getMyThreadID()][0]++;
    }
  }

next:
  if (!page_search(p, k, result, cxt, coro_id, from_cache, isroot)) {
    if (from_cache) { // cache stale
      index_cache->invalidate(entry);
      // Comment it during the test.
//        invalid_counter[dsm->getMyThreadID()][0]++;
//        if(invalid_counter[dsm->getMyThreadID()][0] % 5000 == 0){
//            printf("Invalidate cache 0\n");
//        }
        //The cache hit is the real cache hit counting the invalidation in
      cache_hit[dsm->getMyThreadID()][0]--;
      cache_miss[dsm->getMyThreadID()][0]++;
      from_cache = false;

      p = root;
      isroot = true;
    } else {
      std::cout << "SEARCH WARNING search" << std::endl;
      sleep(1);
    }
    goto next;
  }
  else{
      isroot = false;
  }

  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;

      return true;
    }
    if (result.slibing != GlobalAddress::Null()){ //turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found,当为根节点且仅有根节点的时候，且范围超界，会走到这里
  } else {        // internal
    p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                : result.next_level; 
    //page_search里，search结果指示往sibling节点和next_level去都会返回true
    goto next;
  }
}


bool LC::test_search(uint64_t &key, uint64_t &val, CoroContext *cxt, int coro_id){
    //先去查找index_cache，查到就进行batch读，没查到就两次分别搜索

    assert(dsm->is_register());
    auto root = get_root_ptr(cxt, coro_id);
    SearchResult result;
    GlobalAddress p=GlobalAddress::Null();
    const CacheEntry *entry = nullptr;
    if(enable_cache){
       GlobalAddress cache_addr;
       
       entry = index_cache->search_from_cache(key, &cache_addr);
       if(entry){
          cache_hit[dsm->getMyThreadID()][0]++;
          p = cache_addr;
       }else{
          cache_miss[dsm->getMyThreadID()][0]++;
       }
    }
   
    if(p==GlobalAddress::Null()){  
      
      bool search1=this->model_search(key, val);
      bool search2=this->tree_search(key,val);
      return search1&&search2;
    }
    int loc_up=-100,loc_up_pre,loc_down,loc_pre;
    for(int i=0;i<models.top.size();i++){
       if(models.top[i].anchor_key>key){
          loc_up= (int)(models.top[i].slope*(double)(key)+models.top[i].intercept);
          if(loc_up<0){
            loc_up=0;
          }
          loc_up+=models.top[i].offset; 
          break;
       } 
    }

    for(int i=loc_up-4;i<=loc_up+4;i++){
        if(i<0){
            continue;
        }
        if(models.up[i].anchor_key>key){
            loc_down=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            loc_up_pre=i;
            if(loc_down<0){
                loc_down=0;
            }
            break;
        }
    }
    for(int i=loc_down-4;i<=loc_down+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up_pre].down)[i].anchor_key>key){
            loc_pre=(int)((models.up[loc_up_pre].down)[i].slope*(double)(key)+(models.up[loc_up_pre].down)[i].intercept);
            //待写，read_batch的过程，批量读过来然后检查
             
            char *front_buffer=(dsm->get_rbuf(0)).get_page_buffer();
            char *back_buffer=(dsm->get_rbuf(0)).get_entry_buffer();
            GlobalAddress front_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc_pre);
            GlobalAddress back_loc=p;

            RdmaOpRegion rs[2];
            rs[0].source = (uint64_t)front_buffer;
            rs[0].dest = (uint64_t)front_loc;
            rs[0].size = sizeof(LeafBucket);
            rs[0].is_lock_mr = false;
            rs[1].source = (uint64_t)back_buffer;
            rs[1].dest = (uint64_t)back_loc;
            rs[1].size = sizeof(kLeafPageSize);
            rs[1].is_lock_mr = false;
            dsm->read_batch(rs, 2,true);

            if((!((LeafBucket *)front_buffer)->check_consistent())&&(!((LeafPage *)back_buffer)->check_consistent())){
               return false;
           }
            

           
           for(int i=0;i<BUCKET_SLOTS;i++){
               if(((LeafBucket *)front_buffer)->entry[i].key==key){
                  val=((LeafBucket *)front_buffer)->entry[i].val;
           }
           }
           for(int i=0;i<kLeafCardinality;++i){
               if(((LeafPage *)back_buffer)->records[i].key==key){
                 val=((LeafPage *)back_buffer)->records[i].value;
               }
           }

           return true;

        }
    } 
    return false;
}

bool LC::test_hash_search(uint64_t &key, uint64_t &val, CoroContext *cxt, int coro_id){  //简易的Hash逻辑测试
     murmur2((char *)&key, sizeof(key));
    assert(dsm->is_register());
    auto root = get_root_ptr(cxt, coro_id);
    SearchResult result;
    GlobalAddress p=root;

    int loc_up=-100,loc_up_pre,loc_down,loc_pre;
    for(int i=0;i<models.top.size();i++){
       if(models.top[i].anchor_key>key){
          loc_up= (int)(models.top[i].slope*(double)(key)+models.top[i].intercept);
          if(loc_up<0){
            loc_up=0;
          }
          loc_up+=models.top[i].offset; 
          break;
       } 
    }

    for(int i=loc_up-4;i<=loc_up+4;i++){
        if(i<0){
            continue;
        }
        if(models.up[i].anchor_key>key){
            loc_down=(int)(models.up[i].slope*(double)(key)+models.up[i].intercept);
            loc_up_pre=i;
            if(loc_down<0){
                loc_down=0;
            }
            break;
        }
    }
    for(int i=loc_down-4;i<=loc_down+4;i++){
        if(i<0){
            continue;
        }
        if((models.up[loc_up_pre].down)[i].anchor_key>key){
            loc_pre=(int)((models.up[loc_up_pre].down)[i].slope*(double)(key)+(models.up[loc_up_pre].down)[i].intercept);
            //待写，read_batch的过程，批量读过来然后检查
             
            char *front_buffer=(dsm->get_rbuf(0)).get_page_buffer();
            char *back_buffer=(dsm->get_rbuf(0)).get_entry_buffer();
            GlobalAddress front_loc=GADD((models.up[loc_up_pre].down)[i].leaf_ptr,STRUCT_OFFSET(LeafNode,front_buckets)+sizeof(LeafBucket)*loc_pre);
            GlobalAddress back_loc=p;

            RdmaOpRegion rs[2];
            rs[0].source = (uint64_t)front_buffer;
            rs[0].dest = (uint64_t)front_loc;
            rs[0].size = sizeof(LeafBucket);
            rs[0].is_lock_mr = false;
            rs[1].source = (uint64_t)back_buffer;
            rs[1].dest = (uint64_t)back_loc;
            rs[1].size = sizeof(kLeafPageSize);
            rs[1].is_lock_mr = false;
            dsm->read_batch(rs, 2,true);

            if((!((LeafBucket *)front_buffer)->check_consistent())&&(!((LeafPage *)back_buffer)->check_consistent())){
               return false;
           }
            

           
           for(int i=0;i<BUCKET_SLOTS;i++){
               if(((LeafBucket *)front_buffer)->entry[i].key==key){
                  val=((LeafBucket *)front_buffer)->entry[i].val;
           }
           }
           for(int i=0;i<BUCKET_SLOTS;i++){  //模拟一下情况
               if(((LeafBucket *)front_buffer)->entry[i].key==key){
                  val=((LeafBucket *)front_buffer)->entry[i].val;
                }
           }
           return true;

        }
    } 
    return false;     


     


   
     return true;



}    
void LC::print_page_num(){
     // std::cout<<"实际使用的缓存中间节点数量: "<<this->index_cache->count_page()<<std::endl;
      std::cout<<"Internal Node大小:"<<sizeof(InternalPage)<<"  CacheEntry的大小:"<<sizeof(CacheEntry)<<std::endl;
      this->index_cache->statistics();
      std::cout<<"down层模型数量: "<<this->models.down.size()<<std::endl;
      std::cout<<"up层模型数量: "<<this->models.up.size()<<std::endl;
      std::cout<<"top层模型数量: "<<this->models.top.size()<<std::endl;

  }



































