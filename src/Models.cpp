#include "Models.h"
#include "PLR.h"
void Models::bulkload_train(std::vector<uint64_t> &keys,std::vector<uint64_t> &vals,DSM *dsm){   //主要为bulk_loading阶段服务的train
           PLR *plr=new PLR(down_epsilon-1);
           std::cout<<"actually used KV nums:"<<keys.size()<<std::endl;
           uint64_t p=keys[0];
           size_t pos=0;
           plr->add_point(p,pos);
           auto k_iter = keys.begin();
           auto v_iter = vals.begin();
           int max_num_per_model=1;
           for(int i=1; i<keys.size(); i++){
            uint64_t next_p = keys[i];
            if (next_p == p){
                exit(0);
            }
            p = next_p;
            pos++;
            
            if((!plr->add_point(p, pos)||i==keys.size()-1)||max_num_per_model>128){   //如果add_point失败，则执行下面的操作   
                if(i!=keys.size()-1){         
                    auto cs = plr->get_segment();
                    std::pair<long double, long double>  cs_param= cs.get_slope_intercept(); 
                    
                    append_model(cs_param.first, cs_param.second, k_iter, v_iter, pos,dsm);    //待写append_model的变化代码
                    
                    k_iter += pos;
                    v_iter += pos;
                    pos=0;
                    plr = new PLR(down_epsilon-1);
                    plr->add_point(p, pos);
                    max_num_per_model=1;
                }else{
                    auto cs = plr->get_segment();
                    std::pair<long double, long double>  cs_param= cs.get_slope_intercept(); 
                    append_model(cs_param.first, cs_param.second, k_iter, v_iter, pos+1,dsm);
                    continue;
                }
            }else{
                max_num_per_model++;
            }
        }   
      //      std::cout<<"up level collisions:"<<std::endl;
            this->bulkload_train_up();
         //   std::cout<<"top level collisions:"<<std::endl;
            this->bulkload_train_top();
            std::cout<<"used down models:"<<this->down.size()<<std::endl;
            std::cout<<"used up models:"<<this->up.size()<<std::endl;
            std::cout<<"used top models:"<<this->top.size()<<std::endl;
            std::cout<<"last key:"<<keys[keys.size()-1]<<std::endl;
            std::cout<<"last down's anchor key:"<<this->down[down.size()-1].anchor_key<<std::endl;
            std::cout<<"last up's anchor key:"<<this->up[up.size()-1].anchor_key<<std::endl;
            std::cout<<"last top's anchor key:"<<this->top[top.size()-1].anchor_key<<std::endl;


    }

void Models::bulkload_train_up(){ //down层的情况来学习上层的up_model
    //    uint64_t epsilon=4;
        PLR *plr=new PLR(upper_epsilon-1);
        Upper_Model tup;
        uint64_t p=down[0].anchor_key;
        tup.down.push_back(down[0]);
        size_t pos=0;
        plr->add_point(p,pos);
        auto k_iter = down.begin();
        for(int i=1; i<down.size();i++){
            uint64_t next_p = down[i].anchor_key;
            if (next_p == p){
                std::cout<<"can't do this since the dulpicated keys."<<std::endl;
                exit(0);
            }
            p = next_p;
            pos++;
            
            if(!plr->add_point(p, pos)||i==down.size()-1){   //如果add_point失败，则执行下面的操作            
                auto cs = plr->get_segment();
                std::pair<long double, long double>  cs_param= cs.get_slope_intercept();
                
                SubModel submodel;
                if(i==down.size()-1){
                   submodel=*(k_iter+pos);}
                else{
                   submodel=*(k_iter+pos-1);
                }
                tup.anchor_key=submodel.anchor_key;
                if(i==down.size()-1){
                    std::cout<<"the last anchor in upper train is:"<<submodel.anchor_key<<std::endl;
                }
                tup.slope=cs_param.first;
                tup.intercept=cs_param.second;
                if(i==down.size()-1){
                    tup.down.push_back(down[i]);
                }

                
                up.push_back(tup);
                
                if(i!=down.size()-1){
                    k_iter += pos;
                    pos=0;
                    plr = new PLR(upper_epsilon-1);
                    plr->add_point(p, pos);
                    tup.down.clear();
                    tup.down.push_back(down[i]);
                }else{
                    continue;
                }

            }else{
                tup.down.push_back(down[i]);
            }
        }               
    }
void Models::bulkload_train_top(){ //down层的情况来学习上层的up_model
        int offset=0;
        Top_Model tup;
        tup.offset=0;
        PLR *plr=new PLR(upper_epsilon-1);
        uint64_t p=up[0].anchor_key; //0溢出问题，加上这个标定一下范围
        size_t pos=0;
        plr->add_point(p,pos);
        auto k_iter = up.begin();
        for(int i=1; i<up.size(); i++) {
            uint64_t next_p = up[i].anchor_key;
            if (next_p == p){
                std::cout<<"can't do this since the dulpicated keys."<<std::endl;
                exit(0);
            }
            p = next_p;
            pos++;
            
            if(!plr->add_point(p, pos)||i==up.size()-1){   //如果add_point失败，则执行下面的操作            
                auto cs = plr->get_segment();
                std::pair<long double, long double>  cs_param= cs.get_slope_intercept();
                
                Upper_Model submodel;
                if(i==up.size()-1){
                   submodel=*(k_iter+pos);
                   }
                else{
                   submodel=*(k_iter+pos-1);
                }
                tup.anchor_key=submodel.anchor_key;
                tup.slope=cs_param.first;
                tup.intercept=cs_param.second;
                top.push_back(tup);

                if(i!=up.size()-1){
                    k_iter += pos;
                    pos=0;
                    plr = new PLR(upper_epsilon-1);
                    plr->add_point(p, pos);
                    offset++;
                    tup.offset=offset;}
                else{
                    
                    continue;
                }
            }else{
                offset++;
               // pos++;
            }
        }
                




    }
void Models::append_model(double slope, double intercept,
                     const typename std::vector<uint64_t>::const_iterator &keys_begin,
                     const typename std::vector<uint64_t>::const_iterator &vals_begin, 
                     size_t size,DSM *dsm){
       SubModel sub;
       sub.slope=slope;
       sub.intercept=intercept;
       auto k=*(keys_begin+size-1);
       sub.anchor_key=k;
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
       this->down.push_back(sub);

   return;
}