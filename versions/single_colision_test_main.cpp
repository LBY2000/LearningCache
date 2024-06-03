#include <iostream>
#include <random>
#include <map>
#include <unordered_map>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>
#include <iostream>
#define LOAD_KEYS 30000000
#define BACK_KEYS 2000000
std::map<int,int> total_times;
using namespace std;
struct Slope {
  using SX=__int128;
  using SY=__int128;
  SX dx{};
  SY dy{};  //会调用对应类型的默认构造函数，这里会将其置为0

  bool operator<(const Slope &p) const { return dy * p.dx < dx * p.dy; }
  bool operator>(const Slope &p) const { return dy * p.dx > dx * p.dy; }
  bool operator==(const Slope &p) const { return dy * p.dx == dx * p.dy; }
  bool operator!=(const Slope &p) const { return dy * p.dx != dx * p.dy; }
  explicit operator long double() const { return dy / (long double) dx; }//强制类型转换过程中自动调用
};
struct Point {
  using X=uint64_t;
  using Y=uint64_t;
  using SX=__int128;
  using SY=__int128;
  X x{};
  Y y{};

  Slope operator-(const Point &p) const { 
      Slope S;
      S.dx=SX(x)-p.x;
      S.dy=SY(y)-p.y;
      return S;}
      // return (Slope){SX(x) - p.x, SY(y) - p.y}; }  //将自己的x/y成员转为SX类型再进行运算，返回Slope类型变量
};

class CanonicalSegment {
  friend class PLR;
  using X=uint64_t;
  using Y=uint64_t;
  Point rectangle[4];
  X first;

  CanonicalSegment(const Point &p0, const Point &p1, X first) : rectangle{p0, p1, p0, p1}, first(first) {};

  CanonicalSegment(const Point (&rectangle)[4], X first)
    : rectangle{rectangle[0], rectangle[1], rectangle[2], rectangle[3]}, first(first) {};

  bool one_point() const {
    return rectangle[0].x == rectangle[2].x && rectangle[0].y == rectangle[2].y
        && rectangle[1].x == rectangle[3].x && rectangle[1].y == rectangle[3].y;
  }

public:
  CanonicalSegment() = default;

  X get_first_x() const { return first; }

  std::pair<long double, long double> get_intersection() const {
    auto &p0 = rectangle[0];
    auto &p1 = rectangle[1];
    auto &p2 = rectangle[2];
    auto &p3 = rectangle[3];
    auto slope1 = p2 - p0;
    auto slope2 = p3 - p1;

    if (one_point() || slope1 == slope2)
      return {p0.x, p0.y};

    auto p0p1 = p1 - p0;
    auto a = slope1.dx * slope2.dy - slope1.dy * slope2.dx;
    auto b = (p0p1.dx * slope2.dy - p0p1.dy * slope2.dx) / static_cast<long double>(a);
    auto i_x = p0.x + b * slope1.dx;
    auto i_y = p0.y + b * slope1.dy;
    return {i_x, i_y};
  }
  std::pair<long double, __int128> get_floating_point_segment(const uint64_t &origin) const {
    if (one_point())
      return {0, (rectangle[0].y + rectangle[1].y) / 2};

   // if (std::is_integral_v<uint64_t> && std::is_integral_v<uint64_t>) {

      auto slope = rectangle[3] - rectangle[1];
      auto intercept_n = slope.dy * (__int128(origin) - rectangle[1].x);
      auto intercept_d = slope.dx;
      auto rounding_term = ((intercept_n < 0) ^ (intercept_d < 0) ? -1 : +1) * intercept_d / 2;
      auto intercept = (intercept_n + rounding_term) / intercept_d + rectangle[1].y;
      return {static_cast<long double>(slope), intercept};
  }
  std::pair<long double, long double> get_slope_intercept() const {
    if (one_point())
      return {0, (rectangle[0].y + rectangle[1].y) / 2};

   // auto[i_x, i_y] = get_intersection();
    std::pair<long double, long double> p = get_intersection();
   // auto[min_slope, max_slope] = get_slope_range();
    std::pair<long double, long double> p2 = get_slope_range();
    auto slope = (p2.first + p2.second) / 2.;
    auto intercept = p.second - p.first * slope;
    return {slope, intercept};
  }
  std::pair<long double, long double> get_slope_range() const {
    if (one_point())
      return {0, 1};

    auto min_slope = static_cast<long double>(rectangle[2] - rectangle[0]);
    auto max_slope = static_cast<long double>(rectangle[3] - rectangle[1]);
    return {min_slope, max_slope};
  }




};


class PLR{
private:
  using SX = __int128;  
  using SY = __int128;
  using X = uint64_t;
  using Y = uint64_t;




  const Y epsilon;
  std::vector<Point> lower;
  std::vector<Point> upper;
  X first_x = 0;
  X last_x = 0;
  size_t lower_start = 0;
  size_t upper_start = 0;
  
  Point rectangle[4];

  auto cross(const Point &O, const Point &A, const Point &B) const {
    Slope OA = A - O;
    Slope OB = B - O;
    return OA.dx * OB.dy - OA.dy * OB.dx;
  }

public:
  size_t points_in_hull = 0;

  explicit PLR(Y epsilon) : epsilon(epsilon), lower(), upper() {
    if (epsilon < 0)
      throw std::invalid_argument("epsilon cannot be negative");
    upper.reserve(1u << 16);
    lower.reserve(1u << 16);
    //std::cout<<"this is PLR Model"<<std::endl;
  }

  bool add_point(const X &x, const Y &y) {
    if (points_in_hull > 0 && x <= last_x)
      throw std::logic_error("Points must be increasing by x.");

    last_x = x;
    auto max_y = std::numeric_limits<Y>::max();
    auto min_y = std::numeric_limits<Y>::lowest();
    Point p1;  //问题就在这里
    p1.x=x;
    if(y >= max_y - epsilon){
        p1.y=max_y;
    }else{
        p1.y=y+epsilon;
    }
    Point p2; 
    p2.x=x; 
    if(y <= min_y + epsilon){
        p2.y=min_y;
    }else{
        p2.y=y-epsilon;
    }     //问题就在这里
    
    if (points_in_hull == 0) {
      first_x = x;
      rectangle[0] = p1;
      rectangle[1] = p2;
      upper.clear();//清空容器内容，但是不会清空其容量
      lower.clear();
      upper.push_back(p1);
      lower.push_back(p2);
      upper_start = lower_start = 0;
      ++points_in_hull;
      return true;
    }

    if (points_in_hull == 1) {
      rectangle[2] = p2;
      rectangle[3] = p1;
      upper.push_back(p1);
      lower.push_back(p2);
      ++points_in_hull;
      return true;
    }
    auto slope1 = rectangle[2] - rectangle[0];
    auto slope2 = rectangle[3] - rectangle[1];
    bool outside_line1 = p1 - rectangle[2] < slope1;
    bool outside_line2 = p2 - rectangle[3] > slope2;

    if (outside_line1 || outside_line2) {
      points_in_hull = 0;
      return false;
    }

    if (p1 - rectangle[1] < slope2) {
      // Find extreme slope
      auto min = lower[lower_start] - p1;
      auto min_i = lower_start;
      for (auto i = lower_start + 1; i < lower.size(); i++) {
        auto val = lower[i] - p1;
        if (val > min)
          break;
        min = val;
        min_i = i;
      }

      rectangle[1] = lower[min_i];
      rectangle[3] = p1;
      lower_start = min_i;

      // Hull update
      auto end = upper.size();
      for (; end >= upper_start + 2 && cross(upper[end - 2], upper[end - 1], p1) <= 0; --end)
          continue;
      upper.resize(end);
      upper.push_back(p1);
    }

    if (p2 - rectangle[0] > slope1) {
      // Find extreme slope
      auto max = upper[upper_start] - p2;
      auto max_i = upper_start;
      for (auto i = upper_start + 1; i < upper.size(); i++) {
          auto val = upper[i] - p2;
          if (val < max)
              break;
          max = val;
          max_i = i;
      }

      rectangle[0] = upper[max_i];
      rectangle[2] = p2;
      upper_start = max_i;

      // Hull update
      auto end = lower.size();
      for (; end >= lower_start + 2 && cross(lower[end - 2], lower[end - 1], p2) >= 0; --end)
          continue;
      lower.resize(end);
      lower.push_back(p2);
    }

    ++points_in_hull;
    return true;
  }

  CanonicalSegment get_segment(){
    if (points_in_hull == 1)
      return CanonicalSegment(rectangle[0], rectangle[1], first_x);
    return CanonicalSegment(rectangle, first_x);
  }

  void reset() {
    points_in_hull = 0;
    lower.clear();
    upper.clear();
  }

};
std::vector<uint64_t> exist_keys;
std::vector<uint64_t> nonexist_keys;

void load_data(){  //在这里切换不同负载，现在只考虑一种生成负载

    std::cout << "==== LOAD normal ====="<<std::endl;
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
};

class SubModel{

    public:
      uint64_t anchor_key; //单个子model内最大的key值
      double slope,intercept;
      uint64_t leaf_size;  //一个bucket有8个slots，16*8* 8*32 = 32KB，256个kv为一组
    
};
void append_model(double slope, double intercept,
                     const typename std::vector<uint64_t>::const_iterator &keys_begin,
                     const typename std::vector<uint64_t>::const_iterator &vals_begin, 
                     size_t size){

       SubModel sub;
       sub.slope=slope;
       sub.intercept=intercept;
       sub.leaf_size=1024;
       auto k=*(keys_begin+size-1);
       sub.anchor_key=k;


       //char *tb=(dsm->get_rbuf(0)).get_sibling_buffer();

       std::unordered_map<int,int> m;
       std::map<int,int> m_times;
       for(int i=0;i<size;i++){
          auto temp_k=*(keys_begin+i);
          int pre_loc=(int)(slope*(double)(temp_k)+intercept);
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
       
       
      // models.down.push_back(sub);
       

  //待写
   return;
}


int main(int argc,char **argv){
    uint64_t epsilon=16;
    PLR *plr=new PLR(epsilon-1);
    load_data();
    std::sort(exist_keys.begin(), exist_keys.end());
           exist_keys.erase(std::unique(exist_keys.begin(), exist_keys.end()), exist_keys.end());
           std::sort(exist_keys.begin(), exist_keys.end());
           std::cout<<"used "<<exist_keys.size()<<" keys."<<std::endl;
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
              //  std::cout<<"i="<<i<<std::endl;       
                auto cs = plr->get_segment();
                std::pair<long double, long double>  cs_param= cs.get_slope_intercept(); 
                
                append_model(cs_param.first, cs_param.second, k_iter, v_iter, pos);    //待写
                
                k_iter += pos;
                v_iter += pos;
                pos=0;
                delete plr;
                PLR *plr = new PLR(epsilon-1);
                
                plr->add_point(p, pos);
               // std::cout<<"i="<<i<<std::endl;
               // std::cout<<"used "<<exist_keys.size()<<" keys."<<std::endl;
            }
        }
    std::map<int,int>::iterator it;
        for(it=total_times.begin();it!=total_times.end();it++){
            std::cout<<it->first<<" collisons accurs: "<<it->second<<std::endl;
        }
    return 0;
}