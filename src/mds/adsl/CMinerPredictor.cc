// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CMinerPredictor.h"
#include "OnlineMiner.h"

#include "mds/CInode.h"
#include "mds/MDBalancer.h"
#include "mds/MDSRank.h"

namespace adsl {

CMinerPredictor::CMinerPredictor()
{}

CMinerPredictor::~CMinerPredictor()
{
}

CInode* ino_to_CIno(inodeno_t ino, MDBalancer *m){  // for using r_pos_map
  MDSRank *mds = m->get_mds();
  return mds->mdcache->get_inode(ino);
} 

int do_predict(boost::string_view script,
	       PredInputLoad &input_load,
	       LoadArray_Double &pred_load)
{
  pred_load.clear();
  MDBalancer *m = input_load.dirfrag->bal;
  map<int, CInode*> &pos_map = input_load.dirfrag->pos_map;   // index与实际inode对应
  map<CInode*, int> &r_pos_map = input_load.dirfrag->r_pos_map;
  OnlineMiner *miner = m->miner;

  int serial_num = -1;
  for (auto &load : input_load.cur_loads) {

    serial_num += 1;

    inodeno_t load_ino = pos_map[serial_num]->ino();
    auto correlation = miner->get_correlated(load_ino);
    
    auto &last_epoch_load = *(load.end() - 1);

    for(auto &corr_list: correlation){

      inodeno_t corr_ino = corr_list.first;
      CInode* corr_CIno = ino_to_CIno(corr_ino, m);
      auto corr_serial_num = r_pos_map.find(corr_CIno);
      if(corr_serial_num == r_pos_map.end()){
        continue;
      }
      pred_load[corr_serial_num->second] += corr_list.second.second * last_epoch_load;
    
    }
  }

  return 0;
}

}; // namespace adsl
