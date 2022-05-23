#include "CMiner.h"
#include "mds/MDBalancer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout
#undef dout
#define dout(lvl) \
	  do {\
		      auto subsys = ceph_subsys_mds;\
		      if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_balancer, lvl)) {\
				        subsys = ceph_subsys_mds_balancer;\
				      }\
		      dout_impl(dout_context, subsys, lvl) dout_prefix \
              << "epoch: " << balancer->get_beat_epoch() << " "
#undef dendl
#define dendl dendl_impl; } while (0)

CMiner::CMiner(MDBalancer * balancer, int am_supp, float am_conf, int m_epoch, int win_size, int m_gap, int m_supp, float m_conf):
	OnlineMiner(balancer),
	overall_min_support(am_supp),
	overall_min_confidence(am_conf),
	max_epoch(m_epoch),
	window_size(win_size),
	max_gap(m_gap),
	min_support(m_supp),
	min_confidence(m_conf),
	mine_lock("mine_lock")
{
	create("mine_thread"); // create the thread
	last_epoch = balancer->get_beat_epoch(); // get current epoch
}

CMiner::~CMiner() {
	this->kill(9); // kill the thread
}

void CMiner::hit(inodeno_t ino){
    input_lock.lock();

    input.push_back(ino);

    input_lock.unlock();

	// signal_miner();
}

map<inodeno_t, pair<int, float> >& CMiner::get_correlated(inodeno_t ino){
    return correlation_table[ino];
}

map<inodeno_t, map<inodeno_t, pair<int, float> > >& CMiner::get_full_correlated_table(){
    return correlation_table;
}

void CMiner::process(){

    map<inodeno_t, map<inodeno_t, pair<int, float> > > rules;

    //mine()
    input_lock.lock();
    // move the data into temp
    vector<inodeno_t> temp_input = move(input);
    input_lock.unlock();

	// print debug info
	// std::stringstream debug_out;
	// for (auto &inode : temp_input) {
	//	debug_out << inode << " ";
	//}
	// dout(0) << "PROCESS VECTOR: " << debug_out.str() << dendl;

    /********** Step 1 **********/
    // convert 1D data into 2D
    int segment_count = 0;
    vector<inodeno_t> segment;
    vector<vector<inodeno_t> > segments;

    for (auto &item: temp_input) {
        segment.push_back(item);
        segment_count += 1;
        if (segment_count == window_size) {
            segments.push_back(move(segment));
            segment_count = 0;
        }
    }

    //  Last segment
    if (segment_count != 0) {
        segments.push_back(move(segment));
    }

    /********** Step 2 **********/
    // Generate Deltas (substructure of segment)
    // 所有的片段按照第一个元素进行归类，并用map保存

    // 通过元素inodeno_t，找到inodeno_t开头的所有片段
    map<inodeno_t, vector<vector<inodeno_t>>> deltas;

    for (auto &item_segment: segments) {

        int seg_len = item_segment.size();

        for (int j = 0; j < item_segment.size(); j++) {

            auto item = item_segment[j];
            int expected = j + max_gap;

            // check the boundary
            if (expected >= item_segment.size()) {
                expected = item_segment.size();
            }

            deltas[item].push_back(vector<inodeno_t>(item_segment.begin() + j, item_segment.begin() + expected));
        }
    }

    /********** Step 3 **********/
    // Check frequency and generate rules
    // 统计有多少个片段是以inodeno_t开头的
    map<inodeno_t, int> item_freqs;

    for (auto &iter: deltas) {//auto iter = deltas.begin(); iter!=deltas.end(); iter++){

        auto &header = iter.first; // the item
        auto &delta_lists = iter.second; // the list related to the item

        // --- 1. Check the header itself
        int item_freq = delta_lists.size();

        if (item_freq < min_support) { // 出现次数小于最低要求的就跳过
            continue;
        }
        item_freqs[header] = item_freq; // record the frequency

        map<inodeno_t, int> corr_freqs; // 统计某个元素在header元素后一共出现了多少次
        map<inodeno_t, int> corr_appear; // 同上，但一个片段中如果出现多次，只会计算一次
        // --- 2. Check each subsequent items
        // 遍历所有的片段
        for (auto &delta_list: delta_lists) { // delta_list: 一个片段（一维数组）

            map<inodeno_t, bool> corr_list_appear;   // For counting which items appear in THIS list

            // 统计一个片段中，某个元素出现了多少次，记录到corr_list_appear中
            for (auto &item: delta_list) {

                if (item == header) {
                    continue;
                }

                corr_list_appear[item] = 1;
                if (corr_freqs.find(item) == corr_freqs.end()) {
                    corr_freqs[item] = 1;
                } else {
                    corr_freqs[item] += 1;
                }
            }

            // 遍历所有出现过的元素
            for (auto &item_pair: corr_list_appear) {
                auto &item = item_pair.first;

                // Appear more than 1 times counts 1 for calculating confidence
                if (corr_appear.find(item) == corr_appear.end()) {
                    corr_appear[item] = 1;
                } else {
                    corr_appear[item] += 1;
                }
            }
        }

        // --- 3. Finally count each possible subsequent items. Check min_support and calculate confidence if valid
        map<inodeno_t, pair<int, float> > this_rule_set;
        for (auto &item_pair: corr_freqs) {

            auto &item = item_pair.first; // 元素
            auto &occur_times = item_pair.second; // 一共出现的次数

            if (occur_times < min_support) {
                continue;
            }

            auto this_conf = float(corr_appear[item]) / item_freq;
            if (this_conf > min_confidence) {
                this_rule_set[item] = make_pair(corr_appear[item], this_conf);
            }
        }
        rules[header] = this_rule_set;
    }

    correlation_series.push_back(move(rules));
    if(correlation_series.size() > max_epoch) {
        correlation_series.erase(correlation_series.begin());
    }

    // 从最新的correlation_series整合出correlation_table
    from_series_to_table();
}

void CMiner::from_series_to_table(){
    for(auto &rules: correlation_series){

        for(auto &item: rules){
            auto &rule = rules[item.first];

            for(auto &item_tail: item.second){   // head---tail num，num
                
                if(correlation_table.find(item.first) == correlation_table.end()){
                    correlation_table[item.first][item_tail.first] = item_tail.second;
                }
                else if(correlation_table[item.first].find(item_tail.first) == correlation_table[item.first].end()){
                    correlation_table[item.first][item_tail.first] = item_tail.second;
                }
                else{
                    correlation_table[item.first][item_tail.first].second = correlation_table[item.first][item_tail.first].first / correlation_table[item.first][item_tail.first].second + item_tail.second.first / item_tail.second.second;
                    correlation_table[item.first][item_tail.first].first += item_tail.second.first;
                    correlation_table[item.first][item_tail.first].second = correlation_table[item.first][item_tail.first].first / correlation_table[item.first][item_tail.first].second;
                }
            }
        }
    }

    // check overall_min_support and overall_min_confidence if valid
	for(auto &item: correlation_table){
        
        for(auto &item_tail: item.second){

            if(correlation_table[item.first][item_tail.first].first < overall_min_support || correlation_table[item.first][item_tail.first].second < overall_min_confidence){
                
                correlation_table[item.first].erase(item_tail.first);
                if(correlation_table[item.first].size() == 0){
                    correlation_table.erase(item.first);
                }         
            }            
        }
    }   
}

void *CMiner::entry() {
	dout(0) << "Thread created." << dendl;

	while (1) {
		//mine_lock.Lock();
		//mine_cond.Wait(mine_lock);
		sleep(9);

		if (balancer->get_beat_epoch() - last_epoch >= 6 * 3) {
			dout(0) << "CMiner start processing." << dendl;
			last_epoch = balancer->get_beat_epoch();
			process();
			dout(0) << "CMiner finish processing." << dendl;

			// debug info
			int curr_index = 0;
			for (auto &map_item : correlation_series) {
				dout(0) << "current index: " << ++curr_index << dendl;
				for (auto &inode : map_item) {
					auto& curr_inode = inode.first;
					auto& miner_series = inode.second;

					dout(0) << "inode number: " << curr_inode << dendl;

					for (auto &relation_pair : miner_series) {
						auto &related_inode = relation_pair.first;
						auto &data = relation_pair.second;

						dout(0) << "    related inode number: " << related_inode << " (" << data.first << ", " << data.second << ")" << dendl;
					}
				}
			}
		}

		//mine_lock.Unlock();
	}

	return nullptr;
}

void CMiner::signal_miner() {
	mine_lock.Lock();
	mine_cond.Signal();
	mine_lock.Unlock();
}
