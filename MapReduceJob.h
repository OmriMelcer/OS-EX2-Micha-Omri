#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
#include "MapContext.h"
#include "ReduceContext.h"
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>
#include <barrier>
// you can add other includes here

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	inline bool operator==(const MapReduceState &other) const
	{
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	void wait(void);

	OutputVec getOutput(void);

private:
	/*
		You can change everything on this part (these are just recommendations)
	*/
  const InputVec& inputVec;
  const MapReduceClient& client;
  const int numThreads;
  std::atomic<int> indexer;
  std::atomic<uint64_t> state_incoder;
  std::vector<std::thread> threads;
  std::shared_ptr<OutputVec> outputVec;
  std::vector<IntermediateVec> shuffleQueue;
  std::mutex outputVecMutex;
  std::vector<MapContext> mapContexts; 
  ReduceContext reduceContext;
  std::barrier<> barrier;
  std::mutex waitMutex;
  bool joined;
  void update_state(MapReduceStage new_stage, uint64_t to_do);
  void threadWorker(int id);
  void shuffleFunc();
  int find_max_back() const;
};
	
#endif // MAP_REDUCE_JOB_H
