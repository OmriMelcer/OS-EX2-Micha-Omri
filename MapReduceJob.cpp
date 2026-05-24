#include "MapReduceJob.h"

/*
===============================================
Implement:
===============================================
*/


MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
  : inputVec(inputVec), client(client), numThreads(multiThreadLevel),
    indexer(0), state_incoder(0), barrier(multiThreadLevel), joined(false)
{
}

MapReduceState MapReduceJob::getState(void) const
{
  // two MSB are the stage, 31 LSB are the to do, 31 LSB are the finished
uint64_t state_code = state_incoder.load();
MapReduceStage stage = static_cast<MapReduceStage>((state_code >> 62) & 0x3);
uint64_t to_do     = (state_code >> 31) & 0x7FFFFFFF;  // bits 61-31, 31 bits
uint64_t finished  =  state_code        & 0x7FFFFFFF;  // bits 30-0,  31 bits

  double percentage = (to_do == 0) ? 0.0 : (double)finished / to_do * 100.0;
  return MapReduceState{stage, percentage};
}

void MapReduceJob::wait(void)
{
    // TODO: implement this function
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

bool MapReduceJob::isDone(void) const
{
  MapReduceState state = getState();
  return state.stage == REDUCE_STAGE && state.percentage == 100.0;
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}
