#include "MapContext.h"

// implement here your constructor and destructor

void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{
    IntermediatePair pair (key, value);
    context.push_back(pair);
}
