# Market Data Aggregator

## Assumptions

1. It is part of the distributed system. Protobuf is being used as the message format.
2. Assume the number of symbols is bounded, so the system can store in memory.
3. Each symbol will have more than one incoming updates per second. The system should accept all
   NewMarketDataMsg, but the system won't produce the more than one update (
   publishAggregatedMarketData) to the client in one second.
4. MarketData.updateTime is just simply one of the property in the data. The execution env time is
   being used for all windowing or rate limit calculations.
5. It is an Akka like system. So I changed the signature
   to `public void onMessage(final @NotNull NewMarketDataMsg newMarketDataMsg)`. Wrapped the data
   point with NewMarketDataMsg.

## Features

1. A priority queue is implemented. When a symbol is fired with higher frequency, it has higher
   priority to be published.

## To Improve

1. Make the rate limiter to be compatible with the RxJava TestScheduler. Now the test case for
   MarketDataProcessor is implemented with Thread.sleep.
2. The priority scope is local.
3. Use Apache Ignite DISTRIBUTED DATA STRUCTURES to support distributed and fault-tolerant calculations
   for the system.