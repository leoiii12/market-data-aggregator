# Market Data Aggregator

## Assumptions

1. It is part of the distributed system. Protobuf is being used as the message format.
2. Assume the number of symbols is bounded, so the system can store in memory.

## Features

1. A priority queue is implemented. When a symbol is fired with higher frequency, it has higher
   priority to be published.

## To Improve

1. Make the rate limiter to be compatible with the RxJava TestScheduler. Now the test case for
   MarketDataProcessor is implemented with Thread.sleep.