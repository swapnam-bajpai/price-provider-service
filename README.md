# price-provider-service

A service that serves multiple producers and consumers in parallel for price discovery.

Producers can push Price data by the following steps :
1. Initiating the batch upload
2. Sending price data in chunks of size 1000
3. Signalling completing or cancellation of the batch upload

The service is resilient against requests that do not follow the above steps and arrive in incorrect order.

Consumers can get the latest (timestamp ordering) Price data by providing the instrument id.

Producer and Consumer implementations are also provided as examples on how to interact with the service.
Please see the tests for typical legitimate / illegitimate scenarios and expected outcomes.

Design decisions and performance characteristics :

1. Read-write locking with fairness enabled is used in the critical section of reading/writing the underlying price information. This ensures thread safety against race conditions while preventing starvation. 
2. The service maintains an unbounded thread pool to serve producer/consumer requests. This means threads are created and destroyed as per the demand but also implies that a large number of simultaneous requests can lead to context switching overhead.
3. There is no mechanism currently enabled for safety against rogue producers that initiate a batch request and then disappear without completing or cancelling. An improvement is to timeout such inactive clients and free up the memory allocated to their partially submitted price data.
4. There is no congestion control / throttling mechanism currently enabled that would ensure that a particular producer or consumer doesn't overwhelm the service with too many requests. This should be delegated to a separate authentication layer that manages resource allocation to different clients depending on business policy.
