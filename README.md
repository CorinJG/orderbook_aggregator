# Orderbook Aggregator

A gRPC server streaming a summary of an aggregated orderbook for a given currency pair (truncated to top-*n* levels) and the aggregated spread. Streams a message upon update. 

Where possible, diff/delta websocket channels are preferable to channels which push periodic full orderbook snapshots as it significantly reduces network and server loads, enabling downstream consumers to react quicker to market events. 

However, I believe some exchange websocket APIs have issues on certain channels making the Delta channel unfeasable.

## Design

<kbd><img src="https://mermaid.ink/img/pako:eNqNj0Frg0AQhf_KMCcDZlGj3WYPhdDSUw8lHgLFy0ZHE-K6YV3bWvW_dxsLIdBD5zTz5n2PmQFzXRAKLGv9kR-ksfCyzRpwtQu9He1bnZ_IhgtYLh9GbQoysNf6BN25kJbaETbepqoMVdJqs_gloysZXUj4N9p4wBiDmfrDcCPN0WmnlDT9CM9etX19hJTMO81-9FGRUfJYuBeHHyVDeyBFGQrXFlTKrrYZZs3krLKzOu2bHIU1Hfk4H_p0lJWRCkUp69apZ9m8aX0zoxjwE8WaxQFfxQnnIb9fJ4mPPYooWrEwCKIgju84d-t48vHrEhBO3_XWdzo?type=png" alt="drawing" width="500" height="200"  style="border:1px solid black;"/></kbd>  &nbsp; 

Components communicate using Tokio channels.

 ## Run

Service can be configured in `config.yml`.

`cargo run --release service`

## Testing as client

`grpcurl` can be used as a test client to print received messages to stdout (execute the following command from the manifest directory): 

```bash
grpcurl -plaintext -import-path ./proto -proto orderbook.proto  '[::]:50051' orderbook.OrderbookAggregator/BookSummary
```
## Extensions

Possible scopes for extension include:
- **generalizing to n exchanges**
- **truncate by volume**: as the top orders may be for insignificant quantities, truncating on cumulative order quantity instead may be another useful trader signal.
- **performance**: we deserialize websocket message price and quantity fields into `rust_decimal::Decimal`, to support precise floating point arithmetic. It may be worth profiling alternatives if performance is a primary consideration; certain websocket event message validation could be omitted if trading in conditions of extreme sensitivity to latency - websocket events can be assumed to fail validation very rarely, so it may be a calculated risk in some contexts. In particular we can skip deserializing certain fields; whilst a `BTreeMap` is a good candidate for our local version of the orderbook, the std lib implementation uses sensible defaults but it's notable that in our use case the vast majority of inserts and removes will be close to one end (near the spread); 
- **security**: Upgrading gRPC to serve the stream over TLS would eliminate the risk of nefarious actors corrupting the datastream.  
- **logging and instrumentation**: if this were a real production service logging and instrumentation would be essential.
