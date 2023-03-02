# Orderbook Aggregator

A gRPC server which connects to N exchange websockets for a configurable currency pair and streams an order book summary consisting of an aggregated top-*n* levels and the aggregated spread. 

Where possible, diff/delta websocket channels are preferable to channels which push periodic full orderbook snapshots as it reduces network and processing loads, enabling downstream consumers to react quicker to market events. 

However, some exchange websocket APIs have issues on certain channels making the Delta channel unfeasable.

## Design

<kbd><img src="https://i.ibb.co/zSTg3fc/mermaid-diagram-2023-03-02-023950.png" alt="drawing" width="460" height="270"  style="border:1px solid black;"/></kbd>  &nbsp; 

The aggregator, gRPC server and each websocket client are Tokio tasks, which communicate using Tokio channels. The message flow is one-directional.

## Features 

- Generic over symbol, number of exchanges aggregated over and the depth.
- Automatic reconnection attempts (stale data dropped when a websocket client loses connection)
- `OrderbookWebsocketClient` trait provided, facilitating rapid new exchange implementation by extending a minimal set of async fns.

 ## Run

Service can be configured in `config.yml`.

`cargo run --release service`

## Testing as client

`grpcurl` can be used as a test client to print received messages to stdout (execute the following command from the manifest directory): 

```bash
grpcurl -plaintext -import-path ./proto -proto orderbook.proto  '[::]:50051' orderbook.OrderbookAggregator/BookSummary
```
## Extension

Possible scopes for extension may include:
- **truncate by volume**: as the top orders may be for insignificant quantities, truncating on cumulative order quantity may be another useful trader signal;
- **performance**: 
    - the `BTreeMap` used in the aggregator to maintain state of a combined orderbook uses the std lib defaults, but it's possible an alternative implementation could be optimal for our use case; 
    - we deserialize websocket message price and quantity fields into `rust_decimal::Decimal`, to support precise floating point arithmetic. It may be worth profiling alternatives if performance is a primary consideration; 
    - certain websocket event message validation could be omitted if trading in conditions of extreme sensitivity to latency - websocket events can be assumed to fail validation very rarely, so it may be a calculated risk in some contexts. In particular we can skip deserializing certain fields; 
    - profiling of the number of threads in the runtime with different numbers of exchanges.
- **security**: Upgrading gRPC to serve the stream over TLS would eliminate the risk of nefarious actors corrupting the datastream.  
- **logging and instrumentation**: if this were a real production service logging and instrumentation would be essential.

## Unimplemented
- exchange symbol validation via restful endpoints, e.g. https://www.bitstamp.net/api/v2/trading-pairs-info/
