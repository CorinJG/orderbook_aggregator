# Orderbook Aggregator

A gRPC server which connects to N exchange websockets for a configurable currency pair and streams an order book summary consisting of an aggregated top-*n* levels and the aggregated spread. 

## Features 

- Generic over symbol, number of exchanges and depth
- Automatic reconnection (stale data dropped when a websocket client loses connection)
- Truncated deserialization - only the first \<depth\> levels in the asks and bids fields in a snapshot payload are deserialized, skipping the remaining values
- Delta clients "fast-forward" by updating local state without sending updates downstream for a short period immediately following synchronization, to avoid stale updates being forwarded 
- `WebsocketClient` trait, facilitating new exchange implementation by extending a set of async methods.


Where feasible, delta channels are preferable to periodic full orderbook snapshots as it reduces network and processing loads, enabling downstream consumers to react quicker to market events. 

## Design

<kbd><img src="https://i.ibb.co/zSTg3fc/mermaid-diagram-2023-03-02-023950.png" alt="drawing" width="460" height="270"  style="border:1px solid black;"/></kbd>  &nbsp; 

The components are Tokio tasks, which communicate using channels. The message flow is one-directional.

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
    - By far the most significant factor is network latency between client and exchange.
    - How the aggregator stores its state is an area for exploration. A `BTreeMap` combinding all order books for all exchanges is efficient when clients are sending delta updates, but when a subset of clients are sending snapshots, regularly removing all levels from a particular exchange is slow. An alternative would be to keep separate `Vec`s for each exchange, and visit each of them when producing summaries.
    - May be worth profiling alternatives to `rust_decimal::Decimal`
    - Certain websocket event message validation could be omitted if trading in conditions of extreme sensitivity to latency - events fail validation very rarely, although with potentially serious consequences. It may be a calculated risk to skip deserializing certain message fields in some contexts.
    - Profiling of the number of threads in the runtime with different numbers of exchanges.
    - Mutexes not required if using a local task set on a single thread
- **security**: Upgrading gRPC to TLS would eliminate the risk of nefarious actors corrupting the datastream.  
- **logging and instrumentation**: if this were a real production service logging and instrumentation would be essential.

## Unimplemented
- exchange symbol validation via restful endpoints, e.g. https://www.bitstamp.net/api/v2/trading-pairs-info/. 
