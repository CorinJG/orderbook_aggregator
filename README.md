# Orderbook Aggregator

A gRPC server streaming a summary of an aggregated orderbook (truncated to top-*n* levels) and the aggregated spread, upon update from any exchange. 

Where possible, diff/delta websocket channels are preferable to channels which push periodic full orderbook snapshots as it significantly reduces our network and server load, enabling us to react quicker to market events. 

## Design

<kbd><img src="https://github.com/CorinJG/orderbook_aggregator/blob/master/mermaid.png" alt="drawing" width="500" height="200"  style="border:1px solid black;"/></kbd>  &nbsp; 

Components communicate using Tokio channels.

 ## Run

Service can be configured in `config.yml`.

`cargo run --release service`

## Testing as client

`grpcurl` can be used as an ad-hoc client to test connection to the server (execute the following command from manifest directory): 

```bash
grpcurl -plaintext -import-path ./proto -proto orderbook.proto  '[::]:50051' orderbook.OrderbookAggregator/BookSummary
```
## Documentation

Documentation can be viewed with `cargo doc --open`.

## Extensions

Possible scopes for extension include:
- **generalizing to n exchanges**
- **truncate by volume**: as the top orders may be for insignificant quantities, truncating on cumulative order quantity instead may be a more useful trader signal.
- **performance**: we make heavy use of the `rust_decimal` crate for precise numerical accounting. Whilst this is far superior to strings or floats, it may not be designed with performance as its primary goal; certain websocket event message validation could be omitted if trading in conditions of extreme sensitivity to latency (websocket events can be assumed to fail validation very rarely, so it may be a caalculated risk in some contexts); whilst a `BTreeMap` is a good candidate for our local version of the orderbook, the std lib implementation uses sensible defaults which could be tuned to the use case; when a single-threaded async runtime is used the channels don't need to be `Send` so the use of atomics can be avoided by using local task sets;
- **security**: Upgrading gRPC to serve the stream over TLS would eliminate the risk of nefarious actors corrupting the datastream.  
- **logging and instrumentation**: if this were a real production service then logging and instrumentation would be essential.
- **extending the `.proto` schema**: for example, clients could be notified if m/n exchanges have healthy connections.