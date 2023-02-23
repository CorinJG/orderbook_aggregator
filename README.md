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

Documentation can be accessed with `cargo doc --open` (from manifest directory).

## Extensions

Possible scopes for exploration include:
- **extending the `.proto` schema**: such that clients are notified on a websocket disconnection instead of being dropped
- **truncate by volume**: truncating the orderbook based on levels is somewhat arbitrary as the top orders may be for insignificant quantities. Truncating the summary based on cumulative order quantity may be a more useful trader signal.
- **performance**: when a single-threaded async runtime is used the channels don't need to be `Send` so the use of atomics can be avoided by using local task sets; certain websocket event message validation could be omitted if trading in conditions of extreme sensitivity to latency (messages can be assumed to fail validation very rarely); whilst a `BTreeMap` is a good candidate for our local version of the orderbook, the standard library implementation uses sensible defaults which could be tuned to the use case (to increase cache hits and optimize the data structure for operations frequent to our use case)
- **security**: gRPC presently operates over an insecure, plain-text channel. In theory Man-in-the-Middle attacks could be costly to a trader using the service. Upgrading to serve the stream over TLS would eliminate this risk; We also make heavy use of the `rust_decimal` crate for precise numerical accounting. Whilst this is far superior to strings or floats, it may not be designed with performance as its primary goal; it's possible that our websocket client implementation or even the exchange API has a bug meaning our local orderbook state isn't accurately tracking the true market. Periodically making a separate request to the exchange's rest API and reconciling our local orderbook state could be desirable, say once every 180s.
- **logging and instrumentation**: if this were a real production service then logging and instrumentation would be essential.