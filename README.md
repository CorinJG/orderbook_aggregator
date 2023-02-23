# Orderbook Aggregator

A gRPC server streaming a summary of an aggregated orderbook (truncated to top-*n* levels) and the aggregated spread, upon update from any exchange. 

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