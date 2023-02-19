Server configuration can be done in `config.yml`.

grpcurl command to test server: 

```bash
grpcurl -plaintext -import-path ./proto -proto orderbook.proto  '[::]:50051' orderbook.OrderbookAggregator/BookSummary
```