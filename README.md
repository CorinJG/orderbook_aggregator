Service configuration can be done in `config.yml`.

grpcurl command to test server: 

```bash
grpcurl -plaintext -import-path ./proto -proto orderbook.proto  '[::]:50051' orderbook.OrderbookAggregator/BookSummary
```
# Binance Websocket API notes
Detailed documentation, including isntructions for tracking orderbook state using the Diff Depth channel, can be found at: 

https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md

- A connection lasts for 24h then expect to be disconnected
- Server will send ping frame every 3 minutes
- If server doesn't receive pong frame within 10 mins, client will be disconnected
- Client may send unsolicited pong frames

Example Diff Depth stream URL (no subscription required): 
`wss://stream.binance.com:9443/ws/ethbtc@depth@100ms` 

Example message: 
```
{"e":"depthUpdate","E":1676819103587,"s":"BNBBTC","U":2979280130,"u":2979280131,"b":[["0.01280800","2.47500000"]],"a":[["0.01280900","1.13100000"]]}
```
Rest API URL for initial snapshot: 
`https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000`

