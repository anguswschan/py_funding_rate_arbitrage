import redis
import asyncio
from api_service import API

class Scanner:

    def __init__(self):
        self.api = API({})
        self.symbol_db = redis.Redis(db = 1, decode_responses = True)
    
    async def runScan(self):

        exchanges = ["binance", "bybit", "gate", "kucoin", "okx"]
        market = "ufuture"
        symbol_infos = {}
        symbols_list = {}
        for exchange in exchanges:
            symbol_infos[exchange] = await self.api.getSymbolInfo(exchange, market, "")
            symbols_list[exchange] = symbol_infos[exchange].keys()
        for i in symbols_list:
            print(symbols_list[i])
            for j in symbols_list[i]:
                funding = await self.api.getFundingRate(i, market, j)
                if funding != None:
                    self.symbol_db.hset(f"{i}:funding_rate", j, str(funding))
                print(f"exchange: {i}, symbol: {j}, funding: {funding}")
    
    def run(self):
        asyncio.run(self.runScan())

Scanner().run()

