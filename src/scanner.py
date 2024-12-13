import redis
import ast

class Scanner:

    def __init__(self):
        self.config_db = redis.Redis(db = 0, decode_responses = True)
        self.market_data_db = redis.Redis(db = 1, decode_responses = True)
    
    def runScan(self, filer_1000 = True):

        exchanges = ["binance", "bybit", "gate", "kucoin", "okx"]
        market = "ufuture"
        symbols_list = {}
        funding_rate_map = {}
        max_funding_rate_map = {}
        for exchange in exchanges:
            res = self.config_db.hget(f"{exchange}:{market}:common:symbol_list", "symbols")
            if res != None:
                symbols_list[exchange] = ast.literal_eval(res)
        for i in symbols_list:
            for j in symbols_list[i]:
                if filer_1000:
                    if "1000" not in j:
                        res = self.market_data_db.hget(f"{i}:funding_rate", j)
                        if res != None:
                            funding_rate = ast.literal_eval(res)
                            if funding_rate["funding_rate"] > 0:
                                funding_rate_map[j] = {i: funding_rate["funding_rate"]}
        for i in funding_rate_map:
            if (len(funding_rate_map[i]) > 1):
                exchange = max(funding_rate_map[i], key = lambda x: funding_rate_map[i][x])
                funding_rate = funding_rate_map[i][exchange]
            else:
                exchange = list(funding_rate_map[i].keys())[0]
                funding_rate = funding_rate_map[i][exchange]
                max_funding_rate_map[i] = {"exchange": exchange, "funding_rate": funding_rate}

        sorted_max_funding_rate_map = dict(sorted(max_funding_rate_map.items(), key = lambda x: x[1]["funding_rate"], reverse = True))
        return sorted_max_funding_rate_map
