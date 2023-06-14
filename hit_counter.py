import faust

app = faust.App('hit_counter', broker="kafka://localhost:29092")

class hitCount(faust.Record, validation=True):
    hits: int
    timestamp: float
    userId: str

class exchangeRate(faust.Record, validation=True): #datatype
    rates: dict
    time_last_update: str

exchange_rate_topic = app.topic("exchange_rate", value_type = exchangeRate)

hit_topic = app.topic("hit_count",value_type=hitCount)
count_topic = app.topic('count_topic', internal=True, partitions=1, value_type=hitCount)

hits_table = app.Table('hitCount', default=int)
count_table = app.Table("major-count",key_type=str,value_type=int,partitions=1,default=int)

@app.agent(hit_topic)
async def count_hits(counts):
    async for count in counts:
        print(f"Data recieved is {count}")
        if count.hits > 20:
            await count_topic.send(value=count)

@app.agent(count_topic)
async def increment_count(counts):
    async for count in counts:
        print(f"Count in internal topic is {count}")
        count_table[str(count.userId)]+=1
        print(f'{str(count.userId)} has now been seen {count_table[str(count.userId)]} times')

@app.agent(exchange_rate_topic)
async def exchange_rate_hit(counts):
    async for count in counts:
        print(f"Data recieved is {count}")
