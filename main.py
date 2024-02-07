import os
import asyncpg
import time
from fastapi import FastAPI
from settings import Settings

app = FastAPI()
settings = Settings()

app = FastAPI()

@app.get("/transact")
async def transact(v: int = 0):
    conn = await asyncpg.connect(dsn=settings.database_dsn)

    decr_query = '''
        UPDATE public.decreasing
            SET value=value-$1;
        '''

    await conn.execute(decr_query, v)

    time.sleep(5)

    incr_query = '''
        UPDATE public.increasing
            SET value=value+$1;
        '''

    await conn.execute(incr_query, v)

    await conn.close()

    return {
        "status": "ok",
        "value": v
    }
