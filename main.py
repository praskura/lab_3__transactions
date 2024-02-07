import os
import asyncpg
import time
import logging
from fastapi import FastAPI
from settings import Settings

app = FastAPI()
settings = Settings()
logging.basicConfig(format='%(asctime)s.%(msecs)03d - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

app = FastAPI()

@app.get("/transact")
async def transact(v: int = 0):
    conn = await asyncpg.connect(dsn=settings.database_dsn)
    logging.info('DB connected')

    decr_query = '''
        UPDATE public.decreasing
            SET value=value-$1;
        '''

    logging.info('Decreasing started')
    await conn.execute(decr_query, v)
    logging.info('Decreasing finished')

    logging.info('Sleep...')
    time.sleep(5)
    logging.info('Wakeup')

    incr_query = '''
        UPDATE public.increasing
            SET value=value+$1;
        '''

    logging.info('Increasing started')
    await conn.execute(incr_query, v)
    logging.info('Increasing finished')

    await conn.close()
    logging.info('DB connection closed')

    return {
        "status": "ok",
        "value": v
    }
