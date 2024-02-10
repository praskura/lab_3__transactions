import os
import asyncpg
import asyncio
import logging
from fastapi import FastAPI
from settings import Settings
from enum import Enum
from contextlib import asynccontextmanager


settings = Settings()
logging.basicConfig(format='%(asctime)s.%(msecs)03d - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.path.isfile(settings.transaction_data_file_path):
        logging.info('Previous transaction was broken, restoring...')
        transaction_data_file = open(settings.transaction_data_file_path)
        transaction_value = transaction_data_file.read()
        transaction_data_file.close()
        logging.info(f'Rolling back: increase the source by {transaction_value}')
        conn = await asyncpg.connect(dsn=settings.database_dsn)

        rollback_decr_query = '''
            UPDATE public.decreasing
                SET value=value+$1;
            '''

        await conn.execute(rollback_decr_query, int(transaction_value))
        await conn.close()
        logging.info('Successfully rolled back the broken transaction')
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/transact")
async def transact(value: int = 0):
    conn = await asyncpg.connect(dsn=settings.database_dsn)
    logging.info('DB connected')

    transaction_data_file = open(settings.transaction_data_file_path, 'w')

    decr_query = '''
        UPDATE public.decreasing
            SET value=value-$1;
        '''
    logging.info(f'Got value {value}')
    logging.info(f'Write value {value} to the transaction data file')
    transaction_data_file.write(f'{value}')
    transaction_data_file.close()
    logging.info(f'Successfully registered value {value} in the transaction data file')

    logging.info('Decreasing started')
    await conn.execute(decr_query, int(value))
    logging.info('Decreasing finished')

    logging.info('Sleep...')
    await asyncio.sleep(5)
    logging.info('Wakeup')

    incr_query = '''
        UPDATE public.increasing
            SET value=value+$1;
        '''

    logging.info('Increasing started')
    await conn.execute(incr_query, int(value))
    logging.info('Increasing finished')
    logging.info('Removing transaction data file since the transaction has been successfully finished')
    os.remove(settings.transaction_data_file_path)

    await conn.close()
    logging.info('DB connection closed')

    return {
        "status": "ok",
        "value": value
    }
