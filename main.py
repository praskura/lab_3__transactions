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
        log_file = open(settings.transaction_data_file_path)
        log = log_file.read()
        log_file.close()
        os.remove(settings.transaction_data_file_path)
        log_info = log.split(':')

        stage_number = log_info[0]
        stage_value = log_info[1]
        conn = await asyncpg.connect(dsn=settings.database_dsn)

        if stage_number == '1':
            yield
        if stage_number == '2':
            restore_incr_query = '''
            UPDATE public.increasing
                SET value=$1
            '''
            await conn.execute(restore_incr_query, int(stage_value))
            await conn.close()

        logging.info('Previous transaction has been restored')
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/transact")
async def transact(value: int = 0):
    conn = await asyncpg.connect(dsn=settings.database_dsn)
    logging.info('DB connected')

    log_file = open(settings.transaction_data_file_path, 'w')
    current_decr_value_query = '''
        SELECT value
            FROM public.decreasing;
        '''
    current_decr_value = await conn.fetchval(current_decr_value_query)

    current_incr_value_query = '''
        SELECT value
            FROM public.increasing;
        '''
    current_incr_value = await conn.fetchval(current_incr_value_query)

    expected_decr_value = current_decr_value - value
    expected_incr_value = current_incr_value + value
    
    with open(settings.transaction_data_file_path, 'w') as log_file:
        log_file.write(f'1:{expected_decr_value}')

    decr_query = '''
        UPDATE public.decreasing
            SET value=$1;
        '''
    logging.info(f'Got value {value}')

    logging.info('Decreasing started')
    await conn.execute(decr_query, expected_decr_value)
    logging.info('Decreasing finished')

    with open(settings.transaction_data_file_path, 'w') as log_file:
        log_file.write(f'2:{expected_incr_value}')

    logging.info('Sleep...')
    await asyncio.sleep(5)
    logging.info('Wakeup')

    incr_query = '''
        UPDATE public.increasing
            SET value=$1;
        '''

    logging.info('Increasing started')
    await conn.execute(incr_query, expected_incr_value)
    logging.info('Increasing finished')
    logging.info('Removing transaction data file since the transaction has been successfully finished')
    log_file.close()
    os.remove(settings.transaction_data_file_path)

    await conn.close()
    logging.info('DB connection closed')

    return {
        "status": "ok",
        "value": value
    }
