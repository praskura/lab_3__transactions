import os
import asyncpg
from fastapi import FastAPI
from settings import Settings

app = FastAPI()
settings = Settings()

app = FastAPI()

@app.get("/transact")
async def transact():
    conn = await asyncpg.connect(dsn=settings.database_dsn)

    await conn.execute('''
        UPDATE public.decreasing
            SET value=value-1;
        ''')

    await conn.execute('''
        UPDATE public.increasing
            SET value=value+1;
        ''')

    await conn.close()

    return {"finished": 'ok'}
