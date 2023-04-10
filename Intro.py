import asyncio
import aiohttp
import datetime
from pprint import pprint
from dotenv import load_dotenv
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import JSON, Integer, Column
from sqlalchemy.exc import IntegrityError


CHUNK_SIZE = 5


load_dotenv()
PG_DSN = f'postgresql+asyncpg://{os.getenv("PG_USER")}:{os.getenv("PG_PASSWORD")}@{os.getenv("PG_HOST")}:' \
      f'{os.getenv("PG_PORT")}/{os.getenv("PG_DB")}'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class SwapiPeople(Base):
    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    json = Column(JSON)


async def main(total, start_pos=1, chunk=CHUNK_SIZE):
    # Существует несколько способов запуска coroutines (асинхронных функций)
    # из другой асинхронной функции

    # # Способ 1:
    # res_1 = await hello('Ivan')
    # print(res_1)
    # print('*' * 50)
    #
    # # Способ 2:
    # coroutine = hello('Petr')
    # res_2 = await coroutine
    # print(res_2)
    # print('*' * 50)
    #
    # # Способ 3:
    # res_3 = hello('Lena')
    # res_4 = hello('Sveta')
    # results = await asyncio.gather(res_3, res_4)
    # print(results)
    async with engine.begin() as con:
        # await con.run_sync(Base.metadata.drop_all)
        await con.run_sync(Base.metadata.create_all)

    current_pos = start_pos
    while current_pos <= total:
        if current_pos + chunk < total:
            end_pos = current_pos + chunk
        else:
            end_pos = total + 1

        # Получаем объекты от ресурса
        async with aiohttp.ClientSession() as session:
            # вызываем асинхронные coroutines
            coros = [get_character(session, char_id, fields=['species', 'starships', 'films', 'vehicles', 'homeworld'])
                     for char_id in range(current_pos, end_pos)]
            results = await asyncio.gather(*coros)
        pprint(results)
        print('Q-ty of characters:', len(results))
        # await session.close()
        # coro_1 = get_character(1)
        # coro_2 = get_character(2)
        # results = await asyncio.gather(coro_1, coro_2)
        # pprint(results)
        # print(len(results))

        # Вставка объектов в БД
        asyncio.create_task(paste_to_db(results))
        current_pos += chunk

    all_tasks = asyncio.all_tasks()
    all_tasks = all_tasks - {asyncio.current_task()}
    await asyncio.gather(*all_tasks)
    # paste_to_db_task = asyncio.create_task(paste_to_db(results))
    # await paste_to_db_task


async def hello(name):
    print(f'hello, {name}')
    return name


async def get_character(client_session, character_id, fields=None):
    url = 'https://swapi.dev/api/people'

    print(f'{character_id} started')
    async with client_session.get(f'{url}/{character_id}') as response:
        json_data = await response.json()

        if fields is not None:
            for field in fields:
                field_links = json_data.get(field, [])
                field_coro = download_links(client_session, field_links, field)
                field_data = await field_coro
                json_data[field] = field_data

    json_data['id'] = character_id
    del json_data['created']
    del json_data['edited']
    del json_data['url']
    print(f'{character_id} finished')
    return json_data
    # print(f'{character_id} started')
    # client_session = aiohttp.ClientSession()
    # response = await client_session.get(f'https://swapi.dev/api/people/{character_id}')
    # json_data = await response.json()
    # _ = await client_session.close()
    # print(f'{character_id} ready')
    # return json_data


async def download_links(client_session: aiohttp.ClientSession, links: list | str, field: str):
    if type(links) == list:
        coros = [client_session.get(link) for link in links]
        responses = await asyncio.gather(*coros)

        response_data_coros = [response.json() for response in responses]
        result = await asyncio.gather(*response_data_coros)

        # Извлекаем только нужное нам поле ("название") из каждого объекта, полученного по ссылке
        if field in ['species', 'starships', 'vehicles']:
            result = ','.join([item.get('name', '') for item in result])
        elif field in ['films']:
            result = ','.join([item.get('title', '') for item in result])
    else:
        coro = client_session.get(links)
        response = await coro
        result = await response.json()
        result = result['name']

    return result


async def paste_to_db(data):
    async with Session() as session:
        try:
            objects = [SwapiPeople(id=item.pop('id'), json=item) for item in data]
            session.add_all(objects)
            await session.commit()
        except IntegrityError:
            await session.rollback()
            print('Данные уже есть в БД!')


start = datetime.datetime.now()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main(total=12))
end = datetime.datetime.now()
print('Execution time:', end - start)
