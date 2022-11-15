import asyncio
import aiohttp

from more_itertools import chunked

import models

MAX_PERSON = 100
CHUNK_SIZE = 20
SWAPI_URL = 'https://swapi.dev/api/'

async def get_resource(session: aiohttp.ClientSession, res_url: str) -> dict:
    response = await session.get(res_url, ssl=False)
    return await response.json(), response.status

async def get_related_attributes(
    session: aiohttp.ClientSession, links_list: list, related_attr: str
    ):
        if links_list is not None:
            attr_list = []
            for link in links_list:
                resource, _ = await get_resource(session, link)
                attr_list.append(resource[related_attr])
            return attr_list
        return links_list
    
async def get_character(session: aiohttp.ClientSession, character_id: int):
    character, status = await get_resource(session, f'{SWAPI_URL}people/{character_id}')
    character.update(id=character_id)
    character.update(status=status)
    character['films'] = await get_related_attributes(
        session=session,
        links_list=character.get('films'),
        related_attr='title'
    )
    character['species'] = await get_related_attributes(
        session=session,
        links_list=character.get('species'),
        related_attr='name'
    )
    character['starships'] = await get_related_attributes(
        session=session,
        links_list=character.get('starships'),
        related_attr='name'
    )
    character['vehicles'] = await get_related_attributes(
        session=session,
        links_list=character.get('vehicles'),
        related_attr='name'
    )
    return character

async def main():
    await models.migrate()
    async with aiohttp.ClientSession() as session:
        for chunk_ids in chunked(range(1, MAX_PERSON + 1), CHUNK_SIZE):
            coroutins = [get_character(session, person_id) for person_id in chunk_ids]
            results = await asyncio.gather(*coroutins)
            await models.transfer_to_db(results)


if __name__ == "__main__":
    asyncio.run(main())
