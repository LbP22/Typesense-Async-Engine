import asyncio
import aiohttp


class TypeSenseEngine():
    def __init__(self, base_url: str, api_key: str):
        auth = api_key
        base_url = base_url

    async def get_collections(self):
        async with aiohttp.ClientSession() as client:
            response = await client.get(
                f'{self.base_url}/collections',
                headers={'x-typesense-api-key': self.auth}
            )
            collections = [c['name'] for c in (await response.json())]
            return collections


    async def create_collection(self, collection_name: str, params: list[dict]):
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                f'{self.base_url}/collections',
                headers={'x-typesense-api-key': self.auth},
                json={
                    'name': collection_name,
                    'fields': params,
                }
            )
            return await response.json()


    async def drop_collection(self, collection_name: str):
        async with aiohttp.ClientSession() as client:
            response = await client.delete(
                f'{self.base_url}/collections/{collection_name}',
                headers={'x-typesense-api-key': self.auth}
            )
            return await response.json()


    async def drop_collections(self):
        collections = await self.get_collections()
        for collection_name in collections:
            async with aiohttp.ClientSession() as client:
                response = await client.delete(
                    f'{self.base_url}/collections/{collection_name}',
                    headers={'x-typesense-api-key': self.auth}
                )
                await response.text()


    async def put(self, collection_name: str, data: dict):
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                f'{self.base_url}/collections/{collection_name}/documents',
                headers={'x-typesense-api-key': self.auth},
                json=data
            )
            return await response.json()


    async def get(self, collection_name: str, obj_id: str):
        async with aiohttp.ClientSession() as client:
            response = await client.get(
                f'{self.base_url}/collections/{collection_name}/documents/{obj_id}',
                headers={'x-typesense-api-key': self.auth}
            )
            to_return = await response.json()
            if to_return.get('message', False):
                if not 'Could not find' in to_return['message']:
                    print('Can\'t get object from TypeSense', to_return)
                return None
            return to_return


    async def edit(self, collection_name: str, obj_id: str, data: dict):
        async with aiohttp.ClientSession() as client:
            response = await client.patch(
                f'{self.base_url}/collections/{collection_name}/documents/{obj_id}',
                headers={'x-typesense-api-key': self.auth},
                json=data
            )
            return await response.json()


    async def delete(self, collection_name: str, obj_id: str):
        async with aiohttp.ClientSession() as client:
            response = await client.delete(
                f'{self.base_url}/collections/{collection_name}/documents/{obj_id}',
                headers={'x-typesense-api-key': self.auth}
            )
            return await response.json()


    async def search(self, collection_name: str, query: str, params: dict):
        if params is None or len(params) == 0:
            return None

        async with aiohttp.ClientSession() as client:
            response = await client.get(
                f'{self.base_url}/collections/{collection_name}/documents/search',
                headers={'x-typesense-api-key': self.auth},
                params={'q': query, **params}
            )
            return await response.json()
