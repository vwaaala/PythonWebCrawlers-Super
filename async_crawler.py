from itertools import islice
import asyncio
import aiohttp
import aiofiles
import ssl


class AsyncCrawler:
    def __init__(self, max_concurrency=200):
        self.max_concurrency = max_concurrency
        self.session = None
        self.bounded_semaphore = None

    async def __aenter__(self):
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.set_ciphers('HIGH:!DH:!aNULL')
        self.session = aiohttp.ClientSession()
        self.bounded_semaphore = asyncio.BoundedSemaphore(self.max_concurrency)
        return self

    async def __aexit__(self, *err):
        await self.session.close()
        self.session = None

    async def _http_request(self, url):
        async with self.bounded_semaphore:
            max_retry = 5
            while True:
                try:
                    async with self.session.get(url, timeout=30, ssl=self.ssl_ctx) as response:
                    #async with self.session.request("GET", url, timeout=30, ssl=False) as response:
                        response.raise_for_status()
                        html = await response.read()
                        return html
                #except aiohttp.ClientConnectionError as e:
                #    print('Error handled')
                except aiohttp.ClientError:
                    max_retry -= 1
                    if max_retry == 0:
                        return None
                    print("Retry on url: ", url)

    async def extract_async(self, url):
        data = await self._http_request(url)
        return url, data

    # TODO
    #async def extract_multi_async(self, to_fetch):
    #    return await asyncio.gather(*[self.extract_async(url) for url in to_fetch], return_exceptions=True)

    #async def download_file(self, url, local_file_path):
    #    async with self.bounded_semaphore:
    #        #async with self.session.get(url, timeout=30) as response:
    #        async with self.session.get(url, timeout=30, ssl=self.ssl_ctx) as response:
    #            if response.status == 200:
    #                f = await aiofiles.open(local_file_path, mode='wb')
    #                await f.write(await response.read())
    #                await f.close()

    async def download_file(self, url, local_file_path):
        data = await self._http_request(url)
        f = await aiofiles.open(local_file_path, mode='wb')
        await f.write(data)
        await f.close()

    @staticmethod
    def limited_as_completed(coroutines, limit=1):
        futures = [asyncio.ensure_future(c) for c in islice(coroutines, 0, limit)]

        async def first_to_finish():
            while True:
                await asyncio.sleep(0)
                for f in futures:
                    if f.done():
                        futures.remove(f)
                        try:
                            new_future = next(coroutines)
                            futures.append(asyncio.ensure_future(new_future))
                        except StopIteration as e:
                            pass
                        return f.result()

        while len(futures) > 0:
            yield first_to_finish()
