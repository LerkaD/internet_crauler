from datetime import datetime
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import aiofiles
from urllib.parse import urlparse

START_URL = 'https://www.f1news.ru/'
COUNT = 3  # глубина рекурсии
FORMAT = '%M:%S'
visited_urls = set()
written_links = set()

async def write_links(link: str):
    if link not in written_links:
        written_links.add(link)
        async with aiofiles.open('crauler_links.txt', mode='a') as f:
            await f.write(link + ' ___ ' + datetime.now().strftime(FORMAT) + '\n')

def is_external_link(base_url: str, link: str) -> bool:
    # Проверяем, что ссылка абсолютная и домен отличается от базового
    if not link.startswith('http'):
        return False
    base_domain = urlparse(base_url).netloc
    link_domain = urlparse(link).netloc
    return link_domain != '' and link_domain != base_domain

async def aio_request(url: str, count: int, session: aiohttp.ClientSession):
    if count == 0 or url in visited_urls:
        return
    print(f'Processing {url} with depth {count} at {datetime.now().strftime(FORMAT)}')
    visited_urls.add(url)
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Failed to fetch {url} with status {response.status}")
                return
            html = await response.text()
            soup = BeautifulSoup(html, features="xml")
            external_links = set()
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if isinstance(href, str) and href.startswith('http'):
                    if is_external_link(url, href):
                        # Записываем внешние ссылки
                        external_links.add(href)
                        print()
                        await write_links(href)
            # Рекурсивная обработка внешних ссылок с уменьшением count
            # if len(external_links) > 3:
            #     external_links = list(external_links)[:3]
            # print('external_links',len(external_links), external_links)
            # await asyncio.sleep(5)
            tasks = [aio_request(link, count - 1, session) for link in external_links if link not in visited_urls]
            await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Error processing {url}: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        await aio_request(START_URL, COUNT, session)

if __name__ == '__main__':
    asyncio.run(main())
