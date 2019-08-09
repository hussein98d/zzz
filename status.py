import aiohttp
import argparse
import asyncio
import csv
import re
import sys
import time
from urllib.parse import urlparse

# Edit this to adjust how many HTTP requests you can make in parallel
CONCURRENT_REQUESTS = 200
PROTOCOL_PATTERN = re.compile(r'^\w+://')
DOMAIN_PATTERN = re.compile(r'^[^:]+://([^/:]+)')

async def get_status(session, url):
    '''Return the status code for a URL'''
    if PROTOCOL_PATTERN.match(url) is None:
        url = 'http://' + url

    domain = DOMAIN_PATTERN.match(url).group(1)
    rate_lock = next((x for x in options.rate if x.matches(domain)), None)
    if rate_lock:
        await rate_lock.wait()

    async with session.head(url, allow_redirects=True) as response:
        return response.status

async def worker(file, output, options):
    '''
    Reads URLs as lines from a file, requests their status, and outputs the
    results in CSV format. Spawn multiple of these to work on more than one
    URL in parallel.
    '''
    start_time = time.time()
    writer = csv.writer(output)
    ssl_mode = False if options.ignore_ssl_errors else None
    connector = aiohttp.TCPConnector(ssl=ssl_mode)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            line = file.readline()
            if line == '':
                break
            url = line.strip()
            try:
                status = await get_status(session, url)
                writer.writerow(['Status:'status, ' <br><a href='url'>'url'</br>'])
            except Exception as error:
                writer.writerow([url, f'ERROR: {error}'])

            if time.time() - start_time > 2:
                output.flush()


class RateLimit:
    'A lock that can only be acquired at a certain rate.'

    def __init__(self, raw):
        domain = None
        rate_text = raw
        if ':' in raw:
            domain, rate_text = raw.split(':', 1)
        try:
            rate = float(rate_text)
        except ValueError:
            raise ValueError(f'Invalid rate format: "{raw}"')
        
        self.domain = domain
        self.rate = rate
        self.interval = 1 / rate if rate > 0 else 0
        self.last_use = 0
    
    def matches(self, domain):
        'Determine if this rate limit should be used for the given domain.'
        if not self.domain:
            return True

        return domain == self.domain or domain.endswith(f'.{self.domain}')

    async def wait(self):
        'Wait for the next available usage according to the rate limit.'
        remaining = 1
        while remaining > 0:
            remaining = self.last_use + self.interval - time.time()
            if remaining > 0:
                await asyncio.sleep(remaining)

        self.last_use = time.time()


parser = argparse.ArgumentParser(description='Check the statuses of a list of URLs.')
parser.add_argument('path', help='path to a file that is a newline-delimited list of URLs')
parser.add_argument('--ignore-ssl-errors', action='store_true', help='ignore errors in SSL handshakes')
parser.add_argument('--rate', action='append', type=RateLimit, default=[], help='Maximum number of requests per second to make. Repeat with `--rate "example.com:2"` to set specific rate limits per domain.')
options = parser.parse_args()

# Sort rate limits by longest (i.e. most specific) domain first
options.rate.sort(key=lambda x: x.domain or '', reverse=True)

# Start event loop, open the URL list, and spawn workers to read from it.
loop = asyncio.get_event_loop()
with open(options.path) as urls_file:
    workers = [worker(urls_file, sys.stdout, options)
               for i in range(CONCURRENT_REQUESTS)]
    loop.run_until_complete(asyncio.gather(*workers))
