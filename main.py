import asyncio
import aiohttp
import tqdm
import yarl
import pathlib
import logging
import tempfile
import pyzstd
import tarfile
import os
import sys
import time


async def download(url: yarl.URL, session: aiohttp.ClientSession, sem: asyncio.Semaphore, dest: pathlib.Path) -> int:
    async with sem:
        logging.info('Acquired semaphore.')
        async with session.get(url) as response:
            if not response.ok:
                logging.error(f'GET {url} returned {response.status}: {response.reason}')
                print(f'GET {url} returned {response.status}: {response.reason}')
                return 1
            else:
                logging.info(f'GET {url} returned {response.status}: {response.reason}')
            logging.debug('Extracted total file size from headers.')
            total = int(response.headers.get('content-length'))
            pb = tqdm.tqdm(desc=dest.name if len(dest.name) < 36 else dest.name[:33] + '...', total=total, unit='B', unit_scale=True, bar_format='{desc:72}{percentage:3.0f}%|{bar:72}{r_bar}', file=sys.stdout)

            with open(dest, mode='wb') as fout:
                async for chunk in response.content.iter_chunked(512):
                    logging.debug('Got another 512 bytes.')
                    fout.write(chunk)
                    logging.debug('Wrote chunk to file.')
                    pb.update(len(chunk))
            pb.refresh()
            pb.close()


def install_package(package: pathlib.Path, *, overwrite: bool = False) -> None:
    # Make sure that the monkeys calling this function are actually passing in a file as the package
    assert package.is_file(), "package is not a file"
    # assert package.is_absolute(), "package path is not absolute"

    logging.debug('Creating temporary file.')
    archive = tempfile.TemporaryFile()
    fp = open(package, 'rb')
    logging.info('Decompressing Zstandard-compressed archive...')
    tqdm.tqdm.write('Decompressing Zstandard-compressed archive...', file=sys.stdout)
    pyzstd.decompress_stream(fp, archive)
    # Move the cursor back to the beginning of the file (current at the end because we just wrote to it)
    logging.debug('Seeking to position 0 in decompresssed archive.')
    archive.seek(0)
    logging.debug('Opening decompressed archive with tarfile.')
    tar_archive = tarfile.open(fileobj=archive)
    total_size = 0
    to_be_extracted = []
    logging.debug('Finding which files to extract and total size.')
    for i in tar_archive.getmembers():
        if not i.name.startswith('.'):
            logging.debug(f'{i.name} does not start with .')
            if os.path.exists(i.name.replace('data', '')):
                if not overwrite:
                    logging.warning(f'{i.name} already exists in the filesystem. (Won\'t extract)')
                else:
                    logging.debug(f'{i.name} will be extracted.')
                    to_be_extracted.append(i)
                    total_size += i.size
            else:
                logging.debug(f'{i.name} will be extracted.')
                to_be_extracted.append(i)
                total_size += i.size
    a = f'Installing {package.name}'
    pb = tqdm.tqdm(desc=a if len(a) < 36 else a[:33] + '...', total=total_size, unit='B', unit_scale=True, bar_format='{desc:72}{percentage:3.0f}%|{bar:72}{r_bar}', file=sys.stdout)
    for i in to_be_extracted:
        logging.debug(f'Extracted {i.name}.')
        tar_archive.extract(i, 'install/', numeric_owner=True)
        pb.update(i.size)
    pb.refresh()
    pb.close()


if __name__ == '__main__':
    async def main():
        sites = [
            "https://mirror.pkgbuild.com/core/os/x86_64/linux-5.11.13.arch1-1-x86_64.pkg.tar.zst",
            "https://mirror.pkgbuild.com/core/os/x86_64/linux-headers-5.11.13.arch1-1-x86_64.pkg.tar.zst",
            "https://mirror.pkgbuild.com/core/os/x86_64/man-db-2.9.4-1-x86_64.pkg.tar.zst",
            "https://mirror.pkgbuild.com/core/os/x86_64/tar-1.34-1-x86_64.pkg.tar.zst",
            "https://mirror.pkgbuild.com/core/os/x86_64/util-linux-2.36.2-1-x86_64.pkg.tar.zst"
            ]

        session = aiohttp.ClientSession()
        sem = asyncio.Semaphore(3)

        coros = [download(yarl.URL(i), session, sem, pathlib.Path('download/' + i.split('/')[-1])) for i in sites]
        print('Downloading packages...')
        await asyncio.gather(*coros)
        await session.close()

        for i in sites:
            print('')
            install_package(pathlib.Path('download/' + i.split('/')[-1]), overwrite=True)
    logging.basicConfig(filename='stest.log', level=logging.DEBUG)
    asyncio.run(main())

