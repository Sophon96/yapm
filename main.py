import asyncio
import aiohttp
import tqdm.asyncio as tqdm
import pathlib
import logging
import tempfile
import pyzstd
import tarfile
import os
import sys
from typing import *


async def download(repo: str,
                   package: str,
                   session: aiohttp.ClientSession,
                   sem: asyncio.Semaphore,
                   dest: pathlib.Path,
                   mirrors: list[str]) -> tuple[int, Union[tqdm.tqdm, None]]:
    """
    Download a package
    """

    # Acquire the semaphore (maximum parallel downloads limit)
    async with sem:
        logging.info('Acquired semaphore.')

        # Try to download from every mirror provided
        response = None
        for url in mirrors:
            actual_url = url.replace(
                r"$repo", repo).replace(
                r"$arch", "x86_64").strip(r"/")
            response = await session.get(f'{actual_url}/{package}')
            # Failed
            if not response.ok:
                logging.error(
                    f'GET {actual_url} returned {response.status}: {response.reason}')
                tqdm.tqdm.write(
                    f'error: GET {actual_url} returned {response.status} {response.reason}')
                response = None
            # Succeeded
            else:
                logging.info(
                    f'GET {actual_url} returned {response.status}: {response.reason}')
                break

        # Couldn't download from any mirror
        if response is None:
            return 1, None

        # Grab the total filesize from the HTTP headers
        logging.debug('Extracted total file size from headers.')
        total = int(response.headers.get('content-length', 0))

        # Set up the progress bar
        pb = tqdm.tqdm(
            desc=dest.name if len(dest.name) < 36 else dest.name[:33] + '...',
            total=total,
            unit='B',
            unit_scale=True,
            bar_format='{desc:72}{percentage:3.0f}%|{bar:72}{r_bar}',
            file=sys.stdout)

        with open(dest, mode='wb') as fout:
            # Download the file and process it 512 bytes at a time
            async for chunk in response.content.iter_chunked(512):
                # Write it to the output file
                logging.debug('Got another 512 bytes.')
                fout.write(chunk)
                # Update the progress bar
                logging.debug('Wrote chunk to file.')
                pb.update(len(chunk))

        pb.refresh()
        return 0, pb


def install_package(
        package: pathlib.Path,
        *,
        overwrite: bool = False,
        root_dir: pathlib.Path = pathlib.Path('/')) -> tqdm.tqdm:
    """
    Installs a downloaded package
    """

    # Make sure that the monkeys calling this function are actually passing in
    # a file as the package
    assert package.is_file(), "package is not a file"
    # And that the specified root is a directory
    assert root_dir.is_dir(), "root is not a directory"

    # Create temporary file to decompress to
    logging.debug('Creating temporary file.')
    archive = tempfile.TemporaryFile()

    # Open package file
    fp = open(package, 'rb')

    # Decompress package
    logging.info('Decompressing Zstandard-compressed archive...')
    tqdm.tqdm.write(
        'Decompressing Zstandard-compressed archive...',
        file=sys.stdout)
    pyzstd.decompress_stream(fp, archive)

    # Move the cursor back to the beginning of the file (current at the end
    # because we just wrote to it)
    logging.debug('Seeking to position 0 in decompressed archive.')
    archive.seek(0)

    # Open decompressed archive with tarfile
    logging.debug('Opening decompressed archive with tarfile.')
    tar_archive = tarfile.open(fileobj=archive)

    total_size = 0
    to_be_extracted = []

    # Figure out which files to extract by determining if they already exist
    # or if the filename starts with a '.'
    logging.info('Finding which files to extract and total size.')
    for i in tar_archive.getmembers():

        # Does not start with a '.' (valid candidate)
        if not i.name.startswith('.'):
            logging.debug(f'{i.name} does not start with .')

            # But exists in filesystem
            if os.path.exists(i.name):

                # And overwrite wasn't specified
                if not overwrite:

                    # So we don't extract it
                    logging.warning(
                        f'{i.name} already exists in the filesystem. (Won\'t extract)')

                # Otherwise
                else:

                    # Extract it
                    logging.info(
                        f'{i.name} EXISTS but will be extracted. (overwrite)')
                    to_be_extracted.append(i)
                    total_size += i.size

            # Otherwise
            else:

                # Extract it
                logging.info(f'{i.name} will be extracted.')
                to_be_extracted.append(i)
                total_size += i.size

    # Set up progress bar
    a = f'Installing {package.name}'
    pb = tqdm.tqdm(desc=a if len(a) < 36 else a[:33] + '...',
                   total=total_size,
                   unit='B',
                   unit_scale=True,
                   bar_format='{desc:72}{percentage:3.0f}%|{bar:72}{r_bar}',
                   file=sys.stdout)

    # Extract files
    for i in to_be_extracted:
        logging.info(f'Extracted {i.name}.')
        tar_archive.extract(i, root_dir, numeric_owner=True)
        pb.update(i.size)

    pb.refresh()
    pb.close()


if __name__ == '__main__':
    # NOTE: This is only for debug. It is not ready for production use.
    async def main():
        packages = [
            "linux-5.12.6.arch1-1-x86_64.pkg.tar.zst",
            "linux-headers-5.12.6.arch1-1-x86_64.pkg.tar.zst",
            "man-db-2.9.4-1-x86_64.pkg.tar.zst",
            "tar-1.34-1-x86_64.pkg.tar.zst",
            "util-linux-2.36.2-1-x86_64.pkg.tar.zst"]
        mirrors = ["https://mirror.pkgbuild.com/$repo/os/$arch"]

        session = aiohttp.ClientSession()
        sem = asyncio.Semaphore(3)

        coros = [download("core",
                          i,
                          session,
                          sem,
                          pathlib.Path('download/' + i.split('/')[-1]),
                          mirrors) for i in packages]
        print('Downloading packages...')
        returns = await asyncio.gather(*coros)
        returns, pbs = zip(*returns)
        await session.close()
        for i in pbs:
            if i is not None:
                i.close()

        tqdm.tqdm.write('')

        if 1 in returns:
            for i in range(len(returns)):
                if returns[i] == 1:
                    tqdm.tqdm.write(f"error: failed to download {packages[i]}")
            exit(1)

        for i in packages:
            print('')
            install_package(pathlib.Path('download/' + i.split('/')
                                         [-1]), overwrite=True, root_dir=pathlib.Path('install/'))
    logging.basicConfig(filename='stest.log', level=logging.DEBUG)
    asyncio.run(main())
