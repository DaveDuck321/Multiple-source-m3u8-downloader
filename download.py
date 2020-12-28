import itertools

import json
from argparse import ArgumentParser
from pathlib import Path
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def print_progress_bar(fill):
    fill_hashes = int(fill*20)
    print("\rProgress: [{:20}] {: >3}% ".format(
        '#'*fill_hashes, int(fill*100)), end='')


def parse_m3_u8(data):
    """ Returns an iterator of the segment names enumerated in 'index.m3u8' """

    # Convert file bytes into a string without newline padding
    # Ignore the comments (marked by a '#' at the start of a line)
    return (
        filter(
            lambda text: ".ts" in text,
            (
                line.decode("utf-8").rstrip()
                for line in data
                if chr(line[0]) != '#'
            )
        )
    )


def download_segment(data):
    # Don't attempt obviously invalid web requests
    if data is None:
        return (None, None)

    # Makes a web request to download the video segment located at the url
    return (data[0], urllib.request.urlopen(data[1]).read())


def download_chunks_to(names, chunks, folder: Path, progress_bar=True):
    """
        Downloads all video segments specified in the 'chunks' list.

        Concurrent chunks are downloaded in parallel on multiple
        threads, starting at the first N chunks. This allows video
        playback before the whole stream has been downloaded.

        Video chunks are saved into 'folder' without any additional
        processing. As a result, 'folder' should be chosen such that
        the downloaded chunk names do not conflict with existing files.
    """

    print(f"Downloading to folder '{folder}'")

    # Download segments concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Split into 8 wide batches to reduce writing latency
        named_chunks = zip(names, chunks)
        segments = list(itertools.zip_longest(*(named_chunks,)*8))

        for index, group in enumerate(segments):
            if progress_bar:
                print_progress_bar(index/len(segments))

            segment_data = executor.map(download_segment, group)

            for name, chunk in segment_data:
                # None values are inserted to match worker_count
                # These are stripped in download_segment() itself
                if name is None:
                    continue

                out_file = folder / name
                out_file.write_bytes(chunk)

    if progress_bar:
        print_progress_bar(1.0)
        print("\nSuccess!")


def download_M3U_stream(stream_url, out_folder: Path):
    """
        Download 'index.m3u8' to identify all stream lengths and
        segment names. The original 'index.m3u8" is also saved to
        'out_folder' to allow video playback.

        This is done synchronously.
    """

    print(f"Downloading {stream_url}")

    # Generate output folder
    out_folder.mkdir(parents=True, exist_ok=False)

    # Discover segment names
    index_content = urllib.request.urlopen(stream_url)
    index_content = index_content.readlines()

    with open(out_folder / 'index.m3u8', 'wb+') as file:
        file.writelines(index_content)

    # Parent folder for relative path navigation
    stream_base_url = stream_url.split('index.m3u8')[0]

    # Adds all segments to the download queue
    # list: Progress bar requires knowledge of stream length
    segment_names = list(parse_m3_u8(index_content))
    segments_to_stream = map(  # Convert to absolute urls
        lambda seg_name: stream_base_url + seg_name,
        segment_names
    )

    # Downloads the actual video chunks to disk
    download_chunks_to(segment_names, segments_to_stream, out_folder)


def download_M3U_streams(streams, out_folder: Path):
    """
        Download a complete multipart stream including all sources.
        The complete base url of all streams are required.
    """
    for index, stream_url in enumerate(streams):
        print(f"\nDownloading stream {index}")
        download_M3U_stream(stream_url, out_folder / str(index))


def index_url_from_master(master_url: str, out_folder: Path):
    """
        Download the 'master.m3u8' file in order to find the url
        of the highest resolution index.m3u8 file available.

        Return the URL of this index file.
    """
    master_content = urllib.request.urlopen(master_url)
    master_content = master_content.readlines()

    # Log full contents to file for debugging
    master_file = (out_folder / 'masters.txt').open('ab+')
    master_file.write(b'<------- START OF MASTER ------->\n')
    master_file.writelines(master_content)
    master_file.write(b'<-------- END OF MASTER -------->\n')
    master_file.close()

    base_url = master_url.split('master.m3u8')[0]

    for line in master_content:
        # Ignore comments
        if chr(line[0]) == '#':
            continue
        return base_url + line[:-1].decode("utf-8")

    raise IOError("Bad master.m3u8 received")


def parse_DeliveryInfo(delivery_info, out_folder: Path):
    # Generate output location
    out_folder.mkdir(parents=True, exist_ok=False)

    # Get all relevant info
    json_info = json.loads(delivery_info)["Delivery"]

    # Save all relevant delivery info to allow synchronisation later
    with open(out_folder / 'DeliveryInfo.json', 'w+') as info_file:
        json.dump(json_info, info_file)

    streams = json_info["Streams"]

    print("Finding streams")
    masters = (stream["StreamUrl"] for stream in streams)
    index_urls = list(index_url_from_master(master, out_folder)
                      for master in masters)

    print(f"Downloading {len(index_urls)} streams")
    download_M3U_streams(index_urls, out_folder)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Downloads a multipart .m3u8 stream")
    parser.add_argument(
        'file_in',
        help="The file containing the DeliveryInfo object")
    parser.add_argument(
        'dir_out',
        help='The destination directory for the downloaded video stream')

    arguments = parser.parse_args()

    in_file = Path(arguments.file_in)
    out_folder = Path(arguments.dir_out)

    parse_DeliveryInfo(in_file.read_text(), out_folder)
