import itertools

import urllib.request
from concurrent.futures import ThreadPoolExecutor


def print_progress_bar(fill):
    fill_hashes = int(fill*20)
    print("\rProgress: [{:10}] {: >3}% ".format('#'*fill_hashes, int(fill*100)), end='')


def parse_m3_u8(data):
    # Returns an iterator of segment names as specified in the 'index.m3u8' file.

    content = data.readlines()
    return map(
        # Converts bytes into string without newline padding
        lambda line_data: line_data.decode("utf-8").rstrip(),
        filter(
            # Comments always appear at the start of a line, ignore these lines
            lambda line: chr(line[0]) != '#',
            content
        )
    )


def download_segment(url):
    # Makes a web request to download the video segment located at the url
    return urllib.request.urlopen(url).read()


def download_chunks_to(chunks, file_name, progress_bar=True):
    # Downloads all segments in 'chunks'
    # Concatenates results into a single file

    print(f"Downloading to file '{file_name}'")
    file = open(file_name, 'wb+')

    # Download segments concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Split into 8 wide batches to reduce writing latency
        segments = list(itertools.zip_longest(*(chunks,)*8))
        for index, group in enumerate(segments):
            if progress_bar:
                print_progress_bar(index/len(segments))

            segment_data = executor.map(download_segment, group)

            for chunk in segment_data:
                file.write(chunk)

    if progress_bar:
        print_progress_bar(1.0)
        print("\nSuccess!")


def download_panopto_stream(stream_url):
    # Download the 'index.m3u8' files to identify stream lengths and segment names
    # This is done synchronously
    index_content = urllib.request.urlopen(stream_url + 'index.m3u8')

    # Adds all segments to the download queue
    # Progress bar requires knowledge of stream length, use list
    segments_to_stream = list(map(  # Convert to absolute urls
        lambda seg_name: stream_url + seg_name,
        parse_m3_u8(index_content)
    ))

    # Downloads the actual video to disk, concatinating to
    download_chunks_to(segments_to_stream, '0.ts')


if __name__ == "__main__":
    download_panopto_stream("")
