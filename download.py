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


def download_segment(data):
    # Makes a web request to download the video segment located at the url
    return (data[0], urllib.request.urlopen(data[1]).read())


def download_chunks_to(names, chunks, folder, progress_bar=True):
    # Downloads all segments in 'chunks'
    # Concatenates results into a single file

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
                with open(folder+name, 'wb+') as file:
                    file.write(chunk)

    if progress_bar:
        print_progress_bar(1.0)
        print("\nSuccess!")


def download_panopto_stream(stream_url, out_folder):
    # Download the 'index.m3u8' files to identify stream lengths and segment names
    # This is done synchronously
    index_content = urllib.request.urlopen(stream_url + 'index.m3u8')
    with open(out_folder + 'index.m3u8', 'w+') as file:
        file.write(index_content)

    # Adds all segments to the download queue
    # Progress bar requires knowledge of stream length, use list
    segment_names = list(parse_m3_u8(index_content))
    segments_to_stream = list(map(  # Convert to absolute urls
        lambda seg_name: stream_url + seg_name,
        segment_names
    ))

    # Downloads the actual video to disk, concatinating to
    download_chunks_to(segment_names, segments_to_stream, 'out/')


if __name__ == "__main__":
    download_panopto_stream("", "out")
