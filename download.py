import itertools

import urllib.request
from concurrent.futures import ThreadPoolExecutor


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


def download_segment(file_index, url):
    # Makes a web request to download the video segment located at the url
    print(url)
    return (file_index, urllib.request.urlopen(url).read())


def download_panopto_streams(stream_addresses):
    # Download the 'index.m3u8' files to identify stream lengths and segment names
    # This is done synchronously
    segments_to_stream = iter([])
    for file_index, stream_url in enumerate(stream_addresses):
        index_content = urllib.request.urlopen(stream_url + 'index.m3u8')

        # Adds all segments to the download queue
        segments_to_stream = itertools.chain(
            segments_to_stream,
            map(  # Convert to absolute urls with identification index
                lambda seg_name: (file_index, stream_url + seg_name),
                parse_m3_u8(index_content)
            )
        )

    # Concurrently downloads all video segments in stream
    with ThreadPoolExecutor(max_workers=8) as executor:
        current_index = -1
        seg_promises = [1]
        while len(seg_promises) != 0:
            # Starts all downloads at once, the ThreadPool will rate limit
            seg_promises = []
            for file_index, url in itertools.islice(segments_to_stream, 0, 8):
                seg_promises.append(executor.submit(download_segment, file_index, url))


            # Save these promises to the correct video files
            for segment_index, segment in enumerate(seg_promises):
                print(f"Downloading file {current_index} segment {segment_index}")

                # Ensures the file is fully downloaded
                (file_index, seg_data) = segment.result()

                # Stream to the correct file
                if file_index != current_index:
                    current_file = open(f'{file_index}.ts', 'wb+')
                    current_index = file_index

                current_file.write(seg_data)


if __name__ == "__main__":
    download_panopto_streams([""])
