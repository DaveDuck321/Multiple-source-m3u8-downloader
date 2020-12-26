# Multiple Source m3u8 Downloader
Download m3u8 internet streams consisting of multiple sources.

## Dependencies

#### `download.py`
- Python 3.8+

#### `merge.py`
- Python 3.8+
- An instance ffmpeg on the path

## Usage

### To download the stream bundle
```
download.py [-h] file_in dir_out
```

Where `file_in` is the file containing the DeliveryInfo object, and `dir_out` is the destination directory for the downloaded video stream.

This creates the directory `dir_out` and writes all video chunks to individual files in that directory.

### To merge the stream chunks
```
merge.py [-h] [-o OUTPUT] input
```

Where `input` is the directory containing multiple *.ts video streams (as specified by for `dir_out` by `download.py`), and `OUTPUT` is the destination directory for the merged .mp4 videos.
