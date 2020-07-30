import subprocess
import re
from pathlib import Path


def merge_stream(in_path: Path, out_path: Path):
    # Generate a list of stream parts
    index_path = in_path / 'index.m3u8'
    merge_path = in_path / 'files.txt'

    with index_path.open('r') as index_file, \
            merge_path.open('w+') as files_path:
        for line in index_file:
            if match := re.search('[0-9]+\\.ts', line):
                target = in_path / match.group()
                files_path.write(f"file '{target.as_posix()}'\n")

    # Use ffmpeg to combine stream
    subprocess.call([
        'ffmpeg',
        '-f', 'concat',
        '-i', merge_path,
        '-c', 'copy',
        out_path / 'out.ts',
    ])


def merge_multipart(in_path: Path, out_path: Path):
    for part in in_path.glob('*/'):
        # Ensure output folder exists
        our_dir = out_path / part.name
        our_dir.mkdir(parents=True, exist_ok=True)

        # Merge files
        merge_stream(part, our_dir)


if __name__ == "__main__":
    merge_multipart(Path(input("path> ")), Path('out'))
