import re
import subprocess
from argparse import ArgumentParser
from pathlib import Path


def merge_stream(in_path: Path, out_file: Path):
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
        out_file,
    ])


def merge_multipart(in_path: Path, out_path: Path):
    # Ensure directory exists
    out_path.mkdir(exist_ok=False, parents=True)

    for part in in_path.glob('*'):
        # Skip individual files, only attempt merge on directories
        if not part.is_dir():
            continue

        # Merge files
        out_file = out_path / (part.name + '.ts')
        merge_stream(part, out_file)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Merges a stream of *.ts video segments")
    parser.add_argument(
        '-o', dest='output', default="./out/",
        help='The destination directory for the merged video')
    parser.add_argument(
        'input',
        help="The directory containing multiple *.ts video streams")

    arguments = parser.parse_args()

    merge_multipart(Path(arguments.input), Path(arguments.output))
