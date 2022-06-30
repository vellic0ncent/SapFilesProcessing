import io
import re
import os
from multiprocessing import Pool
from statistics import median
from collections import defaultdict
from typing import NoReturn
from re import Pattern


pattern_no_whitespaces = r'^\s+|\s+$'


def process_large_text_file(in_filepath: str, out_filepath: str,
                            pattern_sample: Pattern[str] = pattern_no_whitespaces) -> NoReturn:
    with io.open(in_filepath, "r", encoding="utf-8") as r:

        col_length = defaultdict(list[int])
        _is_bug_sample = False

        for _i, line in enumerate(r):
            if _i <= 15:
                line_parts = line.split('|')[1:-1]  # without \n & first ''
                if len(line_parts) > 0:
                    for idx, col in enumerate(line_parts):
                        col_length[idx].append(len(col))
            _i += 1

        col_length_reference = {k: median(v) for k, v in zip(col_length.keys(), col_length.values())}

    with io.open(in_filepath, "r", encoding="utf-8") as r, io.open(out_filepath, "w", encoding="utf-8") as w:
        _last = list(col_length_reference.keys())[-1]
        for line in r:
            processed_line = ''
            line_parts = line.split('|')[1:-1]
            if len(line_parts) > 0:
                _start, _end = 1, 1
                for col, expected_length in col_length_reference.items():
                    _end = _start + expected_length
                    _ = re.sub(pattern=pattern_sample, repl='', string=line[_start: _end])
                    if col != _last:
                        processed_line += f'{_};'
                    else:
                        processed_line += f'{_}'

                    _start = _end + 1
                w.write(processed_line + '\n')


def _arguments(raw_files_path: str, output_files_path: str):
    raw_files = os.listdir(path=raw_files_path)
    args = []
    for raw_file in raw_files:
        args.append(
            (os.path.join(raw_files_path, raw_file),
             os.path.join(output_files_path, raw_file.split(sep=".txt")[0] + '_correct.txt'))
        )
    return args


if __name__ == '__main__':
    args = _arguments(r'C:\Users\vpushkareva\PycharmProjects\ETL-SapFilesPreprocessing\artifacts',
                      r'C:\Users\vpushkareva\PycharmProjects\ETL-SapFilesPreprocessing\res')

    with Pool() as p:
        p.starmap(
            process_large_text_file, args
        )
