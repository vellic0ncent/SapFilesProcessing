import io
import re
import os
from multiprocessing import Pool
from statistics import median
from collections import defaultdict
from typing import NoReturn
from re import Pattern
import sys

pattern_no_whitespaces = r'^\s+|\s+$'
pattern_totals = r'^\| *\* *|^\| *Subtotals: *'  # r'^\| *\* *'
pattern_double_rows_sample = r'^([^\|]+\|)+[^\|]*$'
sep = "|"


def process_oneliner(col_reference: dict, pattern_sample: Pattern[str], line: str, processed_line: str,
                     last: int, start: int, length_bug: bool = False) -> str:

    for col, expected_length in col_reference.items():
        end = start + expected_length
        # Case if length of column is as expected and element at "end" idx is sep | as expected.
        if line[end] == "|":
            _ = re.sub(pattern=pattern_sample, repl='', string=line[start: end])
            if col != last:
                processed_line += f'{_}{sep}'
            else:
                processed_line += f'{_}'
            start = end + 1
        else:
            length_bug = True
            break

    if length_bug:
        for col, line_part in enumerate(line.split("|")):
            _ = re.sub(pattern=pattern_sample, repl='', string=line_part)
            if col != last:
                processed_line += f'{_}{sep}'
            else:
                processed_line += f'{_}'

    return processed_line


def process_twoliner(col_reference: dict, pattern_sample: Pattern[str], line: str, processed_line: str,
                     start: int, length_bug: bool = False) -> str:
    for col, expected_length in col_reference.items():
        end = start + expected_length
        if line[end] == "|":
            _ = re.sub(pattern=pattern_sample, repl='', string=line[start: end])
            processed_line += f'{_}{sep}'
            start = end + 1
        else:
            length_bug = True
            break

    if length_bug:
        for line_part in line.split("|"):
            _ = re.sub(pattern=pattern_sample, repl='', string=line_part)
            processed_line += f'{_}{sep}'

    return processed_line


def process_large_text_file(in_filepath: str, out_filepath: str,
                            pattern_sample: Pattern[str] = pattern_no_whitespaces,
                            pattern_double_rows: Pattern[str] = pattern_double_rows_sample) -> NoReturn:
    # Firstly - recognizing structure block
    with io.open(in_filepath, "r", encoding="utf-8") as r:

        col_length = defaultdict(list[int])
        _extra_col_length = defaultdict(list[int])
        _is_select_option_sample = False  # File that has inside block "---Select Option---"
        _is_inside_select_block = False
        _end_of_select_block = 0  # Row index when "---Select Option---" block ends
        _is_double_row_sample = False  # File that has inside data split by two rows

        for _i, line in enumerate(r):
            line: str
            _i: int
            # Check first n lines (includes headers) to recognize file structure
            if _i <= 20:
                try:
                    # If in file "---Select Option---" block was not identified OR
                    # row not already inside in "---Select Option---" block
                    if not _is_select_option_sample or not _is_inside_select_block:
                        # Check if file has "---Select Option---" block
                        if "--Select options --" in line:
                            _is_select_option_sample, _is_inside_select_block = True, True
                        # Case when file has one-row structure
                        elif not re.match(pattern_double_rows, line):
                            line_parts = line.split('|')[1:-1]  # without \n & first ''
                            if len(line_parts) > 0:
                                for idx, col in enumerate(line_parts):
                                    col_length[idx].append(len(col))
                            _i += 1
                        # Case when file has two-rows structure
                        else:
                            _is_double_row_sample = True  # Change the flag to True, file has data split by two rows
                            line_parts = line[:-2].split('|')  # without \n
                            if len(line_parts) > 0:
                                for idx, col in enumerate(line_parts):
                                    _extra_col_length[idx].append(len(col))
                            _i += 1
                    #  If file has "---Select Option---" block (_is_select_option_sample == True) OR
                    #  row already inside in "---Select Option---" block
                    else:
                        if '------' not in line:
                            pass
                        else:
                            _end_of_select_block = _i
                            _is_inside_select_block = False
                # If we are out of rows - break cycle and move to parsing block
                except IndexError:
                    break
            else:
                break

        col_length_reference = {k: int(median(v)) for k, v in zip(col_length.keys(), col_length.values())}
        if _extra_col_length:
            _extra_col_length_reference = {k: int(median(v)) for k, v in zip(_extra_col_length.keys(),
                                                                             _extra_col_length.values())}

    # Secondly - parsing block
    with io.open(in_filepath, "rb") as r, io.open(out_filepath, "w", encoding="utf-8") as w:
        _counter = 0

        # Case if file has one-row structure
        if not _is_double_row_sample:
            _last = list(col_length_reference.keys())[-1]

            for line in r:
                try:
                    line = line.decode("utf-8")
                except UnicodeDecodeError:
                    line = line.decode("latin-1").encode("utf-8").decode("utf-8")

                if _is_select_option_sample and _counter <= _end_of_select_block:
                    _counter += 1
                    pass
                else:
                    _counter = 1000
                    processed_line = ''
                    if not re.match(pattern_totals, line):
                        line_parts = line.split('|')[1:-1]
                        if len(line_parts) > 0:
                            processed_line = process_oneliner(col_reference=col_length_reference,
                                                              pattern_sample=pattern_sample, line=line, last=_last,
                                                              processed_line=processed_line, start=1)
                            w.write(processed_line + '\n')

        # Case if file has two-rows structure
        else:
            _last = list(_extra_col_length_reference.keys())[-1]
            _start_line_found = None

            for line in r:
                try:
                    line = line.decode("utf-8")
                except UnicodeDecodeError:
                    line = line.decode("latin-1").encode("utf-8").decode("utf-8")

                if _is_select_option_sample and _counter <= _end_of_select_block:
                    _counter += 1
                    pass
                else:
                    _counter = 1000
                    processed_line = ''
                    # Case when we identify first line of two lines
                    if not re.match(pattern_totals, line) and not _start_line_found:
                        line_parts = line.split('|')[1:-1]
                        if len(line_parts) > 0:
                            _start_line_found = process_twoliner(col_reference=col_length_reference,
                                                                 pattern_sample=pattern_sample, line=line,
                                                                 processed_line=processed_line, start=1)
                    # Case when we identify second line of two lines, concatenate it with first and write to file
                    elif _start_line_found:
                        processed_line = process_oneliner(col_reference=_extra_col_length_reference,
                                                          pattern_sample=pattern_sample, line=line, last=_last,
                                                          processed_line=processed_line, start=0)

                        w.write(_start_line_found + processed_line + '\n')
                        _start_line_found = None


def _arguments(raw_files_path: str, output_files_path: str):
    raw_files = [f for f in os.listdir(raw_files_path) if os.path.isfile(os.path.join(raw_files_path, f))]
    args = []
    for raw_file in raw_files:
        args.append(
            (os.path.join(raw_files_path, raw_file),
             os.path.join(output_files_path, raw_file.split(sep=".txt")[0] + '_correct.txt'))
        )
    return args


if __name__ == '__main__':

    path = sys.argv[1]
    args = _arguments(path, path)

    with Pool() as p:
        p.starmap(
            process_large_text_file, args
        )
