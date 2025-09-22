#!/usr/bin/env python3
"""
extract_columns_by_keywords.py

Recursively scan a folder for Excel files, read every sheet, find column names
that contain any of a set of keywords (case-insensitive substring match) and
write a consolidated output Excel listing the matches.

Output columns (sheet 'matches'):
 - file_path: full path to the workbook
 - sheet_name: sheet name inside the workbook
 - column_name: the original column/header found in that sheet
 - matched_keyword: which keyword was matched (first match)
 - standard_column: the target output column name you assign for that keyword

Usage examples:
    python extract_columns_by_keywords.py /path/to/search output_matches.xlsx

You can edit KEYWORD_MAP to change which keywords map to which output column.
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Dict, List, Tuple
import argparse
import logging

# ----- User-configurable mapping -----
# Map a keyword (substring) to a standardized output column name.
# Matching is case-insensitive and checks if the keyword appears anywhere in the header.
# Add as many entries as you need. Order matters: first matching keyword is used.
KEYWORD_MAP: Dict[str, str] = {
    "language": "language",
    "lang": "language",
    "mother tongue": "language",
    "spoken": "language",
    "degree": "degree",
    "qualification": "degree",
    # add more as required
}

EXCEL_EXTS = {".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods"}

# ----- Functions -----

def find_excel_files(root: Path) -> List[Path]:
    """Recursively yield excel files under root."""
    files = [p for p in root.rglob("*") if p.suffix.lower() in EXCEL_EXTS and p.is_file()]
    return files


def match_header(header: str, keyword_map: Dict[str, str]) -> Tuple[str, str]:
    """Return (matched_keyword, standard_column) for the first keyword that matches the header.
    If no match, return ("", "")."""
    if header is None:
        return "", ""
    h = str(header).lower()
    for kw, std in keyword_map.items():
        if kw.lower() in h:
            return kw, std
    return "", ""


def process_workbook(path: Path, keyword_map: Dict[str, str]) -> List[Dict]:
    """Read all sheets and collect matched column headers.
    Returns a list of dicts suitable to create a DataFrame for output."""
    rows = []
    try:
        # sheet_name=None loads all sheets into dict of DataFrames
        xls = pd.read_excel(path, sheet_name=None, nrows=0)  # nrows=0 to only get headers fast
    except Exception as e:
        logging.warning(f"Failed to read {path}: {e}")
        return rows

    for sheet_name, df in xls.items():
        # If nrows=0, pandas returns an empty DataFrame with columns
        headers = list(df.columns)
        for col in headers:
            kw, std = match_header(col, keyword_map)
            if kw:
                rows.append({
                    "file_path": str(path),
                    "sheet_name": sheet_name,
                    "column_name": col,
                    "matched_keyword": kw,
                    "standard_column": std,
                })
    return rows


def run(root_folder: Path, output_file: Path, keyword_map: Dict[str, str]):
    files = find_excel_files(root_folder)
    logging.info(f"Found {len(files)} excel files under {root_folder}")

    all_rows = []
    for f in files:
        rows = process_workbook(f, keyword_map)
        if rows:
            all_rows.extend(rows)

    if not all_rows:
        logging.info("No matching headers found. Creating an empty output file with headers.")
        df_out = pd.DataFrame(columns=["file_path", "sheet_name", "column_name", "matched_keyword", "standard_column"]) 
    else:
        df_out = pd.DataFrame(all_rows)

    # Optionally pivot or group later; for now just write matches.
    try:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="matches", index=False)
        logging.info(f"Wrote output to {output_file}")
    except Exception as e:
        logging.error(f"Failed to write output excel {output_file}: {e}")


# ----- CLI -----

def main(argv=None):
    parser = argparse.ArgumentParser(description="Extract columns matching keywords from many Excel files")
    parser.add_argument("root_folder", help="Root folder to recursively scan for excel files")
    parser.add_argument("output_file", help="Output Excel file path to write matches")
    parser.add_argument("--add-keyword", "-k", action="append", nargs=2, metavar=("KEY","STD"),
                        help="Add a keyword and its standard column name (can be used multiple times). Example: -k language language")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO if not args.verbose else logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(message)s")

    km = KEYWORD_MAP.copy()
    if args.add_keyword:
        for k, s in args.add_keyword:
            km[k] = s

    root = Path(args.root_folder)
    if not root.exists():
        logging.error(f"Root folder does not exist: {root}")
        sys.exit(2)

    out = Path(args.output_file)
    run(root, out, km)


if __name__ == "__main__":
    main()
