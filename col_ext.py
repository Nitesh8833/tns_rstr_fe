#!/usr/bin/env python3
"""
extract_columns_by_keywords.py (with defaults + interactive)

This version supports three ways to provide paths:
 1. Command-line arguments (preferred when running from terminal)
 2. Hardcoded defaults in the script (modify DEFAULT_ROOT_FOLDER / DEFAULT_OUTPUT_FILE)
 3. Interactive prompt (if neither CLI nor defaults are provided)

Examples:
  - CLI (explicit): python extract_columns_by_keywords.py "C:\\data\\excels" "C:\\data\\out.xlsx"
  - Hardcoded: set DEFAULT_ROOT_FOLDER and DEFAULT_OUTPUT_FILE below and run: python extract_columns_by_keywords.py
  - Interactive: run without args and without defaults; the script will ask you to type paths.
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Dict, List, Tuple
import argparse
import logging

# ----- User-configurable mapping -----
KEYWORD_MAP: Dict[str, str] = {
    "language": "language",
    "lang": "language",
    "mother tongue": "language",
    "spoken": "language",
    "degree": "degree",
    "qualification": "degree",
}

EXCEL_EXTS = {".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods"}

# ----- Defaults (edit these if you want to hardcode paths) -----
# If you set these to non-empty strings, the script will use them when CLI args are not provided.
# Use raw strings (r"C:\\path\\to\\folder") or double backslashes in Windows paths.
DEFAULT_ROOT_FOLDER = ""  # e.g. r"C:\\Users\\YourName\\Documents\\excel_folder"
DEFAULT_OUTPUT_FILE = ""  # e.g. r"C:\\Users\\YourName\\Documents\\output_matches.xlsx"

# ----- Functions -----

def find_excel_files(root: Path) -> List[Path]:
    files = [p for p in root.rglob("*") if p.suffix.lower() in EXCEL_EXTS and p.is_file()]
    return files


def match_header(header: str, keyword_map: Dict[str, str]) -> Tuple[str, str]:
    if header is None:
        return "", ""
    h = str(header).lower()
    for kw, std in keyword_map.items():
        if kw.lower() in h:
            return kw, std
    return "", ""


def process_workbook(path: Path, keyword_map: Dict[str, str]) -> List[Dict]:
    rows = []
    try:
        xls = pd.read_excel(path, sheet_name=None, nrows=0)
    except Exception as e:
        logging.warning(f"Failed to read {path}: {e}")
        return rows

    for sheet_name, df in xls.items():
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

    try:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="matches", index=False)
        logging.info(f"Wrote output to {output_file}")
    except Exception as e:
        logging.error(f"Failed to write output excel {output_file}: {e}")


def resolve_paths(cli_root: str | None, cli_out: str | None) -> Tuple[Path, Path]:
    # 1. CLI args
    if cli_root and cli_out:
        return Path(cli_root), Path(cli_out)

    # 2. Defaults
    if DEFAULT_ROOT_FOLDER and DEFAULT_OUTPUT_FILE:
        return Path(DEFAULT_ROOT_FOLDER), Path(DEFAULT_OUTPUT_FILE)

    # 3. Partial CLI
    if cli_root and not cli_out:
        out = input("Output file path (where to write results, e.g. C:\\out.xlsx): ").strip()
        return Path(cli_root), Path(out)
    if cli_out and not cli_root:
        root = input("Root folder to scan for excel files: ").strip()
        return Path(root), Path(cli_out)

    # 4. Interactive prompt
    root = input("Enter root folder to recursively scan for Excel files: ").strip()
    out = input("Enter output Excel file path (will be overwritten if exists): ").strip()
    return Path(root), Path(out)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Extract columns matching keywords from many Excel files")
    parser.add_argument("root_folder", nargs="?", help="Root folder to recursively scan for excel files")
    parser.add_argument("output_file", nargs="?", help="Output Excel file path to write matches")
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

    root_path, out_path = resolve_paths(args.root_folder, args.output_file)

    if not root_path.exists():
        logging.error(f"Root folder does not exist: {root_path}")
        sys.exit(2)

    if not out_path.parent.exists():
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            logging.info(f"Created output directory: {out_path.parent}")
        except Exception as e:
            logging.error(f"Failed to create output directory {out_path.parent}: {e}")
            sys.exit(3)

    run(root_path, out_path, km)


if __name__ == "__main__":
    main()
