#!/usr/bin/env python3
"""
collect_degrees_per_sheet.py

Scan a folder of Excel workbooks and extract unique degree values sheet-by-sheet.

Behavior changes from previous script:
 - Treat each sheet independently (no cross-sheet deduplication).
 - For each sheet, produce one row per distinct degree value found in that sheet.
 - Count how many times that degree value appears in the sheet (across all matched columns).
 - Output all sheet results one after another in a single output sheet.
 - Log processing (file & sheet) to console and to a log file.

Output columns (one row per sheet-degree):
 - degree          : degree value (original casing, first seen)
 - count           : how many occurrences of this degree in the sheet (sum across matched columns)
 - filename        : source filename
 - sheet           : sheet name
 - columns         : comma-separated column headers (in that sheet) where the degree was found
 - sources         : semicolon-separated filename|sheet|column entries (helps trace where occurrences were)
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
import collections
from datetime import datetime

import pandas as pd

# ------------------------------
# CONFIG: candidate substrings for degree columns (case-insensitive)
DEGREE_COLUMN_CANDIDATES = [
    "degree",
    "provider degree",
    "degree1",
    "degree2",
    "deg",
    "provider_degree",
    "providerdegree",
    "degree type",
    "provider degree type",
    # add more variants you encounter...
]
# ------------------------------

# ------------------------------
# CONFIG: exclude header substrings (case-insensitive)
EXCLUDE_COLUMN_SUBSTRINGS = [
    "practice",
    "notes",
    "address",
    # add exclude patterns as needed
]
# ------------------------------

SCAN_ROWS_DEFAULT = 50  # how many top rows to scan to find header row for each sheet


# ---------- utilities ----------
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    return " ".join(str(s).split()).strip()


def _is_valid_header_cell(x: object) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip()
    if not s:
        return False
    if s.lower().startswith("unnamed"):
        return False
    if len(s) > 200:
        return False
    return True


def find_header_row_by_keywords(sample_df: pd.DataFrame, keywords: List[str]) -> Optional[int]:
    """
    Simple header detection: return 0-based row index inside sample_df or None.
    """
    if sample_df is None or sample_df.empty:
        return None

    candidates = []
    for i in range(len(sample_df)):
        row = sample_df.iloc[i]
        vals = [str(v) if pd.notna(v) else "" for v in row]
        valid = sum(1 for v in vals if _is_valid_header_cell(v))
        keyword_hits = sum(1 for v in vals for k in keywords if k in v.casefold())
        nonempty = sum(1 for v in vals if str(v).strip())
        candidates.append((i, keyword_hits, valid, nonempty))

    kw_rows = [c for c in candidates if c[1] > 0]
    if kw_rows:
        kw_rows.sort(key=lambda x: (-x[2], x[0]))
        return kw_rows[0][0]

    candidates.sort(key=lambda x: (-x[2], -x[3], x[0]))
    return candidates[0][0] if candidates else None


def get_headers_for_sheet(file_path: Path, sheet_name: str, scan_rows: int, keywords: List[str]) -> Tuple[List[str], Optional[int]]:
    """
    Return (headers_list, header_row_index) for the sheet (header_row_index is 0-based).
    """
    try:
        sample = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=scan_rows, engine="openpyxl")
    except Exception:
        return [], None

    hdr_idx_rel = find_header_row_by_keywords(sample, keywords)
    if hdr_idx_rel is None:
        try:
            df0 = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0, engine="openpyxl")
            return [str(c) for c in df0.columns.tolist()], 0
        except Exception:
            return [], None

    hdr_idx = hdr_idx_rel
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=hdr_idx, engine="openpyxl")
    except Exception:
        try:
            df0 = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0, engine="openpyxl")
            return [str(c) for c in df0.columns.tolist()], 0
        except Exception:
            return [], None

    headers = []
    for c in df.columns.tolist():
        if isinstance(c, tuple):
            parts = [str(p).strip() for p in c if p is not None and str(p).strip() != ""]
            headers.append(" ".join(parts))
        else:
            headers.append(str(c))
    return headers, hdr_idx


# ---------- processing per-sheet ----------
def collect_per_sheet(
    folder: Path,
    candidates: List[str],
    scan_rows: int = SCAN_ROWS_DEFAULT,
    recursive: bool = False,
    exclude_list: Optional[List[str]] = None,
    verbose: bool = False,
    log_file: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Scan files and return a DataFrame with per-sheet distinct degree rows.
    Rows preserve processing order: files sorted, sheets in file order.
    """
    # prepare logger
    logger = logging.getLogger("collect_degrees")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    if log_file:
        fh = logging.FileHandler(str(log_file), mode="w", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    if verbose:
        logger.setLevel(logging.INFO)

    cand_norm = [c.casefold().strip() for c in candidates if c and c.strip()]
    exclude_norm = [e.casefold().strip() for e in (exclude_list or []) if e and e.strip()]

    patterns = ("*.xlsx", "*.xlsm")
    files = []
    for pat in patterns:
        files.extend(folder.rglob(pat) if recursive else folder.glob(pat))
    files = sorted(files)

    out_rows = []

    for f in files:
        logger.info(f"Processing file: {f.name}")
        try:
            xls = pd.ExcelFile(f, engine="openpyxl")
            sheets = xls.sheet_names
        except Exception as ex:
            logger.warning(f"  Skipping unreadable file: {f.name}  ({ex})")
            continue

        for sheet in sheets:
            logger.info(f"  Processing sheet: {sheet}")
            headers, hdr_idx = get_headers_for_sheet(f, sheet, scan_rows, cand_norm)
            if not headers:
                logger.info(f"    No headers detected (skipping sheet).")
                continue

            # find candidate columns in this sheet, excluding by substring
            matched_cols = []
            for col in headers:
                col_norm = str(col).casefold()
                if any(exc in col_norm for exc in exclude_norm):
                    logger.debug(f"    Excluding header '{col}' (matches exclude list).")
                    continue
                if any(k in col_norm for k in cand_norm):
                    matched_cols.append(col)

            if not matched_cols:
                logger.info(f"    No degree-like columns found in this sheet.")
                continue

            # read sheet with detected header (or fallback)
            header_arg = hdr_idx if hdr_idx is not None else 0
            try:
                df_sheet = pd.read_excel(f, sheet_name=sheet, header=header_arg, engine="openpyxl", dtype=str)
            except Exception as ex:
                logger.warning(f"    Could not read sheet with header={header_arg} ({ex}), trying header=0")
                try:
                    df_sheet = pd.read_excel(f, sheet_name=sheet, header=0, engine="openpyxl", dtype=str)
                except Exception as ex2:
                    logger.warning(f"    Failed to read sheet {sheet}: {ex2}")
                    continue

            # Build per-sheet mapping: normalized_degree -> metadata (count, columns set, sources set, display)
            per_sheet: Dict[str, Dict[str, object]] = {}
            # We'll count occurrences: if degree value appears multiple times across rows/columns, count them.
            for col in matched_cols:
                # map col to actual column in df_sheet (case-insensitive match or substring)
                found_col_name = None
                for actual_col in df_sheet.columns:
                    if any(exc in str(actual_col).casefold() for exc in exclude_norm):
                        continue
                    if str(col).casefold() == str(actual_col).casefold():
                        found_col_name = actual_col
                        break
                if found_col_name is None:
                    for actual_col in df_sheet.columns:
                        if any(exc in str(actual_col).casefold() for exc in exclude_norm):
                            continue
                        if any(k in str(actual_col).casefold() for k in cand_norm) and str(col).casefold() in str(actual_col).casefold():
                            found_col_name = actual_col
                            break
                if found_col_name is None:
                    # fallback but skip if excluded
                    if any(exc in str(col).casefold() for exc in exclude_norm):
                        continue
                    found_col_name = col

                # extract column values (as strings)
                try:
                    series = df_sheet[found_col_name].astype(str).fillna("").apply(lambda x: normalize_text(x))
                except Exception as ex:
                    logger.debug(f"    Could not read column '{found_col_name}' in {f.name}|{sheet}: {ex}")
                    continue

                # count occurrences per value in this column
                # NOTE: treat empty and "nan" as empty
                values = [v for v in series if v != "" and v.lower() != "nan"]
                counts = collections.Counter(values)
                logger.debug(f"    Column '{found_col_name}' produced {len(counts)} unique non-empty values")

                for val, cnt in counts.items():
                    key = val.casefold()
                    if key not in per_sheet:
                        per_sheet[key] = {
                            "display": val,
                            "count": 0,
                            "columns": set(),
                            "sources": set(),
                        }
                    # increment count by occurrences in this column
                    per_sheet[key]["count"] += cnt
                    per_sheet[key]["columns"].add(str(found_col_name))
                    per_sheet[key]["sources"].add(f"{f.name}|{sheet}|{found_col_name}")

            # After processing all matched columns in this sheet, produce rows (distinct per-sheet)
            if not per_sheet:
                logger.info(f"    No non-empty degree values found in sheet.")
                continue

            # Order results deterministically (by display lower)
            for key, meta in sorted(per_sheet.items(), key=lambda x: x[0]):
                out_rows.append({
                    "degree": meta["display"],
                    "count": meta["count"],
                    "filename": f.name,
                    "sheet": sheet,
                    "columns": ", ".join(sorted(meta["columns"])),
                    "sources": "; ".join(sorted(meta["sources"])),
                })
            logger.info(f"    Found {len(per_sheet)} distinct degree values in sheet.")

    # build DataFrame preserving order of out_rows
    df_out = pd.DataFrame(out_rows, columns=["degree", "count", "filename", "sheet", "columns", "sources"])
    return df_out


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Collect distinct degree values per sheet across Excel files.")
    parser.add_argument("-i", "--input-folder", required=True, help="Folder containing Excel files to scan.")
    parser.add_argument("-o", "--output", default="degrees_per_sheet.xlsx", help="Output Excel file path.")
    parser.add_argument("--scan-rows", type=int, default=SCAN_ROWS_DEFAULT, help="Top N rows to scan for header detection.")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan subfolders.")
    parser.add_argument("--candidates", type=str, default=None, help="Comma-separated candidate header substrings (overrides defaults).")
    parser.add_argument("--exclude", type=str, default=None, help="Comma-separated exclude substrings (overrides defaults).")
    parser.add_argument("--log-file", type=str, default=None, help="Optional path to write a processing log file.")
    parser.add_argument("--verbose", action="store_true", help="Verbose console output (INFO).")
    args = parser.parse_args()

    input_folder = Path(args.input_folder).expanduser().resolve()
    if not input_folder.exists() or not input_folder.is_dir():
        print(f"ERROR: {input_folder} is not a valid folder.", file=sys.stderr)
        sys.exit(1)

    if args.candidates:
        cand_list = [c.strip() for c in args.candidates.split(",") if c.strip()]
    else:
        cand_list = DEGREE_COLUMN_CANDIDATES

    if args.exclude:
        exclude_list = [c.strip() for c in args.exclude.split(",") if c.strip()]
    else:
        exclude_list = EXCLUDE_COLUMN_SUBSTRINGS

    # --- create per-run log file if not provided ---
    if args.log_file:
        log_file = Path(args.log_file).expanduser().resolve()
        # ensure parent exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        # default logs directory next to this script
        try:
            script_dir = Path(__file__).resolve().parent
        except Exception:
            # fallback to current working directory if __file__ not available
            script_dir = Path.cwd()
        logs_dir = script_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = logs_dir / f"collect_degrees_{timestamp}.log"

    df_res = collect_per_sheet(
        input_folder,
        cand_list,
        scan_rows=args.scan_rows,
        recursive=args.recursive,
        exclude_list=exclude_list,
        verbose=args.verbose,
        log_file=log_file,
    )

    # write output (single sheet with per-sheet rows appended)
    out_path = Path(args.output).expanduser().resolve()
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_res.to_excel(writer, sheet_name="degrees", index=False)

    print(f"Done. Wrote: {out_path} (rows: {len(df_res)})")
    print(f"Log file: {log_file}")


if __name__ == "__main__":
    main()
