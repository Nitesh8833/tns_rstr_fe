#!/usr/bin/env python3
"""
collect_degrees.py

Scan a folder of Excel workbooks and extract unique degree values from columns
whose header names match any of the configurable candidate substrings.

Output is an Excel file with one row per unique degree value and columns:
  - degree
  - filenames
  - file_count
  - sheets
  - columns
  - sources  (filename|sheet|column ; semicolon-separated)
"""
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional

import pandas as pd

# ------------------------------
# CONFIG: add column name substrings here (case-insensitive matching)
# You can put things like 'degree', 'provider degree', 'degree1', 'deg' etc.
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
    Simple header detection: scan rows in sample_df (header=None) and
    prefer row that contains any of the keywords (case-insensitive substring).
    Fallback: choose row with most 'valid' cells.
    Returns row index (0-based within sample_df) or None.
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

    # prefer rows with any keyword_hits: choose one with highest valid cells then earliest
    kw_rows = [c for c in candidates if c[1] > 0]
    if kw_rows:
        kw_rows.sort(key=lambda x: (-x[2], x[0]))
        return kw_rows[0][0]

    # fallback: highest valid, then highest nonempty, then earliest
    candidates.sort(key=lambda x: (-x[2], -x[3], x[0]))
    return candidates[0][0] if candidates else None


def get_headers_for_sheet(file_path: Path, sheet_name: str, scan_rows: int, keywords: List[str]) -> Tuple[List[str], Optional[int]]:
    """
    Returns (headers_list, header_row_index) for the given sheet.
    header_row_index is 0-based (row index within the sheet).
    """
    try:
        sample = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=scan_rows, engine="openpyxl")
    except Exception as e:
        # can't open sheet sample
        return [], None

    hdr_idx_rel = find_header_row_by_keywords(sample, keywords)
    if hdr_idx_rel is None:
        # fallback to default header=0 if possible
        try:
            df0 = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0, engine="openpyxl")
            return [str(c) for c in df0.columns.tolist()], 0
        except Exception:
            return [], None

    # hdr_idx_rel is relative within sample; it corresponds to the actual row index (same)
    hdr_idx = hdr_idx_rel
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=hdr_idx, engine="openpyxl")
    except Exception:
        # fallback: try reading nrows=0
        try:
            df0 = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0, engine="openpyxl")
            return [str(c) for c in df0.columns.tolist()], 0
        except Exception:
            return [], None

    headers = []
    for c in df.columns.tolist():
        # flatten tuple headers if necessary
        if isinstance(c, tuple):
            parts = [str(p).strip() for p in c if p is not None and str(p).strip() != ""]
            headers.append(" ".join(parts))
        else:
            headers.append(str(c))
    return headers, hdr_idx


# ---------- main processing ----------
def collect_degree_values_from_folder(
    folder: Path,
    candidates: List[str],
    scan_rows: int = SCAN_ROWS_DEFAULT,
    recursive: bool = False,
) -> pd.DataFrame:
    """
    Scan all .xlsx/.xlsm files in folder (optionally recursive) and collect degree values.
    Returns a DataFrame with columns: degree, filenames, file_count, sheets, columns, sources
    """
    # normalize candidate substrings
    cand_norm = [c.casefold().strip() for c in candidates if c and c.strip()]

    # gather results: map normalized_degree -> display_original (first seen) + metadata sets
    results: Dict[str, Dict[str, Set[str]]] = {}
    # structure for results[deg_normalized] = {
    #    "display": original_first_seen,
    #    "filenames": set(...),
    #    "sheets": set(...),
    #    "columns": set(...),
    #    "sources": set("filename|sheet|column")
    # }

    patterns = ("*.xlsx", "*.xlsm")
    files = []
    for pat in patterns:
        files.extend(folder.rglob(pat) if recursive else folder.glob(pat))

    files = sorted(files)

    for f in files:
        try:
            xls = pd.ExcelFile(f, engine="openpyxl")
            sheets = xls.sheet_names
        except Exception:
            # skip unreadable file
            continue

        for sheet in sheets:
            headers, hdr_idx = get_headers_for_sheet(f, sheet, scan_rows, cand_norm)
            if not headers:
                continue

            # check headers for candidate presence (case-insensitive substring)
            matched_cols = []
            for col in headers:
                col_norm = str(col).casefold()
                if any(k in col_norm for k in cand_norm):
                    matched_cols.append(col)

            if not matched_cols:
                continue

            # read sheet using the detected header (if hdr_idx is None fallback to header=0)
            header_arg = hdr_idx if hdr_idx is not None else 0
            try:
                df_sheet = pd.read_excel(f, sheet_name=sheet, header=header_arg, engine="openpyxl", dtype=str)
            except Exception:
                # try read without header as fallback
                try:
                    df_sheet = pd.read_excel(f, sheet_name=sheet, header=0, engine="openpyxl", dtype=str)
                except Exception:
                    continue

            for col in matched_cols:
                # if column not present after re-read (sometimes multiindex flattening differs), try to find best match
                # match by normalized substring in df_sheet.columns
                found_col_name = None
                for actual_col in df_sheet.columns:
                    if str(col).casefold() == str(actual_col).casefold():
                        found_col_name = actual_col
                        break
                if found_col_name is None:
                    # try substring match
                    for actual_col in df_sheet.columns:
                        if any(k in str(actual_col).casefold() for k in cand_norm) and str(col).casefold() in str(actual_col).casefold():
                            found_col_name = actual_col
                            break
                if found_col_name is None:
                    # fallback to the candidate col string itself
                    found_col_name = col

                # get unique non-empty values from this column
                try:
                    series = df_sheet[found_col_name].astype(str).fillna("").apply(lambda x: normalize_text(x))
                except Exception:
                    continue

                vals = set([v for v in series if v != "" and v.lower() != "nan"])
                for v in vals:
                    key = v.casefold()
                    if key not in results:
                        results[key] = {
                            "display": v,
                            "filenames": set(),
                            "sheets": set(),
                            "columns": set(),
                            "sources": set(),
                        }
                    results[key]["filenames"].add(f.name)
                    results[key]["sheets"].add(sheet)
                    results[key]["columns"].add(str(found_col_name))
                    results[key]["sources"].add(f"{f.name}|{sheet}|{found_col_name}")

    # build output rows
    rows = []
    for key, meta in sorted(results.items(), key=lambda x: x[0]):
        filenames_sorted = sorted(meta["filenames"])
        sheets_sorted = sorted(meta["sheets"])
        cols_sorted = sorted(meta["columns"])
        sources_sorted = sorted(meta["sources"])
        rows.append({
            "degree": meta["display"],
            "filenames": ", ".join(filenames_sorted),
            "file_count": len(filenames_sorted),
            "sheets": ", ".join(sheets_sorted),
            "columns": ", ".join(cols_sorted),
            "sources": "; ".join(sources_sorted),
        })

    df_out = pd.DataFrame(rows, columns=["degree", "filenames", "file_count", "sheets", "columns", "sources"])
    return df_out


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Collect unique degree values across many Excel files.")
    parser.add_argument("-i", "--input-folder", required=True, help="Folder containing Excel files to scan.")
    parser.add_argument("-o", "--output", default="degrees_catalog.xlsx", help="Output Excel file path.")
    parser.add_argument("--scan-rows", type=int, default=SCAN_ROWS_DEFAULT, help="Top N rows to scan for header detection.")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan subfolders.")
    parser.add_argument("--candidates", type=str, default=None, help="Comma-separated list of candidate header substrings (overrides code defaults).")
    args = parser.parse_args()

    input_folder = Path(args.input_folder).expanduser().resolve()
    if not input_folder.exists() or not input_folder.is_dir():
        print(f"ERROR: {input_folder} is not a valid folder.", file=sys.stderr)
        sys.exit(1)

    if args.candidates:
        cand_list = [c.strip() for c in args.candidates.split(",") if c.strip()]
    else:
        cand_list = DEGREE_COLUMN_CANDIDATES

    df_res = collect_degree_values_from_folder(input_folder, cand_list, scan_rows=args.scan_rows, recursive=args.recursive)
    out_path = Path(args.output).expanduser().resolve()
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_res.to_excel(writer, sheet_name="degrees", index=False)

    print(f"Done. Wrote: {out_path} (rows: {len(df_res)})")


if __name__ == "__main__":
    main()
