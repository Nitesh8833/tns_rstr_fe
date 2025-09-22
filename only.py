#!/usr/bin/env python3
"""
collect_degrees_per_sheet.py

Scan a folder of Excel workbooks and extract unique degree values sheet-by-sheet.

Output (degrees workbook):
 - degree_column_name, degree, degree_count, filename, sheet

Notes:
 - Header-detection and matching logic is preserved, but the final column
   selection requires an exact match (after normalization) with entries in
   DEGREE_COLUMN_CANDIDATES. Partial matches are NOT accepted.
 - Logging (console + text file) and a separate log Excel workbook are still produced.
 - Output column names are defined in OUTPUT_COLS and OUTPUT_COL_ORDER at the top.
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
import collections
from datetime import datetime
import pandas as pd
import re

# ------------------------------
# CONFIG: candidate substrings for degree columns (case-insensitive, human-readable)
# These are the *exact* header names (after normalization) that will be accepted.
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
]
# ------------------------------

# ------------------------------
# CONFIG: exclude header substrings (case-insensitive)
EXCLUDE_COLUMN_SUBSTRINGS = [
    "practice",
    "notes",
    "address",
]
# ------------------------------

SCAN_ROWS_DEFAULT = 50  # how many top rows to scan to find header row for each sheet

# how many top rows are allowed to be considered the header (0-based index < HEADER_SEARCH_LIMIT)
HEADER_SEARCH_LIMIT = 3  # default: only accept header rows at indices 0..2 (i.e., top 3 rows)

# -------------------------------------------------------------------------
# OUTPUT column configuration (change names here to rename everywhere)
OUTPUT_COLS = {
    "degree_column_name": "degree_column_name",
    "degree": "degree",
    "degree_count": "degree_count",
    "filename": "filename",
    "sheet": "sheet",
    # keep "sources" available if you want to include later - not written by default
    "sources": "sources",
}

# Order of columns to write into the degrees workbook
OUTPUT_COL_ORDER = [
    OUTPUT_COLS["degree_column_name"],
    OUTPUT_COLS["degree"],
    OUTPUT_COLS["degree_count"],
    OUTPUT_COLS["filename"],
    OUTPUT_COLS["sheet"],
]
# -------------------------------------------------------------------------


# ---------- utilities ----------
_non_alnum_re = re.compile(r"[^0-9a-z]+")


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    return " ".join(str(s).split()).strip()


def normalize_header_for_match(s: str) -> str:
    """
    Normalize header strings for matching:
     - convert to string
     - lowercase
     - replace any non-alphanumeric characters with a single space
     - collapse multiple spaces to one, strip edges
    This enables robust exact-equality matching without accidental substring matches.
    """
    if s is None:
        return ""
    s2 = str(s).casefold()
    # replace non-alphanumeric with space
    s2 = _non_alnum_re.sub(" ", s2)
    # collapse spaces and strip
    s2 = " ".join(s2.split()).strip()
    return s2


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
    This uses keyword substring hits as a heuristic to find header row. It does NOT
    determine the final column acceptance rule — column acceptance is done later
    with exact (normalized) matching against DEGREE_COLUMN_CANDIDATES.
    """
    if sample_df is None or sample_df.empty:
        return None

    candidates = []
    for i in range(len(sample_df)):
        row = sample_df.iloc[i]
        vals = [str(v) if pd.notna(v) else "" for v in row]
        valid = sum(1 for v in vals if _is_valid_header_cell(v))
        # keyword_hits using substring on casefold values (heuristic only)
        keyword_hits = sum(1 for v in vals for k in keywords if k in v.casefold())
        nonempty = sum(1 for v in vals if str(v).strip())
        candidates.append((i, keyword_hits, valid, nonempty))

    kw_rows = [c for c in candidates if c[1] > 0]
    if kw_rows:
        kw_rows.sort(key=lambda x: (-x[2], x[0]))
        return kw_rows[0][0]

    candidates.sort(key=lambda x: (-x[2], -x[3], x[0]))
    return candidates[0][0] if candidates else None


def get_headers_for_sheet(
    file_path: Path,
    sheet_name: str,
    scan_rows: int,
    keywords: List[str],
    header_search_limit: int = HEADER_SEARCH_LIMIT,
) -> Tuple[List[str], Optional[int]]:
    """
    Return (headers_list, header_row_index) for the sheet (header_row_index is 0-based).
    Only accepts detected header rows within the first `header_search_limit` rows.
    If no acceptable header is found, returns ([], None) so the sheet is skipped.
    """
    try:
        sample = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=scan_rows, engine="openpyxl")
    except Exception:
        return [], None

    hdr_idx_rel = find_header_row_by_keywords(sample, keywords)
    if hdr_idx_rel is None:
        # no header detected in sample -> skip (do not fallback to reading body as header)
        return [], None

    # If the detected header is beyond the allowed top rows, skip the sheet
    if header_search_limit is not None and hdr_idx_rel >= header_search_limit:
        return [], None

    hdr_idx = hdr_idx_rel
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=hdr_idx, engine="openpyxl")
    except Exception:
        # fallback: skip sheet rather than using body rows as headers
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
    degree_candidates: List[str],
    scan_rows: int = SCAN_ROWS_DEFAULT,
    recursive: bool = False,
    exclude_list: Optional[List[str]] = None,
    verbose: bool = False,
    log_file: Optional[Path] = None,
    header_search_limit: int = HEADER_SEARCH_LIMIT,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Scan files and return df_out and run_info.
    df_out columns: degree_column_name, degree, degree_count, filename, sheet
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

    # Normalize degree candidate names for exact matching (use normalization function)
    cand_deg_norm = [normalize_header_for_match(c) for c in degree_candidates if c and str(c).strip()]
    cand_deg_norm = sorted(set(cand_deg_norm))  # deduplicate and sort

    exclude_norm = [e.casefold().strip() for e in (exclude_list or []) if e and e.strip()]

    patterns = ("*.xlsx", "*.xlsm")
    files = []
    for pat in patterns:
        files.extend(folder.rglob(pat) if recursive else folder.glob(pat))
    files = sorted(files)

    # Counters and lists for summary
    total_files = len(files)
    unreadable_files_count = 0
    unreadable_files: List[Tuple[str, str]] = []  # (filename, error)
    processed_files: List[str] = []
    files_with_data: List[str] = []
    files_without_data: List[str] = []
    total_sheets_scanned = 0
    total_sheets_with_degrees = 0

    out_rows = []

    for f in files:
        logger.info(f"Processing file: {f.name}")
        try:
            xls = pd.ExcelFile(f, engine="openpyxl")
            sheets = xls.sheet_names
        except Exception as ex:
            unreadable_files_count += 1
            unreadable_files.append((f.name, str(ex)))
            logger.warning(f"  Skipping unreadable file: {f.name}  ({ex})")
            continue

        processed_files.append(f.name)
        file_has_data = False

        for sheet in sheets:
            total_sheets_scanned += 1
            logger.info(f"  Processing sheet: {sheet}")
            headers, hdr_idx = get_headers_for_sheet(f, sheet, scan_rows, cand_deg_norm, header_search_limit=header_search_limit)
            if not headers:
                logger.info(f"    No valid headers detected within top {header_search_limit} rows (skipping sheet).")
                continue

            # find candidate degree columns in this sheet — require exact header match
            matched_deg_cols = []
            # Build a mapping from normalized header -> original header string
            header_map: Dict[str, str] = {}
            for col in headers:
                col_display = str(col)
                col_norm = normalize_header_for_match(col_display)
                header_map[col_norm] = col_display

            # Now select only headers whose normalized form exactly matches one of the normalized candidates
            for norm_header, original_header in header_map.items():
                if any(exc in norm_header for exc in exclude_norm):
                    logger.debug(f"    Excluding header '{original_header}' (matches exclude list).")
                    continue
                if norm_header in cand_deg_norm:
                    matched_deg_cols.append(original_header)

            if not matched_deg_cols:
                logger.info(f"    No exact-match degree columns found in this sheet (only checking configured candidates).")
                continue

            # read sheet with detected header (hdr_idx is guaranteed to be not None here)
            header_arg = hdr_idx
            try:
                df_sheet = pd.read_excel(f, sheet_name=sheet, header=header_arg, engine="openpyxl", dtype=str)
            except Exception as ex:
                logger.warning(f"    Could not read sheet with header={header_arg} ({ex}), skipping sheet.")
                continue

            # Build per-sheet mapping: normalized_degree -> metadata (count, columns set, sources set, display)
            per_sheet: Dict[str, Dict[str, object]] = {}
            degree_columns_found: List[str] = []
            for col in matched_deg_cols:
                # map candidate header to actual column in df_sheet using exact normalized match only
                found_col_name = None
                col_norm = normalize_header_for_match(col)
                for actual_col in df_sheet.columns:
                    actual_display = str(actual_col)
                    actual_norm = normalize_header_for_match(actual_display)
                    if any(exc in actual_norm for exc in exclude_norm):
                        continue
                    if actual_norm == col_norm:
                        found_col_name = actual_col
                        break

                if found_col_name is None:
                    logger.debug(f"    Exact header '{col}' not found as a column in {f.name}|{sheet}; skipping.")
                    continue

                degree_columns_found.append(str(found_col_name))

                # extract column values (as strings)
                try:
                    series = df_sheet[found_col_name].astype(str).fillna("").apply(lambda x: normalize_text(x))
                except Exception as ex:
                    logger.debug(f"    Could not read column '{found_col_name}' in {f.name}|{sheet}: {ex}")
                    continue

                # count occurrences per value in this column
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
                    per_sheet[key]["count"] += cnt
                    per_sheet[key]["columns"].add(str(found_col_name))
                    per_sheet[key]["sources"].add(f"{f.name}|{sheet}|{found_col_name}")

            if not per_sheet:
                logger.info(f"    No non-empty degree values found in sheet.")
                continue

            total_sheets_with_degrees += 1
            file_has_data = True

            deg_cols_str = ", ".join(sorted(set(degree_columns_found))) if degree_columns_found else ""

            # append one output row per distinct degree (preserve deterministic order)
            for key, meta in sorted(per_sheet.items(), key=lambda x: x[0]):
                row = {
                    OUTPUT_COLS["degree_column_name"]: deg_cols_str,
                    OUTPUT_COLS["degree"]: meta["display"],
                    OUTPUT_COLS["degree_count"]: meta["count"],
                    OUTPUT_COLS["filename"]: f.name,
                    OUTPUT_COLS["sheet"]: sheet,
                    # keep sources available in the row if you later want to include it
                    OUTPUT_COLS["sources"]: "; ".join(sorted(meta["sources"])),
                }
                out_rows.append(row)

            logger.info(f"    Found {len(per_sheet)} distinct degree values in sheet.")

        # file-level summary
        if file_has_data:
            files_with_data.append(f.name)
        else:
            files_without_data.append(f.name)

    # build DataFrame and restrict to configured columns
    if out_rows:
        df_full = pd.DataFrame(out_rows)
        # ensure configured output columns exist
        for col in OUTPUT_COL_ORDER:
            if col not in df_full.columns:
                df_full[col] = ""
        df_out = df_full[OUTPUT_COL_ORDER].copy()
    else:
        df_out = pd.DataFrame(columns=OUTPUT_COL_ORDER)

    # Final summary info
    total_degree_rows = len(df_out)
    total_degree_occurrences = int(df_out[OUTPUT_COLS["degree_count"]].sum()) if not df_out.empty else 0

    run_info: Dict[str, Any] = {
        "patterns": patterns,
        "total_files": total_files,
        "unreadable_files_count": unreadable_files_count,
        "unreadable_files": unreadable_files,
        "processed_files": processed_files,
        "files_with_data": files_with_data,
        "files_without_data": files_without_data,
        "total_sheets_scanned": total_sheets_scanned,
        "total_sheets_with_degrees": total_sheets_with_degrees,
        "total_degree_rows": total_degree_rows,
        "total_degree_occurrences": total_degree_occurrences,
    }

    logger.info("=== RUN SUMMARY ===")
    logger.info(f"Total files found (patterns {patterns}): {total_files}")
    logger.info(f"Files unreadable / skipped: {unreadable_files_count}")
    if unreadable_files:
        logger.info("  Unreadable files (filename -> error):")
        for name, err in unreadable_files:
            logger.info(f"    {name} -> {err}")

    logger.info(f"Files opened (processed): {len(processed_files)}")
    logger.info(f"Files that produced degree rows: {len(files_with_data)}")
    logger.info(f"Files opened but produced no degree rows: {len(files_without_data)}")
    logger.info(f"Total sheets scanned: {total_sheets_scanned}")
    logger.info(f"Sheets with degree values: {total_sheets_with_degrees}")
    logger.info(f"Total distinct degree rows (output rows): {total_degree_rows}")
    logger.info(f"Total degree occurrences (sum of degree_count): {total_degree_occurrences}")
    logger.info("====================")

    return df_out, run_info


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Collect distinct degree values per sheet across Excel files.")
    parser.add_argument("-i", "--input-folder", required=True, help="Folder containing Excel files to scan.")
    parser.add_argument("-o", "--output", default="degrees_per_sheet.xlsx", help="Output Excel file path (DEGREES workbook).")
    parser.add_argument("--scan-rows", type=int, default=SCAN_ROWS_DEFAULT, help="Top N rows to scan for header detection.")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan subfolders.")
    parser.add_argument("--candidates", type=str, default=None, help="Comma-separated degree header exact names (overrides defaults).")
    parser.add_argument("--exclude", type=str, default=None, help="Comma-separated exclude substrings (overrides defaults).")
    parser.add_argument("--log-file", type=str, default=None, help="Optional path to write a processing text log file.")
    parser.add_argument("--log-excel", type=str, default=None, help="Optional path for separate log Excel workbook.")
    parser.add_argument("--verbose", action="store_true", help="Verbose console output (INFO).")
    parser.add_argument("--header-rows", type=int, default=HEADER_SEARCH_LIMIT,
                        help="Only accept detected header rows within the top N rows (default: 3).")
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

    header_search_limit = args.header_rows

    # prepare text logfile
    if args.log_file:
        log_file = Path(args.log_file).expanduser().resolve()
        log_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        try:
            script_dir = Path(__file__).resolve().parent
        except Exception:
            script_dir = Path.cwd()
        logs_dir = script_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = logs_dir / f"collect_degrees_{timestamp}.log"

    # determine log-excel path
    if args.log_excel:
        log_excel_path = Path(args.log_excel).expanduser().resolve()
    else:
        out_path = Path(args.output).expanduser().resolve()
        log_excel_path = out_path.with_name(out_path.stem + "_log.xlsx")

    # run processing
    df_res, run_info = collect_per_sheet(
        Path(args.input_folder),
        degree_candidates=cand_list,
        scan_rows=args.scan_rows,
        recursive=args.recursive,
        exclude_list=exclude_list,
        verbose=args.verbose,
        log_file=log_file,
        header_search_limit=header_search_limit,
    )

    # flush file handlers of the logger so the text logfile is complete before reading
    logger = logging.getLogger("collect_degrees")
    for h in list(logger.handlers):
        try:
            if isinstance(h, logging.FileHandler):
                h.flush()
        except Exception:
            pass

    # read text logfile into list of lines
    log_lines = []
    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                log_lines.append(line.rstrip("\n\r"))
    except Exception:
        log_lines = [f"Could not read text log file: {log_file}"]

    # 1) write degrees workbook (only configured columns)
    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_res.to_excel(writer, sheet_name="degrees", index=False)

    # 2) write log workbook (run_summary + other log sheets + log_text)
    log_excel_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(log_excel_path, engine="openpyxl") as writer:
        summary_rows = [
            ("patterns", ", ".join(run_info.get("patterns", []))),
            ("total_files", run_info.get("total_files", 0)),
            ("unreadable_files_count", run_info.get("unreadable_files_count", 0)),
            ("processed_files_count", len(run_info.get("processed_files", []))),
            ("files_with_data_count", len(run_info.get("files_with_data", []))),
            ("files_without_data_count", len(run_info.get("files_without_data", []))),
            ("total_sheets_scanned", run_info.get("total_sheets_scanned", 0)),
            ("total_sheets_with_degrees", run_info.get("total_sheets_with_degrees", 0)),
            ("total_degree_rows", run_info.get("total_degree_rows", 0)),
            ("total_degree_occurrences", run_info.get("total_degree_occurrences", 0)),
        ]
        df_summary = pd.DataFrame(summary_rows, columns=["metric", "value"])
        df_summary.to_excel(writer, sheet_name="run_summary", index=False)

        # unreadable files
        unreadable = run_info.get("unreadable_files", [])
        if unreadable:
            df_unreadable = pd.DataFrame(unreadable, columns=["filename", "error"])
        else:
            df_unreadable = pd.DataFrame(columns=["filename", "error"])
        df_unreadable.to_excel(writer, sheet_name="unreadable_files", index=False)

        # processed files
        df_processed = pd.DataFrame(run_info.get("processed_files", []), columns=["filename"])
        df_processed.to_excel(writer, sheet_name="processed_files", index=False)

        # files with data / without data
        df_with = pd.DataFrame(run_info.get("files_with_data", []), columns=["filename"])
        df_without = pd.DataFrame(run_info.get("files_without_data", []), columns=["filename"])
        df_with.to_excel(writer, sheet_name="files_with_data", index=False)
        df_without.to_excel(writer, sheet_name="files_without_data", index=False)

        # log_text sheet: one timestamped log line per row
        df_log_text = pd.DataFrame(log_lines, columns=["log_line"])
        df_log_text.to_excel(writer, sheet_name="log_text", index=False)

    print(f"Done. Degrees workbook: {out_path} (rows: {len(df_res)})")
    print(f"Done. Log workbook: {log_excel_path} (includes log_text sheet)")
    print(f"Text log file: {log_file}")


if __name__ == "__main__":
    main()
