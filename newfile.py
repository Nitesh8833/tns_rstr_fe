#!/usr/bin/env python3
"""
Optimized collect_degrees_per_sheet.py

Same behaviour as before but faster:
 - header detection reads only top N rows (default 20)
 - once header row is found, we read only the matched columns via usecols to avoid loading whole sheet
 - exact normalized header matching kept
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
EXCLUDE_COLUMN_SUBSTRINGS = [
    "practice",
    "notes",
    "address",
]
# ------------------------------

# tune these for speed vs robustness
SCAN_ROWS_DEFAULT = 20   # how many top rows to scan for header detection (reduced for speed)
HEADER_SEARCH_LIMIT = 3  # only accept header rows at indices 0..2 by default

# output config
OUTPUT_COLS = {
    "degree_column_name": "degree_column_name",
    "degree": "degree",
    "degree_count": "degree_count",
    "filename": "filename",
    "sheet": "sheet",
    "sources": "sources",
}
OUTPUT_COL_ORDER = [
    OUTPUT_COLS["degree_column_name"],
    OUTPUT_COLS["degree"],
    OUTPUT_COLS["degree_count"],
    OUTPUT_COLS["filename"],
    OUTPUT_COLS["sheet"],
]

_non_alnum_re = re.compile(r"[^0-9a-z]+")


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    return " ".join(str(s).split()).strip()


def normalize_header_for_match(s: str) -> str:
    if s is None:
        return ""
    s2 = str(s).casefold()
    s2 = _non_alnum_re.sub(" ", s2)
    return " ".join(s2.split()).strip()


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


def find_header_row_by_keywords(sample_df: pd.DataFrame) -> Optional[int]:
    """Pick the row with most plausible header-like cells in the sample (fast)."""
    if sample_df is None or sample_df.empty:
        return None
    candidates = []
    for i in range(len(sample_df)):
        row = sample_df.iloc[i]
        vals = [str(v) if pd.notna(v) else "" for v in row]
        valid = sum(1 for v in vals if _is_valid_header_cell(v))
        nonempty = sum(1 for v in vals if str(v).strip())
        candidates.append((i, valid, nonempty))
    candidates.sort(key=lambda x: (-x[1], -x[2], x[0]))
    best = candidates[0] if candidates else None
    return best[0] if best and best[1] > 0 else None


def get_headers_for_sheet_fast(file_path: Path, sheet_name: str, scan_rows: int, header_search_limit: int) -> Tuple[List[str], Optional[int]]:
    """
    Detect header row using only top `scan_rows` rows, then return header names.
    Important: does NOT read the full sheet; after hdr_idx is found, it uses nrows=0
    to fetch column names quickly.
    """
    try:
        sample = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=scan_rows, engine="openpyxl")
    except Exception:
        return [], None

    hdr_idx_rel = find_header_row_by_keywords(sample)
    if hdr_idx_rel is None:
        return [], None
    if header_search_limit is not None and hdr_idx_rel >= header_search_limit:
        return [], None

    hdr_idx = hdr_idx_rel
    # Use nrows=0 to get columns without loading data rows
    try:
        df0 = pd.read_excel(file_path, sheet_name=sheet_name, header=hdr_idx, nrows=0, engine="openpyxl")
    except Exception:
        return [], None

    headers = []
    for c in df0.columns.tolist():
        if isinstance(c, tuple):
            parts = [str(p).strip() for p in c if p is not None and str(p).strip() != ""]
            headers.append(" ".join(parts))
        else:
            headers.append(str(c))
    return headers, hdr_idx


def collect_per_sheet_fast(
    folder: Path,
    degree_candidates: List[str],
    scan_rows: int = SCAN_ROWS_DEFAULT,
    recursive: bool = False,
    exclude_list: Optional[List[str]] = None,
    verbose: bool = False,
    log_file: Optional[Path] = None,
    header_search_limit: int = HEADER_SEARCH_LIMIT,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
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

    cand_deg_norm = [normalize_header_for_match(c) for c in degree_candidates if c and str(c).strip()]
    cand_deg_norm = sorted(set(cand_deg_norm))
    exclude_norm = [e.casefold().strip() for e in (exclude_list or []) if e and e.strip()]

    patterns = ("*.xlsx", "*.xlsm")
    files = []
    for pat in patterns:
        files.extend(folder.rglob(pat) if recursive else folder.glob(pat))
    files = sorted(files)

    total_files = len(files)
    unreadable_files_count = 0
    unreadable_files: List[Tuple[str, str]] = []
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
            headers, hdr_idx = get_headers_for_sheet_fast(f, sheet, scan_rows, header_search_limit)
            if not headers:
                logger.info(f"    No valid headers detected within top {header_search_limit} rows (skipping sheet).")
                continue

            # map normalized header -> original header string
            header_map: Dict[str, str] = {}
            for col in headers:
                col_display = str(col)
                col_norm = normalize_header_for_match(col_display)
                header_map[col_norm] = col_display

            # choose matched columns by exact normalized equality
            matched_norms = [n for n in header_map.keys() if n in cand_deg_norm and not any(exc in n for exc in exclude_norm)]
            if not matched_norms:
                logger.info("    No exact-match degree columns found in this sheet.")
                continue

            # matched original header names (these are the strings that match after normalization)
            matched_cols = [header_map[n] for n in matched_norms]

            # Read only the matched columns from the sheet (fast)
            # Use usecols with the original header names; pandas will match these after header row
            try:
                df_sheet = pd.read_excel(f, sheet_name=sheet, header=hdr_idx, usecols=matched_cols, engine="openpyxl", dtype=str)
            except Exception as ex:
                logger.warning(f"    Could not read only matched columns (falling back to full read): {ex}")
                try:
                    df_sheet = pd.read_excel(f, sheet_name=sheet, header=hdr_idx, engine="openpyxl", dtype=str)
                except Exception as ex2:
                    logger.warning(f"    Failed to read sheet {sheet}: {ex2}")
                    continue

            # Build per-sheet aggregation
            per_sheet: Dict[str, Dict[str, object]] = {}
            # For each column present in df_sheet (may be subset)
            for actual_col in df_sheet.columns:
                actual_display = str(actual_col)
                actual_norm = normalize_header_for_match(actual_display)
                if any(exc in actual_norm for exc in exclude_norm):
                    continue

                # series of cleaned strings
                try:
                    series = df_sheet[actual_col].astype(str).fillna("").apply(normalize_text)
                except Exception as ex:
                    logger.debug(f"    Could not read column '{actual_col}' in {f.name}|{sheet}: {ex}")
                    continue

                values = [v for v in series if v != "" and v.lower() != "nan"]
                if not values:
                    continue
                counts = collections.Counter(values)
                for val, cnt in counts.items():
                    key = val.casefold()
                    if key not in per_sheet:
                        per_sheet[key] = {"display": val, "count": 0, "columns": set(), "sources": set()}
                    per_sheet[key]["count"] += cnt
                    per_sheet[key]["columns"].add(str(actual_col))
                    per_sheet[key]["sources"].add(f"{f.name}|{sheet}|{actual_col}")

            if not per_sheet:
                logger.info("    No non-empty degree values found in sheet.")
                continue

            total_sheets_with_degrees += 1
            file_has_data = True

            # append one output row per distinct degree; degree_column_name lists only contributing columns
            for key, meta in sorted(per_sheet.items(), key=lambda x: x[0]):
                deg_cols_str = ", ".join(sorted(meta.get("columns", [])))
                row = {
                    OUTPUT_COLS["degree_column_name"]: deg_cols_str,
                    OUTPUT_COLS["degree"]: meta["display"],
                    OUTPUT_COLS["degree_count"]: meta["count"],
                    OUTPUT_COLS["filename"]: f.name,
                    OUTPUT_COLS["sheet"]: sheet,
                    OUTPUT_COLS["sources"]: "; ".join(sorted(meta.get("sources", []))),
                }
                out_rows.append(row)

            logger.info(f"    Found {len(per_sheet)} distinct degree values in sheet.")

        if file_has_data:
            files_with_data.append(f.name)
        else:
            files_without_data.append(f.name)

    if out_rows:
        df_full = pd.DataFrame(out_rows)
        for col in OUTPUT_COL_ORDER:
            if col not in df_full.columns:
                df_full[col] = ""
        df_out = df_full[OUTPUT_COL_ORDER].copy()
    else:
        df_out = pd.DataFrame(columns=OUTPUT_COL_ORDER)

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
    logger.info(f"Files opened (processed): {len(processed_files)}")
    logger.info(f"Files that produced degree rows: {len(files_with_data)}")
    logger.info(f"Files opened but produced no degree rows: {len(files_without_data)}")
    logger.info(f"Total sheets scanned: {total_sheets_scanned}")
    logger.info(f"Sheets with degree values: {total_sheets_with_degrees}")
    logger.info(f"Total distinct degree rows (output rows): {total_degree_rows}")
    logger.info(f"Total degree occurrences (sum of degree_count): {total_degree_occurrences}")
    logger.info("====================")

    return df_out, run_info


# CLI wrapper (same as before, calls collect_per_sheet_fast)
def main():
    parser = argparse.ArgumentParser(description="Collect distinct degree values per sheet across Excel files.")
    parser.add_argument("-i", "--input-folder", required=True, help="Folder containing Excel files to scan.")
    parser.add_argument("-o", "--output", default="degrees_per_sheet.xlsx", help="Output Excel file path.")
    parser.add_argument("--scan-rows", type=int, default=SCAN_ROWS_DEFAULT, help="Top N rows to scan for header detection.")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan subfolders.")
    parser.add_argument("--candidates", type=str, default=None, help="Comma-separated degree header exact names (overrides defaults).")
    parser.add_argument("--exclude", type=str, default=None, help="Comma-separated exclude substrings (overrides defaults).")
    parser.add_argument("--log-file", type=str, default=None, help="Optional path to write a processing text log file.")
    parser.add_argument("--log-excel", type=str, default=None, help="Optional path for separate log Excel workbook.")
    parser.add_argument("--verbose", action="store_true", help="Verbose console output (INFO).")
    parser.add_argument("--header-rows", type=int, default=HEADER_SEARCH_LIMIT, help="Only accept detected header rows within the top N rows.")
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

    # log file default
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

    df_res, run_info = collect_per_sheet_fast(
        Path(args.input_folder),
        degree_candidates=cand_list,
        scan_rows=args.scan_rows,
        recursive=args.recursive,
        exclude_list=exclude_list,
        verbose=args.verbose,
        log_file=log_file,
        header_search_limit=args.header_rows,
    )

    # write output and logs (same as before)
    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_res.to_excel(writer, sheet_name="degrees", index=False)

    log_excel_path = out_path.with_name(out_path.stem + "_log.xlsx")
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

        df_processed = pd.DataFrame(run_info.get("processed_files", []), columns=["filename"])
        df_processed.to_excel(writer, sheet_name="processed_files", index=False)

        df_with = pd.DataFrame(run_info.get("files_with_data", []), columns=["filename"])
        df_without = pd.DataFrame(run_info.get("files_without_data", []), columns=["filename"])
        df_with.to_excel(writer, sheet_name="files_with_data", index=False)
        df_without.to_excel(writer, sheet_name="files_without_data", index=False)

    print(f"Done. Degrees workbook: {out_path} (rows: {len(df_res)})")
    print(f"Done. Log workbook: {log_excel_path}")
    print(f"Text log file: {log_file}")


if __name__ == "__main__":
    main()
