import argparse
import os
from pipeline.loader import load_csv, get_file_info
from pipeline.cleaner import clean_all_records
from pipeline.reporter import write_clean_csv, write_rejected_csv, print_summary_report


def parse_arguments():
    parser = argparse.ArgumentParser(description="heart disease pipeline")

    parser.add_argument("--file", required=True, help="csv file to use")
    parser.add_argument("--mode", choices=["full", "clean", "analyze"], default="full")
    parser.add_argument("--no-target", action="store_true", help="for test.csv - has no heart disease column")
    parser.add_argument("--output-dir", default="output")

    args = parser.parse_args()
    return args


def step_load(filepath):

    print("loading file:", filepath)

    info = get_file_info(filepath)
    print("size:", info["size_mb"], "MB")

    rows = load_csv(filepath)
    print("rows loaded:", len(rows))

    return rows


def step_clean(valid_rows, has_target):

    print("cleaning", len(valid_rows), "records...")

    result = clean_all_records(valid_rows, has_target=has_target)

    print("done -", len(result), "records cleaned")

    return result


def step_write_outputs(clean_records, rejected_rows, pipeline_stats, analysis_results, output_dir, mode):

    print("saving files to:", output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if mode == "full" or mode == "clean":
        write_clean_csv(clean_records, output_dir + "/clean_data.csv")
        write_rejected_csv(rejected_rows, output_dir + "/rejected_records.csv")

    if mode == "full" or mode == "analyze":
        if analysis_results != None:
            print_summary_report(pipeline_stats, analysis_results)
