import csv
import os

def write_clean_csv(records, output_path):

    if len(records) == 0:
        print("nothing to write")
        return

    columns = list(records[0].keys())

    f = open(output_path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(records)
    f.close()

    print("saved clean data to:", output_path)
    print(len(records), "records written")


def write_rejected_csv(records, output_path):

    if len(records) == 0:
        print("no rejected records")
        return

    columns = list(records[0].keys())

    f = open(output_path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(records)
    f.close()

    print("saved rejected records to:", output_path)
    print(len(records), "records rejected")


def build_report_lines(pipeline_stats, analysis_results):

    lines = []

    lines.append("  HEART DISEASE PIPELINE - SUMMARY REPORT")
    lines.append("")
    lines.append("PIPELINE SUMMARY")
    lines.append("-" * 40)
    lines.append("  file: " + str(pipeline_stats.get("source_file", "N/A")))
    lines.append("  rows loaded: " + str(pipeline_stats.get("total_loaded", 0)))
    lines.append("  duplicates: " + str(pipeline_stats.get("total_duplicates", 0)))
    lines.append("  valid: " + str(pipeline_stats.get("total_valid", 0)))
    lines.append("  rejected: " + str(pipeline_stats.get("total_rejected", 0)))
    lines.append("")

    total = pipeline_stats.get("total_loaded", 0)
    valid = pipeline_stats.get("total_valid", 0)
    rejected = pipeline_stats.get("total_rejected", 0)

    if total > 0:
        lines.append("  clean rate: " + str(round(valid / total * 100, 2)) + "%")
        lines.append("  rejection rate: " + str(round(rejected / total * 100, 2)) + "%")
        lines.append("")

    reasons = pipeline_stats.get("rejection_reasons", {})
    if len(reasons) > 0:
        lines.append("REJECTION REASONS:")
        lines.append("-" * 40)

        sorted_reasons = []
        for r in reasons:
            sorted_reasons.append((r, reasons[r]))
        sorted_reasons.sort(key=lambda x: x[1], reverse=True)

        for i in range(len(sorted_reasons)):
            if i >= 10:
                break
            lines.append("  " + str(sorted_reasons[i][1]) + "x - " + sorted_reasons[i][0])
        lines.append("")

    if analysis_results != None:
        lines.append("DATASET OVERVIEW")
        lines.append("  total records: " + str(analysis_results.get("total_records", 0)))

        hd = analysis_results.get("heart_disease_overall")
        if hd != None:
            lines.append("")
            lines.append("  Heart Disease:")
            total_hd = sum(hd.values())
            for label in hd:
                count = hd[label]
                pct = 0
                if total_hd > 0:
                    pct = round(count / total_hd * 100, 2)
                lines.append("    " + label + ": " + str(count) + " (" + str(pct) + "%)")
        lines.append("")

    num = analysis_results.get("numeric_summary", {})
    if len(num) > 0:
        lines.append("")
        lines.append("NUMERIC STATS")
        lines.append("")
        for col in num:
            s = num[col]
            lines.append("  " + col)
            lines.append("    avg: " + str(s["average"]))
            lines.append("    median: " + str(s["median"]))
            lines.append("    min: " + str(s["minimum"]))
            lines.append("    max: " + str(s["maximum"]))
            lines.append("")

    cat = analysis_results.get("categorical_distributions", {})
    if len(cat) > 0:
        lines.append("")
        lines.append("CATEGORICAL COLUMNS")
        lines.append("")
        for col in cat:
            counts = cat[col]
            total_col = sum(counts.values())
            lines.append("  " + col + ":")
            for val in counts:
                count = counts[val]
                pct = 0
                if total_col > 0:
                    pct = round(count / total_col * 100, 2)
                lines.append("    " + str(val) + ": " + str(count) + " (" + str(pct) + "%)")
            lines.append("")

    hd_age = analysis_results.get("heart_disease_by_age_group")
    if hd_age != None:
        lines.append("")
        lines.append("HEART DISEASE BY AGE GROUP")
        lines.append("")
        for group in hd_age:
            s = hd_age[group]
            lines.append("  " + group + " -> total: " + str(s["total"]) + "  presence: " + str(s["presence"]) + "  rate: " + str(s["rate_%"]) + "%")
        lines.append("")

    hd_sex = analysis_results.get("heart_disease_by_sex")
    if hd_sex != None:
        lines.append("")
        lines.append("HEART DISEASE BY SEX")
        lines.append("")
        for sex in hd_sex:
            s = hd_sex[sex]
            lines.append("  " + sex + " -> total: " + str(s["total"]) + "  presence: " + str(s["presence"]) + "  rate: " + str(s["rate_%"]) + "%")
        lines.append("")

    hd_chest = analysis_results.get("heart_disease_by_chest_pain")
    if hd_chest != None:
        lines.append("")
        lines.append("HEART DISEASE BY CHEST PAIN")
        lines.append("")
        for pain in hd_chest:
            s = hd_chest[pain]
            lines.append("  " + pain + " -> total: " + str(s["total"]) + "  presence: " + str(s["presence"]) + "  rate: " + str(s["rate_%"]) + "%")
        lines.append("")

    lines.append("")
    lines.append("  END OF REPORT")

    return lines


def print_summary_report(pipeline_stats, analysis_results):
    lines = build_report_lines(pipeline_stats, analysis_results)
    for line in lines:
        print(line)


def write_summary_report(pipeline_stats, analysis_results, output_path):
    lines = build_report_lines(pipeline_stats, analysis_results)

    f = open(output_path, "w", encoding="utf-8")
    for line in lines:
        f.write(line + "\n")
    f.close()

    print("report saved to:", output_path)


if __name__ == "__main__":
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from pipeline.loader  import load_csv
    from pipeline.cleaner import clean_all_records

    filepath = os.path.join(os.path.dirname(__file__), "..", "data", "train.csv")
    output_dir = os.path.join(os.path.dirname(__file__), "..", "output")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    rows = load_csv(filepath)
    cleaned = clean_all_records(rows)

    write_clean_csv(cleaned, output_dir + "/clean_data.csv")

    fake = [{"row_number": 2, "id": "999", "reasons": "bad age", "Age": "999"}]
    write_rejected_csv(fake, output_dir + "/rejected_records.csv")