sex_map = {
    "0": "Female",
    "1": "Male"
}

chest_pain_map = {
    "1": "Typical Angina",
    "2": "Atypical Angina",
    "3": "Non-anginal Pain",
    "4": "Asymptomatic"
}

fbs_map = {
    "0": "No",
    "1": "Yes"
}

ekg_map = {
    "0": "Normal",
    "1": "ST-T Wave Abnormality",
    "2": "Left Ventricular Hypertrophy"
}

angina_map = {
    "0": "No",
    "1": "Yes"
}

slope_map = {
    "1": "Upsloping",
    "2": "Flat",
    "3": "Downsloping"
}

# thallium codes - 3, 6, 7
thallium_map = {
    "3": "Normal",
    "6": "Fixed Defect",
    "7": "Reversible Defect"
}


def clean_record(row, has_target=True):

    result = {}

    result["id"] = int(row["id"])
    result["age"] = int(row["Age"])
    result["sex"] = sex_map[row["Sex"]]
    result["chest_pain_type"] = chest_pain_map[row["Chest pain type"]]
    result["blood_pressure"] = int(row["BP"])
    result["cholesterol"] = int(row["Cholesterol"])
    result["fasting_blood_sugar_over_120"] = fbs_map[row["FBS over 120"]]
    result["ekg_results"] = ekg_map[row["EKG results"]]
    result["max_heart_rate"] = int(row["Max HR"])
    result["exercise_angina"] = angina_map[row["Exercise angina"]]
    result["st_depression"] = float(row["ST depression"])
    result["slope_of_st"] = slope_map[row["Slope of ST"]]
    result["num_vessels_fluoroscopy"] = int(row["Number of vessels fluro"])
    result["thallium"] = thallium_map[row["Thallium"]]

    # only train data has this column
    if has_target == True:
        result["heart_disease"] = row["Heart Disease"]

    return result


def clean_all_records(valid_rows, has_target=True):

    cleaned = []

    for row in valid_rows:
        try:
            done = clean_record(row, has_target=has_target)
            cleaned.append(done)
        except Exception as e:
            # just print a warning and keep going
            print("skipping record", row.get("id", "?"), "- error:", e)

    return cleaned


if __name__ == "__main__":
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from pipeline.loader import load_csv

    filepath = os.path.join(os.path.dirname(__file__), "..", "data", "train.csv")

    rows = load_csv(filepath)
    cleaned = clean_all_records(rows)

    print("cleaned", len(cleaned), "records")
    print()
    print("first one:")
    for k in cleaned[0]:
        print(" ", k, ":", cleaned[0][k])
