def validate_record(row):
    """
    Validates a single record and returns (is_valid, reason)
    """
    try:
        # Check id
        if "id" not in row or row["id"].strip() == "":
            return False, "Missing id"
        
        int(row["id"])
        
        # Check age
        if "Age" not in row or row["Age"].strip() == "":
            return False, "Missing age"
        age = int(row["Age"])
        if age < 0 or age > 150:
            return False, "Age out of valid range (0-150)"
        
        # Check sex
        if "Sex" not in row or row["Sex"].strip() not in ["0", "1"]:
            return False, "Invalid sex value"
        
        # Check chest pain type
        if "Chest pain type" not in row or row["Chest pain type"].strip() not in ["1", "2", "3", "4"]:
            return False, "Invalid chest pain type"
        
        # Check BP
        if "BP" not in row or row["BP"].strip() == "":
            return False, "Missing blood pressure"
        bp = int(row["BP"])
        if bp < 0 or bp > 300:
            return False, "Blood pressure out of valid range"
        
        # Check cholesterol
        if "Cholesterol" not in row or row["Cholesterol"].strip() == "":
            return False, "Missing cholesterol"
        chol = int(row["Cholesterol"])
        if chol < 0 or chol > 600:
            return False, "Cholesterol out of valid range"
        
        # Check FBS
        if "FBS over 120" not in row or row["FBS over 120"].strip() not in ["0", "1"]:
            return False, "Invalid fasting blood sugar value"
        
        # Check EKG results
        if "EKG results" not in row or row["EKG results"].strip() not in ["0", "1", "2"]:
            return False, "Invalid EKG results"
        
        # Check max HR
        if "Max HR" not in row or row["Max HR"].strip() == "":
            return False, "Missing max heart rate"
        max_hr = int(row["Max HR"])
        if max_hr < 0 or max_hr > 250:
            return False, "Max heart rate out of valid range"
        
        # Check exercise angina
        if "Exercise angina" not in row or row["Exercise angina"].strip() not in ["0", "1"]:
            return False, "Invalid exercise angina value"
        
        # Check ST depression
        if "ST depression" not in row or row["ST depression"].strip() == "":
            return False, "Missing ST depression"
        float(row["ST depression"])
        
        # Check slope of ST
        if "Slope of ST" not in row or row["Slope of ST"].strip() not in ["1", "2", "3"]:
            return False, "Invalid slope of ST"
        
        # Check number of vessels
        if "Number of vessels fluro" not in row or row["Number of vessels fluro"].strip() == "":
            return False, "Missing number of vessels"
        vessels = int(row["Number of vessels fluro"])
        if vessels < 0 or vessels > 4:
            return False, "Number of vessels out of valid range"
        
        # Check thallium
        if "Thallium" not in row or row["Thallium"].strip() not in ["3", "6", "7"]:
            return False, "Invalid thallium value"
        
        return True, "Valid"
        
    except ValueError as e:
        return False, "Type conversion error: " + str(e)
    except Exception as e:
        return False, "Validation error: " + str(e)


def validate_all_records(rows):
    """
    Validates all records and returns (valid_rows, rejected_rows, rejection_reasons_dict)
    """
    valid = []
    rejected = []
    rejection_reasons = {}
    
    for idx, row in enumerate(rows):
        is_valid, reason = validate_record(row)
        
        if is_valid:
            valid.append(row)
        else:
            rejected_record = row.copy()
            rejected_record["row_number"] = idx + 1
            rejected_record["reasons"] = reason
            rejected.append(rejected_record)
            
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
    
    return valid, rejected, rejection_reasons


if __name__ == "__main__":
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from pipeline.loader import load_csv
    
    filepath = os.path.join(os.path.dirname(__file__), "..", "data", "train.csv")
    rows = load_csv(filepath)
    
    valid, rejected, reasons = validate_all_records(rows)
    
    print("Valid records:", len(valid))
    print("Rejected records:", len(rejected))
    print("Rejection reasons:", reasons)


    
