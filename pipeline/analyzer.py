def get_numeric_stats(records, field):
    """
    Calculate min, max, average, and median for a numeric field
    """
    values = []
    for record in records:
        if field in record:
            try:
                val = float(record[field])
                values.append(val)
            except:
                pass
    
    if len(values) == 0:
        return None
    
    values.sort()
    total = sum(values)
    average = total / len(values)
    minimum = values[0]
    maximum = values[-1]
    
    if len(values) % 2 == 0:
        median = (values[len(values)//2 - 1] + values[len(values)//2]) / 2
    else:
        median = values[len(values)//2]
    
    return {
        "average": round(average, 2),
        "median": round(median, 2),
        "minimum": minimum,
        "maximum": maximum
    }


def get_categorical_distribution(records, field):
    """
    Get value distribution for a categorical field
    """
    distribution = {}
    for record in records:
        if field in record:
            val = record[field]
            distribution[val] = distribution.get(val, 0) + 1
    
    return distribution


def get_age_group(age):
    """
    Categorize age into groups
    """
    age = int(age)
    if age < 30:
        return "< 30"
    elif age < 40:
        return "30-39"
    elif age < 50:
        return "40-49"
    elif age < 60:
        return "50-59"
    else:
        return "60+"


def analyze_records(cleaned_records, has_target=True):
    """
    Performs analysis on cleaned records
    Returns analysis results dictionary
    """
    analysis = {}
    
    # Total records
    analysis["total_records"] = len(cleaned_records)
    
    if has_target:
        # Heart disease overall distribution
        hd_dist = get_categorical_distribution(cleaned_records, "heart_disease")
        analysis["heart_disease_overall"] = hd_dist
        
        # Heart disease by age group
        age_groups = {}
        for record in cleaned_records:
            age_group = get_age_group(record["age"])
            if age_group not in age_groups:
                age_groups[age_group] = {"total": 0, "presence": 0}
            
            age_groups[age_group]["total"] += 1
            if record["heart_disease"] != "0":
                age_groups[age_group]["presence"] += 1
        
        # Calculate percentages
        for group in age_groups:
            total = age_groups[group]["total"]
            presence = age_groups[group]["presence"]
            if total > 0:
                rate = round(presence / total * 100, 2)
            else:
                rate = 0
            age_groups[group]["rate_%"] = rate
        
        analysis["heart_disease_by_age_group"] = age_groups
        
        # Heart disease by sex
        sex_groups = {}
        for record in cleaned_records:
            sex = record["sex"]
            if sex not in sex_groups:
                sex_groups[sex] = {"total": 0, "presence": 0}
            
            sex_groups[sex]["total"] += 1
            if record["heart_disease"] != "0":
                sex_groups[sex]["presence"] += 1
        
        # Calculate percentages
        for sex in sex_groups:
            total = sex_groups[sex]["total"]
            presence = sex_groups[sex]["presence"]
            if total > 0:
                rate = round(presence / total * 100, 2)
            else:
                rate = 0
            sex_groups[sex]["rate_%"] = rate
        
        analysis["heart_disease_by_sex"] = sex_groups
        
        # Heart disease by chest pain type
        chest_groups = {}
        for record in cleaned_records:
            chest = record["chest_pain_type"]
            if chest not in chest_groups:
                chest_groups[chest] = {"total": 0, "presence": 0}
            
            chest_groups[chest]["total"] += 1
            if record["heart_disease"] != "0":
                chest_groups[chest]["presence"] += 1
        
        # Calculate percentages
        for chest in chest_groups:
            total = chest_groups[chest]["total"]
            presence = chest_groups[chest]["presence"]
            if total > 0:
                rate = round(presence / total * 100, 2)
            else:
                rate = 0
            chest_groups[chest]["rate_%"] = rate
        
        analysis["heart_disease_by_chest_pain"] = chest_groups
    
    # Numeric stats for all numeric fields
    numeric_fields = ["age", "blood_pressure", "cholesterol", "max_heart_rate", "st_depression", "num_vessels_fluoroscopy"]
    numeric_summary = {}
    for field in numeric_fields:
        stats = get_numeric_stats(cleaned_records, field)
        if stats:
            numeric_summary[field] = stats
    
    analysis["numeric_summary"] = numeric_summary
    
    # Categorical distributions for all categorical fields
    categorical_fields = ["sex", "chest_pain_type", "fasting_blood_sugar_over_120", "ekg_results", "exercise_angina", "slope_of_st", "thallium"]
    categorical_dist = {}
    for field in categorical_fields:
        dist = get_categorical_distribution(cleaned_records, field)
        if dist:
            categorical_dist[field] = dist
    
    analysis["categorical_distributions"] = categorical_dist
    
    return analysis


if __name__ == "__main__":
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from pipeline.loader import load_csv
    from pipeline.cleaner import clean_all_records
    
    filepath = os.path.join(os.path.dirname(__file__), "..", "data", "train.csv")
    rows = load_csv(filepath)
    cleaned = clean_all_records(rows)
    
    analysis = analyze_records(cleaned)
    
    print("Analysis Results:")
    print("Total records:", analysis["total_records"])
    print("Numeric summary fields:", list(analysis.get("numeric_summary", {}).keys()))
    print("Categorical fields:", list(analysis.get("categorical_distributions", {}).keys()))




    
