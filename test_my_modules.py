import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pipeline.loader import load_csv
from pipeline.validator import validate_all_records, validate_record
from pipeline.analyzer import analyze_records
from pipeline.cleaner import clean_all_records

print("="*60)
print("TESTING VALIDATOR MODULE")
print("="*60)

# Test 1: Valid record
valid_record = {
    "id": "1",
    "Age": "45",
    "Sex": "1",
    "Chest pain type": "1",
    "BP": "130",
    "Cholesterol": "250",
    "FBS over 120": "0",
    "EKG results": "0",
    "Max HR": "150",
    "Exercise angina": "0",
    "ST depression": "0.5",
    "Slope of ST": "1",
    "Number of vessels fluro": "1",
    "Thallium": "3",
    "Heart Disease": "1"
}

is_valid, reason = validate_record(valid_record)
print(f"Test 1 - Valid record: {is_valid} ({reason})")
assert is_valid == True, "Should validate good record"

# Test 2: Invalid age
invalid_age = valid_record.copy()
invalid_age["Age"] = "999"
is_valid, reason = validate_record(invalid_age)
print(f"Test 2 - Invalid age: {is_valid} ({reason})")
assert is_valid == False, "Should reject out of range age"

# Test 3: Missing field
missing_field = valid_record.copy()
del missing_field["Age"]
is_valid, reason = validate_record(missing_field)
print(f"Test 3 - Missing age: {is_valid} ({reason})")
assert is_valid == False, "Should reject missing field"

# Test 4: Invalid BP
invalid_bp = valid_record.copy()
invalid_bp["BP"] = "999"
is_valid, reason = validate_record(invalid_bp)
print(f"Test 4 - Invalid BP: {is_valid} ({reason})")
assert is_valid == False, "Should reject out of range BP"

print("\n" + "="*60)
print("TESTING ANALYZER MODULE")
print("="*60)

# Load small sample
filepath = os.path.join(os.path.dirname(__file__), "data", "train.csv")
print(f"\nLoading sample from {filepath}...")
rows = load_csv(filepath)
print(f"Loaded: {len(rows)} rows")

# Validate
print("Validating...")
valid_rows, rejected_rows, rejection_reasons = validate_all_records(rows)
print(f"Valid: {len(valid_rows)}, Rejected: {len(rejected_rows)}")

# Clean
print("Cleaning...")
cleaned = clean_all_records(valid_rows, has_target=True)
print(f"Cleaned: {len(cleaned)} records")

# Analyze
print("Analyzing...")
analysis = analyze_records(cleaned, has_target=True)

# Test analysis results
print("\nAnalysis Results:")
print(f"  Total records: {analysis.get('total_records')}")

hd = analysis.get("heart_disease_overall")
if hd:
    print(f"  Heart Disease distribution: {hd}")
    total_hd = sum(hd.values())
    print(f"  Total with disease data: {total_hd}")

age_groups = analysis.get("heart_disease_by_age_group")
if age_groups:
    print(f"\n  Age groups found: {len(age_groups)}")
    for group in sorted(age_groups.keys()):
        print(f"    {group}: {age_groups[group]}")

sex_groups = analysis.get("heart_disease_by_sex")
if sex_groups:
    print(f"\n  Sex groups found: {len(sex_groups)}")
    for sex in sex_groups:
        print(f"    {sex}: {sex_groups[sex]}")

numeric = analysis.get("numeric_summary")
if numeric:
    print(f"\n  Numeric fields analyzed: {list(numeric.keys())}")
    print(f"    Age stats: {numeric.get('age')}")

categorical = analysis.get("categorical_distributions")
if categorical:
    print(f"\n  Categorical fields: {list(categorical.keys())}")

print("\n" + "="*60)
print("ALL TESTS PASSED!")
print("="*60)


