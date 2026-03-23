# Heart-Disease-Data-Pipeline
A modular Python data pipeline that validates, cleans, and analyses heart disease patient records

Lorena's contributions:

loader.py
- Opens the CSV file and reads every row into a list of dictionaries
- Strips whitespace from all values
- Raises errors if the file is missing or empty

cleaner.py
- Converts string values to proper types (e.g. "1" → 1)
- Decodes numeric codes into readable labels (e.g. "1" → "Male")
- Skips the Heart Disease column if the file is test data (no target)

reporter.py
- Writes the cleaned records to clean_data.csv
- Writes rejected records to rejected_records.csv with rejection reasons
- Writes or prints a summary report with pipeline stats and analysis results

main.py
- parse_arguments() — defines the CLI options (--file, --mode, --no-target, --output-dir)
- step_load() — calls the loader and prints file info
- step_clean() — calls the cleaner and prints how many records were cleaned
- step_write_outputs() — decides what to write based on mode, calls the reporter
