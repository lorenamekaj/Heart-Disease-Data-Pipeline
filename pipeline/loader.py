import csv
import os


def load_csv(filepath):

    # make sure the file is there before trying to open it
    if not os.path.exists(filepath):
        raise FileNotFoundError("file not found: " + filepath)

    all_rows = []

    # open the file and read it
    f = open(filepath, encoding="utf-8")
    reader = csv.DictReader(f)

    if reader.fieldnames is None:
        f.close()
        raise ValueError("the file looks empty")

    for row in reader:
        new_row = {}
        for key in row:
            val = row[key]
            if key != None:
                if val != None:
                    new_row[key.strip()] = val.strip()
                else:
                    new_row[key.strip()] = ""
        all_rows.append(new_row)

    f.close()

    return all_rows


def get_file_info(filepath):
    name = os.path.basename(filepath)
    bytes = os.path.getsize(filepath)
    mb = round(bytes / 1024 / 1024, 2)

    return {"filename": name, "size_mb": mb}


if __name__ == "__main__":
    filepath = os.path.join(os.path.dirname(__file__), "..", "data", "train.csv")

    info = get_file_info(filepath)
    print("file:", info["filename"])
    print("size:", info["size_mb"], "MB")

    rows = load_csv(filepath)
    print("total rows:", len(rows))
    print()
    print("first row:")
    for k in rows[0]:
        print(" ", k, "=", rows[0][k])
