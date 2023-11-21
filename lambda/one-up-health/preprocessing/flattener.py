import json
import os


def snake_case(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def flatten_json(item, parent_key="", sep="__"):
    if isinstance(item, list):
        # If it's a list of dictionaries, handle each item separately
        if all(isinstance(elem, dict) for elem in item):
            # Combine all dictionaries into a single dictionary with lists of values
            combined_dict = {}
            for i, elem in enumerate(item):
                for key, value in elem.items():
                    new_key = f"{parent_key}{sep}{snake_case(key)}"
                    if new_key in combined_dict:
                        combined_dict[new_key].append(value)
                    else:
                        combined_dict[new_key] = [value]
            # Flatten each list of values if it's not a dictionary
            for key, value in combined_dict.items():
                if not isinstance(value[0], dict):
                    combined_dict[snake_case(key)] = value if len(value) > 1 else value
            return combined_dict
        else:
            # If it's a list of values, just return the list under the current key
            return {parent_key: item}
    elif isinstance(item, dict):
        items = {}
        for key, value in item.items():
            new_key = (
                parent_key + sep + snake_case(key) if parent_key else snake_case(key)
            )
            if isinstance(value, (dict, list)):
                sub_items = flatten_json(value, new_key, sep=sep)
                if isinstance(sub_items, dict):
                    items.update(sub_items)
                else:
                    items[new_key] = sub_items
            else:
                items[new_key] = value
        return items
    else:
        return {parent_key: item}


if __name__ == "__main__":
    # Get the directory of the script file
    script_dir = os.path.dirname(__file__)
    # Define the path to your NDJSON file relative to the script file
    ndjson_file_name = "tests/test_data/test_data.ndjson"
    ndjson_file_path = os.path.join(script_dir, ndjson_file_name)
    json_file_path = ndjson_file_path.rsplit(".", 1)[0] + "_flattened.ndjson"

    # Process each line in the NDJSON file and write to a new NDJSON file
    with open(ndjson_file_path, "r") as ndjson_file, open(
        json_file_path, "w"
    ) as json_file:
        line_number = 0
        for line in ndjson_file:
            line_number += 1
            try:
                # Load the JSON object from the line
                json_obj = json.loads(line)
                # Flatten the JSON object
                flattened_obj = flatten_json(json_obj)
                # Write the flattened JSON object to the file, followed by a newline
                json_file.write(json.dumps(flattened_obj) + "\n")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line {line_number}: {e}")

    print(f"Flattened NDJSON has been written to {json_file_path}")
