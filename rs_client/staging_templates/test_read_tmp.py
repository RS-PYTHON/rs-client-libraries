import yaml
from typing import List, Optional, Dict
from pydantic import BaseModel, StrictStr
from jsonschema import validate
import os
import yaml
import os
import json
from jsonschema import validate, exceptions

def load_yaml_with_references(filename, base_dir=None, loaded_files=None, max_depth=5):
    """
    Load a YAML file resolving references to other yaml files 

    :param filename (str): path to the main yaml file
    :param base_dir: base directory to solve relative paths.
    :param loaded_files: Dictionary of loaded files to resolve circular references.
    :param max_depth: Depth limit for  the recursion process
    :return: final content of the yaml with references replaced with the real yaml content
    """
    if base_dir is None:
        base_dir = os.path.dirname(filename)

    if loaded_files is None:
        loaded_files = {}

    # Avoid loading the same file several time (in the case of circular references)
    if filename in loaded_files:
        return loaded_files[filename]

    # Check that we don't reach the maximum recuresion depth limit
    if max_depth <= 0:
        raise RecursionError("Profondeur maximale de récursion atteinte.")

    # Load the main YAML file and store it in the loaded file dictionary
    with open(filename, 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
    loaded_files[filename] = data

    def resolve_references(data, current_depth):
        if isinstance(data, dict):
            if "$ref" in data and isinstance(data["$ref"], str) and data["$ref"].endswith(".yaml"):
                ref_file = os.path.join(base_dir, data["$ref"])
                if os.path.exists(ref_file):
                    # Load the referenced yaml file
                    ref_data = load_yaml_with_references(ref_file, base_dir, loaded_files, max_depth - 1)
                    # Replace the content of the parent key with the content of the referenced yaml file
                    # and delete the ref key
                    data.update(ref_data)
                    del data["$ref"]
                    ###data[key] = ref_data
                else:
                    print(f"Caution: file not found {ref_file}")
            else:
                # Recursive call for subitems
                for key, value in data.items():
                    resolve_references(value, current_depth + 1)
        elif isinstance(data, list):
            for item in data:
                resolve_references(item, current_depth + 1)

    # Solve references in the content of the main YAML file
    resolve_references(data, 0)
    return data

def validate_with_jsonschema(data, schema):
    """
    Validate input data according to a specified schema
    :param data: Data to validate
    :param schema: Json schema to use for validation
    """
    print(" ----------------- Launching validation -----------------")
    try:
        validate(instance=data, schema=schema)
        print("Validation succeeded !")
    except exceptions.ValidationError as e:
        print(f"Validation failed : {e.message}")


if __name__ == '__main__':
    fichier_schema_yaml = "/home/mcolinde/Bureau/json_schema_valid.json"
    #'/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/process.yaml'
    data_to_validate = {
        "inputs":{
            "schema"
        },
        "outputs": {},
        "id": "aaa",
        "version": "test"
    }
    # Charger et résoudre les références du fichier YAML de schéma
    schema_data = load_yaml_with_references(fichier_schema_yaml)
    #print(f"Schéma data vaut: ", schema_data)

# Validate data with the schema
validate_with_jsonschema(data_to_validate, schema_data)
