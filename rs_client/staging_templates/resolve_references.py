import json
import os
from jsonschema import validate, exceptions, RefResolver
import yaml
import json
import fileinput
import os.path as osp


def load_json(filename):
    """Load a JSON file from the specified path."""
    with open(filename, 'r', encoding='utf-8') as file:
        return json.load(file)

def load_json_with_references(filename, base_dir=None, loaded_files=None, processing_files=None):
    """
    Load a JSON schema and resolve external references while avoiding recursion errors.
    
    :param filename: Path to the main JSON schema file.
    :param base_dir: The base directory for resolving relative paths.
    :param loaded_files: A dictionary to track already loaded files and their content.
    :param processing_files: A set to track files currently being processed to avoid circular references.
    :return: The complete schema with resolved references.
    """
    if base_dir is None:
        base_dir = os.path.dirname(filename)

    if loaded_files is None:
        loaded_files = {}

    if processing_files is None:
        processing_files = set()

    # Avoid reprocessing the same file (detect circular references)
    if filename in processing_files:
        return {}  # Return an empty object or handle it in a way that suits your validation

    if filename in loaded_files:
        return loaded_files[filename]

    processing_files.add(filename)

    data = load_json(filename)
    loaded_files[filename] = data

    def resolve_references(obj):
        """Resolve references in an object (dictionary or list)."""
        if isinstance(obj, dict):
            if "$ref" in obj and ".json" in obj["$ref"]:
                ref_path = obj["$ref"]
                ref_file = os.path.join(base_dir, ref_path)
                ref_file = os.path.normpath(ref_file)
                ref_data = load_json_with_references(ref_file, base_dir, loaded_files, processing_files)
                obj.update(ref_data)
                del obj["$ref"]
            else:
                for key, value in obj.items():
                    resolve_references(value)
        elif isinstance(obj, list):
            for item in obj:
                resolve_references(item)
                
    resolve_references(data)
    processing_files.remove(filename)
    return data

def validate_with_jsonschema(data, schema):
    """Validate a Python dictionary against a JSON schema with resolved references."""
    try:
        validate(instance=data, schema=schema)
        print("Validation successful!")
    except exceptions.ValidationError as e:
        print(f"Validation failed: {e.message}")

def convert_schema_booleans_to_json_compatible_with_defaults(schema):
    if isinstance(schema, dict):
        # Parcourir chaque clé/valeur du dictionnaire
        return {k: convert_schema_booleans_to_json_compatible_with_defaults(v) for k, v in schema.items()}
    elif isinstance(schema, list):
        # Parcourir les éléments de la liste
        return [convert_schema_booleans_to_json_compatible_with_defaults(v) for v in schema]
    elif schema is True or schema is False:
        # Convertir les booléens en objets JSON Schema avec une valeur par défaut
        return {
            "type": "boolean",
            "default": schema  # Utiliser la valeur booléenne actuelle comme défaut
        }
    else:
        return schema


if __name__ == '__main__':

    yaml_folder = "/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/yaml"
    json_folder = "/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/json"
    yaml_file_list = os.listdir(yaml_folder)

    for yaml_filename in yaml_file_list:
        yaml_filepath = osp.join(yaml_folder, yaml_filename)
        json_filepath = osp.join(json_folder, yaml_filename.replace(".yaml", ".json"))
        # Convert yaml file to json
        with open(yaml_filepath, 'r') as yaml_in, open(json_filepath, "w") as json_out:
            yaml_object = yaml.safe_load(yaml_in) # yaml_object will be a list or a dict
            json.dump(yaml_object, json_out, indent=4)
        
        # Replace ".yaml" with ".json" extension inside this new json file  
        with fileinput.FileInput(json_filepath, inplace=True) as file:
            for line in file:
                print(line.replace(".yaml", ".json"), end="")
            
    # Validate Python object with the newly created jsonschema
    data_to_validate = {"name": "John", "age": "30"}
    schema_filename = "/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/json/process.json"

    # Load and resolve references in the schema
    schema_resolved = load_json_with_references(schema_filename)
            
    #schema_resolved = convert_schema_booleans_to_json_compatible_with_defaults(schema_resolved)
    
    # Example Python object to validate
    # data_to_validate = {
    #     "name": "test",
    #     "address": {
    #         "street": "123 Main St",
    #         "city": "Paris"
    #     },
    #     "contact": {
    #         "phone": "123-456-7890"
    #     }
    # }
    
    print(f"schema resolved is: ", schema_resolved)
    # Validate the Python object with the resolved schema
    print("----------- Launching validation -----------")
    validate_with_jsonschema({"aaa":123}, schema_resolved)