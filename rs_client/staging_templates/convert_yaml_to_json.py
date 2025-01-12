import yaml
import json
import fileinput
import os
import os.path as osp
from jsonschema import validate, exceptions

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
    schema_to_use = "/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/json/process.json"
    # Load the json schema as a Python dictionary
    with open(schema_to_use) as json_file:
        schema = json.load(json_file)   
    print(schema)
    
    # schema = {
    #     "type": "object",
    #     "properties": {
    #         "name": {"type": "string"},
    #         "age": {"type": "number"},
    #     },
    #     "required": ["name"],
    # }

    # Validate data with the schema
    validate_with_jsonschema(data_to_validate, schema)