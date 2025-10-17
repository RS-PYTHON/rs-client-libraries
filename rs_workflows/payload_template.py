# Copyright 2025 CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    This file defines the schema for the payload template. The following link has been used to create it
    https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/processor-orchestration-guide/triggering-usage.html
    The schema is based on Pydantic (standard for schema + validation + autocompletion).
"""

from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator#, ConfigDict


# Utility / Common classes

class StorageOptions(BaseModel):
    """Options to access a storage backend"""
    key: Optional[str] = None
    secret: Optional[str] = None
    client_kwargs: Optional[Dict[str, str]] = None

class StoreOptionsWrapper(BaseModel):
    """Wrapper for a list of storage options"""
    storage_options: List[StorageOptions]

class StoreParams(BaseModel):
    """Flexible store_params representation for payloads"""
    # Either a simple S3 secret alias
    s3_secret_alias: Optional[str] = None
    # Or a list of storage options
    options: Optional[List[StoreOptionsWrapper]] = None
    # Or a regex + multiplicity
    regex: Optional[str] = None
    multiplicity: Optional[Union[str, int]] = None

    @field_validator("multiplicity")
    @classmethod
    def validate_multiplicity(cls, v):
        """ Validation of multiplicity field """
        if v is None:
            return v
        if isinstance(v, str) and v not in {"exactly_one", "at_least_one", "more_than_one"}:
            raise ValueError(
                'multiplicity must be "exactly_one", "at_least_one", "more_than_one" or an integer'
            )
        elif not isinstance(v, (str, int)):
            raise ValueError("multiplicity must be a string or an integer")
        return v

    @classmethod
    def from_dict(cls, data):
        """Helper to parse from dict-like YAML structure"""
        if isinstance(data, dict):
            if "s3_secret_alias" in data:
                return cls(s3_secret_alias=data["s3_secret_alias"])
            elif "regex" in data or "multiplicity" in data:
                return cls(regex=data.get("regex"), multiplicity=data.get("multiplicity"))
        elif isinstance(data, list):
            wrappers = [StoreOptionsWrapper(**item) for item in data]
            return cls(options=wrappers)
        raise ValueError("Invalid store_params format")

class LoggingConfig(BaseModel):
    level: Optional[str] = Field(default="INFO", description="Logging level")
    
# Main sections

class GeneralConfiguration(BaseModel):
    """General configuration options for EOConfiguration behavior"""
    logging: Optional[LoggingConfig] = None

    triggering__use_basic_logging: Optional[bool] = None
    triggering__wait_before_exit: Optional[int] = None
    dask__export_graphs: Optional[str] = None
    breakpoints__folder: Optional[str] = None
    triggering__create_temporary: Optional[bool] = None
    triggering__temporary_shared: Optional[bool] = None
    triggering__validate_run: Optional[bool] = None
    triggering__validate_mode: Optional[str] = None
    triggering__error_policy: Optional[str] = None
    temporary__folder: Optional[str] = None
    temporary__folder_s3_secret: Optional[str] = None
    temporary__folder_create_folder: Optional[bool] = None
    triggering__dask_monitor__enabled: Optional[bool] = None
    triggering__dask_monitor__cancel: Optional[bool] = None
    triggering__dask_monitor__cancel_state: Optional[str] = None
    
    
    class Config:
        """Allow future unknown fields"""
        extra = "allow"  

class ExternalModule(BaseModel):
    """Definition of an external module to import dynamically"""
    name: str
    alias: Optional[str] = None
    nested: Optional[bool] = None
    folder: Optional[str] = None


class Breakpoints(BaseModel):
    """Configuration for debugging breakpoints"""
    activate_all: Optional[bool] = None
    folder: Optional[str] = None
    store_params: Optional[StoreParams] = None
    ids: Optional[List[str]] = None

class WorkflowStep(BaseModel):
    """Definition of a workflow step (processing unit)"""
    name: str
    active: Optional[bool] = True
    validate_output: Optional[bool] = Field(True, alias="validate")
    step: Optional[int] = None    
    module: Optional[str] = None
    processing_unit: Optional[str] = None
    inputs: Optional[Dict[str, str]] = None
    outputs: Optional[Dict[str, str]] = None
    adfs: Optional[Dict[str, str]] = None
    parameters: Optional[Dict[str, Union[str, int, float, bool,
                                         Union[int, List[int]],
                                         Union[str, List[str]],
                                         ]]] = None


class InputProduct(BaseModel):
    """Definition of an input product in the I/O configuration"""
    id: str
    path: str
    type: Optional[str] = Field(default="filename")
    store_type: str
    store_params: Optional[StoreParams] = None


class OutputProduct(BaseModel):
    """Definition of an output product in the I/O configuration"""
    id: str
    path: str
    store_type: str
    store_params: Optional[StoreParams] = None
    type: Optional[str] = Field(default="filename")
    opening_mode: Optional[str] = Field(default="CREATE")
    apply_eoqc: Optional[bool] = Field(default=False)


class AdfConfig(BaseModel):
    """Definition of an ADF configuration entry"""
    id: str
    path: str
    store_params: Optional[StoreParams] = None


class IOConfig(BaseModel):
    """Input/output configuration"""
    input_products: List[InputProduct] = []
    output_products: List[OutputProduct] = []
    adfs: List[AdfConfig] = []


class DaskContext(BaseModel):
    """Configuration for the DaskContext"""
    cluster_type: Optional[str] = None
    cluster_config: Optional[Dict[str, Union[str, int, bool]]] = None
    client_config: Optional[Dict[str, Union[str, int, bool]]] = None
    dask_config: Optional[Dict[str, Union[str, int, bool]]] = None
    performance_report_file: Optional[str] = None


class EOQCConfig(BaseModel):
    """Configuration for the EOQC processor"""
    config_folder: Optional[str] = Field(default="default")
    parameters: Optional[Dict[str, Union[str, int, float, bool]]] = Field(default_factory=dict)
    update_attrs: Optional[bool] = Field(default=True)
    report_path: Optional[str] = None
    config_path: Optional[str] = None
    additional_config_folders: Optional[List[str]] = None


# Root payload model

class PayloadSchema(BaseModel):
    """Root payload schema containing all configuration sections"""
    dotenv: Optional[List[str]] = None
    general_configuration: Optional[GeneralConfiguration] = None
    external_modules: Optional[List[ExternalModule]] = None
    breakpoints: Optional[Breakpoints] = None
    workflow: Optional[List[WorkflowStep]] = None
    io: Optional[IOConfig] = Field(None, alias="I/O")
    dask_context: Optional[DaskContext] = None
    logging: Optional[List[str]] = None
    config: Optional[List[str]] = None
    eoqc: Optional[EOQCConfig] = None
    
    class Config:
        """Allow population by name"""
        populate_by_name = True


# Disable validation globally. Do we want this? If yes, uncomment the import of ConfigDict

#BaseModel.model_config = ConfigDict(validate_assignment=False, extra='allow', arbitrary_types_allowed=True)
