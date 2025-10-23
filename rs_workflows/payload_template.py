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

from typing import Optional, Union
from pydantic import BaseModel, Field, field_validator#, Configdict

class BasePayloadModel(BaseModel):
    """Base class shared by all the schema models"""
    class Config:
        """Configuration for pydantic"""
        # Allow using field names even when aliases are set
        populate_by_name = True
        # Optional: disable validation errors
        validate_assignment = False
        arbitrary_types_allowed = True
        extra = "allow"  # ignore unknown fields
        
    def dump(self, **kwargs):
        """Custom dump that:
         - skips None fields by default.
         - skips all unset
         - use the alias for fields by default
         """
        return self.model_dump(by_alias=True, 
                               exclude_none=True, 
                               exclude_unset=True,
                               **kwargs)
# Utility / Common classes

class StorageOptions(BasePayloadModel):
    """Options to access a storage backend"""
    key: Optional[str] = None
    secret: Optional[str] = None
    client_kwargs: Optional[dict[str, str]] = None

class StoreOptionsWrapper(BasePayloadModel):
    """Wrapper for a list of storage options"""
    storage_options: list[StorageOptions]

class StoreParams(BasePayloadModel):
    """Flexible store_params representation for payloads"""
    # Either a simple S3 secret alias
    s3_secret_alias: Optional[str] = None
    # Or a list of storage options
    options: Optional[list[StoreOptionsWrapper]] = None
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

class LoggingConfig(BasePayloadModel):
    level: Optional[str] = Field(default="INFO", description="Logging level")
    
# Main sections

class GeneralConfiguration(BasePayloadModel):
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

class ExternalModule(BasePayloadModel):
    """Definition of an external module to import dynamically"""
    name: str
    alias: Optional[str] = None
    nested: Optional[bool] = None
    folder: Optional[str] = None


class Breakpoints(BasePayloadModel):
    """Configuration for debugging breakpoints"""
    activate_all: Optional[bool] = None
    folder: Optional[str] = None
    store_params: Optional[StoreParams] = None
    ids: Optional[list[str]] = None

class WorkflowStep(BasePayloadModel):
    """Definition of a workflow step (processing unit)"""
    name: str
    active: Optional[bool] = True
    validate_output: Optional[bool] = Field(True, alias="validate")
    step: Optional[int] = None    
    module: Optional[str] = None
    processing_unit: Optional[str] = None
    inputs: Optional[list[dict[str, str]]] = None
    outputs: Optional[list[dict[str, str]]] = None
    adfs: Optional[list[dict[str, str]]] = None
    parameters: Optional[dict[
        str,
        Union[
            str,
            int,
            float,
            bool,
            list[int],
            list[str],
        ],
    ]] = None


class InputProduct(BasePayloadModel):
    """Definition of an input product in the I/O configuration"""
    id: str
    path: str
    type: Optional[str] = Field(default="filename")
    store_type: str
    store_params: Optional[StoreParams] = None


class OutputProduct(BasePayloadModel):
    """Definition of an output product in the I/O configuration"""
    id: str
    path: str
    store_type: str
    store_params: Optional[StoreParams] = None
    type: Optional[str] = Field(default="filename")
    opening_mode: Optional[str] = Field(default="CREATE")
    apply_eoqc: Optional[bool] = Field(default=False)


class AdfConfig(BasePayloadModel):
    """Definition of an ADF configuration entry"""
    id: str
    path: str
    store_params: Optional[StoreParams] = None


class IOConfig(BasePayloadModel):
    """Input/output configuration"""
    input_products: list[InputProduct] = []
    output_products: list[OutputProduct] = []
    adfs: list[AdfConfig] = []


class DaskContext(BasePayloadModel):
    """Configuration for the DaskContext"""
    cluster_type: Optional[str] = None
    cluster_config: Optional[dict[str, Union[str, int, bool]]] = None
    client_config: Optional[dict[str, Union[str, int, bool]]] = None
    dask_config: Optional[dict[str, Union[str, int, bool]]] = None
    performance_report_file: Optional[str] = None


class EOQCConfig(BasePayloadModel):
    """Configuration for the EOQC processor"""
    config_folder: Optional[str] = Field(default="default")
    parameters: Optional[dict[str, Union[str, int, float, bool]]] = Field(default_factory=dict)
    update_attrs: Optional[bool] = Field(default=True)
    report_path: Optional[str] = None
    config_path: Optional[str] = None
    additional_config_folders: Optional[list[str]] = None


# Root payload model

class PayloadSchema(BasePayloadModel):
    """Root payload schema containing all configuration sections"""
    dotenv: Optional[list[str]] = None
    general_configuration: Optional[GeneralConfiguration] = None
    external_modules: Optional[list[ExternalModule]] = None
    breakpoints: Optional[Breakpoints] = None
    workflow: Optional[list[WorkflowStep]] = None
    io: Optional[IOConfig] = Field(None, alias="I/O")
    dask_context: Optional[DaskContext] = None
    logging: Optional[list[str]] = None
    config: Optional[list[str]] = None
    eoqc: Optional[EOQCConfig] = None
    
    class Config:
        """Allow population by name"""
        populate_by_name = True


# Disable validation globally. Do we want this? If yes, uncomment the import of Configdict

#BasePayloadModel.model_config = Configdict(validate_assignment=False, extra='allow', arbitrary_types_allowed=True)
