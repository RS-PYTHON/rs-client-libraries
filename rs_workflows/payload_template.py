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

from typing import cast

from pydantic import (  # , Configdict
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

DEFAULT_CLUSTER_CONFIG = cast(
    dict[str, str | int | bool],
    {
        "n_workers": 6,
        "threads_per_worker": 1,
    },
)

DEFAULT_DASK_CONFIG = cast(
    dict[str, str | int | bool],
    {
        "distributed.worker.local_directory": "~/eopf/output",
    },
)


class BasePayloadModel(BaseModel):
    """Base class shared by all the schema models"""

    model_config = ConfigDict(
        # Allow using field names even when aliases are set
        populate_by_name=True,
        # Optional: disable validation errors
        validate_assignment=False,
        arbitrary_types_allowed=True,
        extra="allow",  # ignore unknown fields
    )

    @model_validator(mode="before")
    def fill_defaults(cls, values):  # pylint: disable=no-self-argument
        """Ensure defaults and nested models are properly initialized.
        Thus, when creating any model, the defaults should be written in the payload
        IF not provided and IF a default (except NONE) exists
        """
        if not isinstance(values, dict):
            return values
        # Pydantic v2/v3-safe
        # follow the official Pydantic v2 recommended access pattern
        # https://docs.pydantic.dev/latest/migration/#model-fields
        # https://docs.pydantic.dev/latest/migration/#validator-and-root_validator-are-deprecated
        # So normally this should work in Pydantic v3. But it seems it isn't working in v2
        # NOTE when Pydantic v3 is released, we should be able to use:
        # fields = type(cls).model_fields
        # and then iterate over fields.items() instead of cls.model_fields.items()
        for name, field in cls.model_fields.items():
            # Value missing → set default
            if name not in values:
                values[name] = field.get_default(call_default_factory=True)
                continue

            # Value is explicitly None → also replace with default
            if values[name] is None:
                values[name] = field.get_default(call_default_factory=True)

        return values

    def dump(self, **kwargs):
        """Custom dump that:
        - skips None fields by default.
        - skips all unset
        - use the alias for fields by default
        """
        return self.model_dump(by_alias=True, exclude_none=True, exclude_unset=True, **kwargs)


# Utility / Common classes


class StorageOptions(BasePayloadModel):
    """Options to access a storage backend"""

    name: str
    key: str | None = None
    secret: str | None = None
    client_kwargs: dict[str, str] | None = None


# class StoreOptionsWrapper(BasePayloadModel):
#     """Wrapper for a list of storage options"""

#     storage_options: list[StorageOptions]


class StoreParams(BasePayloadModel):
    """Flexible store_params representation for payloads"""

    # Either a simple S3 secret alias
    s3_secret_alias: str | None = None
    # Or a list of storage options
    # options: list[StoreOptionsWrapper] | None = None
    storage_options: list[StorageOptions] | None = None
    # Or a regex + multiplicity
    regex: str | None = None
    multiplicity: str | int | None = None

    @field_validator("multiplicity")
    @classmethod
    def validate_multiplicity(cls, v):
        """Validation of multiplicity field"""
        if v is None:
            return v
        if isinstance(v, str) and v not in {"exactly_one", "at_least_one", "more_than_one"}:
            raise ValueError('multiplicity must be "exactly_one", "at_least_one", "more_than_one" or an integer')
        if not isinstance(v, (str, int)):
            raise ValueError("multiplicity must be a string or an integer")
        return v

    # @classmethod
    # def from_dict(cls, data):
    #     """Helper to parse from dict-like YAML structure"""
    #     if isinstance(data, dict):
    #         if "s3_secret_alias" in data:
    #             return cls(s3_secret_alias=data["s3_secret_alias"])
    #         if "regex" in data or "multiplicity" in data:
    #             return cls(regex=data.get("regex"), multiplicity=data.get("multiplicity"))
    #     elif isinstance(data, list):
    #         wrappers = [StoreOptionsWrapper(**item) for item in data]
    #         return cls(options=wrappers)
    #     raise ValueError("Invalid store_params format")


class LoggingConfig(BasePayloadModel):
    """Logging configuration used in the general_configuration section"""

    level: str | None = Field(default="INFO", description="Logging level")


# Main sections


class GeneralConfiguration(BasePayloadModel):
    """General configuration options for EOConfiguration behavior"""

    logging: LoggingConfig | None = LoggingConfig(level="DEBUG")
    triggering__use_basic_logging: bool | None = True
    triggering__wait_before_exit: int | None = 10
    dask__export_graphs: str | None = None
    breakpoints__folder: str | None = None
    triggering__create_temporary: bool | None = None
    triggering__temporary_shared: bool | None = None
    triggering__validate_run: bool | None = None
    triggering__validate_mode: str | None = None
    triggering__error_policy: str | None = None
    temporary__folder: str | None = None
    temporary__folder_s3_secret: str | None = None
    temporary__folder_create_folder: bool | None = None
    triggering__dask_monitor__enabled: bool | None = None
    triggering__dask_monitor__cancel: bool | None = None
    triggering__dask_monitor__cancel_state: str | None = None


class ExternalModule(BasePayloadModel):
    """Definition of an external module to import dynamically"""

    name: str
    alias: str | None = None
    nested: bool | None = None
    folder: str | None = None


class Breakpoints(BasePayloadModel):
    """Configuration for debugging breakpoints"""

    activate_all: bool | None = None
    folder: str | None = None
    store_params: StoreParams | None = None
    ids: list[str] | None = None


class WorkflowStep(BasePayloadModel):
    """Definition of a workflow step (processing unit)"""

    name: str
    active: bool | None = True
    validate_output: bool | None = Field(default=None, alias="validate")
    step: int | None = None
    module: str | None = None
    processing_unit: str | None = None
    inputs: list[dict[str, str]] | None = None
    outputs: list[dict[str, str]] | None = None
    adfs: list[dict[str, str]] | None = None
    parameters: None | (
        dict[
            str,
            (str | int | float | bool | list[int] | list[str]),
        ]
    ) = None


class InputProduct(BasePayloadModel):
    """Definition of an input product in the I/O configuration"""

    id: str
    path: str
    type: str | None = Field(default="filename")
    store_type: str
    store_params: StoreParams | None = None


class OutputProduct(BasePayloadModel):
    """Definition of an output product in the I/O configuration"""

    id: str
    path: str
    store_type: str
    store_params: StoreParams | None = None
    type: str | None = Field(default="filename")
    opening_mode: str | None = Field(default="CREATE")
    apply_eoqc: bool | None = Field(default=False)


class AdfConfig(BasePayloadModel):
    """Definition of an ADF configuration entry"""

    id: str
    path: str
    store_params: StoreParams | None = None


class IOConfig(BasePayloadModel):
    """Input/output configuration"""

    input_products: list[InputProduct] = []
    output_products: list[OutputProduct] = []
    adfs: list[AdfConfig] = []


class DaskContext(BasePayloadModel):
    """Configuration for the DaskContext"""

    cluster_type: str | None = "local"  # Optional but if not available "address" is mandatory
    address: str | None = None
    cluster_config: dict[str, str | int | bool] | None = DEFAULT_CLUSTER_CONFIG
    client_config: dict[str, str | int | bool] | None = {}
    dask_config: dict[str, str | int | bool] | None = DEFAULT_DASK_CONFIG
    performance_report_file: str | None = "report.html"


class EOQCConfig(BasePayloadModel):
    """Configuration for the EOQC processor"""

    config_folder: str | None = Field(default="default")
    parameters: dict[str, str | int | float | bool] | None = Field(default_factory=dict)
    update_attrs: bool | None = Field(default=True)
    report_path: str | None = None
    config_path: str | None = None
    additional_config_folders: list[str] | None = None


# Root payload model


class PayloadSchema(BasePayloadModel):
    """Root payload schema containing all configuration sections"""

    dotenv: list[str] | None = None
    general_configuration: GeneralConfiguration | None = None
    external_modules: list[ExternalModule] | None = None
    breakpoints: Breakpoints | None = None
    workflow: list[WorkflowStep] | None = None
    io: IOConfig | None = Field(None, alias="I/O")
    dask_context: DaskContext | None = None
    logging: str | None = None
    config: list[str] | None = None
    eoqc: EOQCConfig | None = None


# Disable validation globally. Do we want this? If yes, uncomment the import of Configdict

# BasePayloadModel.model_config = Configdict(validate_assignment=False, extra='allow', arbitrary_types_allowed=True)
