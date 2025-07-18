# Copyright 2024 CS Group
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

"""Configuration objects and functions."""

from enum import Enum

#############
# Constants #
#############

# datetime format used for HTTP requests
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FORMAT_MS = "%Y-%m-%dT%H:%M:%S.%fZ"

# name of the Auxip station, used in the same logic as the ECadipStation enum (see below)
AUXIP_STATION = "AUXIP"

#########
# Enums #
#########


class EPlatform(str, Enum):
    """Platform enumeration."""

    S1A = "S1A"
    S2A = "S2A"
    S2B = "S2B"
    S3A = "S3A"
    S3B = "S3B"


class ECadipStation(str, Enum):
    """Cadip stations enumeration."""

    CADIP = "CADIP"
    INS = "INS"
    MPS = "MPS"
    MTI = "MTI"
    NSG = "NSG"
    SGS = "SGS"


class EAuxipStation(str, Enum):
    """Auxip stations enumeration."""

    ADGS = "ADGS"
    ADGS2 = "ADGS2"


class EDownloadStatus(str, Enum):
    """Download status enumeration."""

    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    DONE = "DONE"
