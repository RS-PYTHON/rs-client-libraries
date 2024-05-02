"""Configuration objects and functions."""

from enum import Enum

#########
# ENums #
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


class EDownloadStatus(str, Enum):
    """Download status enumeration."""

    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    DONE = "DONE"
