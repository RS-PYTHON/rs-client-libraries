# Copyright 2023-2026 Airbus, CS Group
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

"""General utilities"""

import re
from os.path import commonprefix
from typing import Any


def search_by_name(values: list[dict[str, Any]], name: str) -> Any:
    """Return the first item whose "name" field matches the given value."""
    return next((obj for obj in values if obj.get("name") == name), None)


def get_common_and_relative_paths(paths: list[str]) -> tuple[str, list[str]]:
    """
    Returns the common folder prefix of the given paths and their escaped
    relative parts.

    The common prefix is truncated to the last `/` so it always represents
    a directory.

    Args:
        paths (list[str]): List of absolute paths or URIs.

    Returns:
        tuple[str, list[str]]:
            - Common folder ending with `/`
            - Escaped relative parts for each path
    """
    # Compute longest common prefix
    common_prefix = commonprefix(paths)
    # Ensure prefix stops on folder boundary
    common_folder = common_prefix[: common_prefix.rfind("/") + 1]
    # Build regex from relative names inside common folder
    relative_parts = [re.escape(path.removeprefix(common_folder)) for path in paths]
    return common_folder, relative_parts


# Pattern used in reading from logfile in run_processor()
# The main line:
# 2026-04-17 11:56:08 - module - DEBUG - message
# Compacted regex for debugging in any online website
# ^(?P<timestamp>\d{4}-\d{2}-\d{2}[^\n]*?)\s+-\s+(?P<logger>.*?)\s*(?:-\s+|:\s+)\[?(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\]?\s*(?:-\s+|:\s+)?(?P<message>.*)$
LOG_PATTERN = re.compile(
    r"""
    # positions itself at the start of the line
    ^
    # find timestamp
    (?P<timestamp>\d{4}-\d{2}-\d{2}[^\n]*?)
    # matches any whitespace with - in the middle
    \s+-\s+
    # find the module name (ex. eopf)
    (?P<logger>.*?)
    # find the logging level for both DEBUG and [DEBUG]
    \s*
    (?:
        -\s+
        |
        :\s+
    )

    \[?
    (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)
    \]?

    \s*
    (?:
        -\s+
        |
        :\s+
    )?
    # finds the log message
    (?P<message>.*)
    # assert position at end of the line
    $
    """,
    re.MULTILINE | re.VERBOSE,
)


def parse_logs(text):
    """
    Parse EOPF processor logs into structured log entries.
    The parser detects the beginning of a new log entry using the global LOG_PATTERN regex.

    Each yielded log entry contains:
    - timestamp
    - logger
    - level
    - message

    Text found before the first detected log entry is classified as an INFO-level message.
    """

    current = None
    # Stores lines found before the first valid log match
    preamble = []

    for line in text.splitlines():

        match = LOG_PATTERN.match(line)

        # If text exists before the first valid log entry, return INFO log entry
        if match:

            if preamble:

                yield {"timestamp": "", "logger": "system", "level": "INFO", "message": "\n".join(preamble)}

                # Reset buffer
                preamble = []

            if current:
                yield current

            current = {
                "timestamp": match.group("timestamp"),
                "logger": match.group("logger"),
                "level": match.group("level"),
                "message": match.group("message"),
            }

        else:

            if current is None:
                preamble.append(line)

            # multiline message
            else:
                current["message"] += "\n" + line

    if current:
        yield current

    elif preamble:
        yield {"timestamp": "", "logger": "system", "level": "INFO", "message": "\n".join(preamble)}
