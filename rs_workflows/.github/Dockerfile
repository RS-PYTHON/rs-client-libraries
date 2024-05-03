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


#
# Create a docker image with no services that is just used to store .whl package files
#

FROM python:3.11-slim-bookworm

# The CI builds the wheel packages, downloads them into ./whl/, then builds this Dockerfile from ./whl/,
# so the .whl files are under ./*.whl
RUN mkdir -p /dist/whl
ADD ./*.whl /dist/whl

# Add a default user
RUN useradd -m user
USER user
WORKDIR /home/user

ENTRYPOINT [ "/bin/bash", "-c", "echo -e \".whl pacakges are stored under:\n$(realpath /dist/whl/*.whl)\"" ]
