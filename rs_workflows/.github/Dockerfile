
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
