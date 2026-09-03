# Runs the amei_exercises "maricopa" pixi task in a container.
#
# amei_exercises depends on mas_python_fbp and mas_python_common as editable pypi
# dependencies via relative paths (see pyproject.toml: path = "../mas_python_fbp" /
# "../mas_python_common"), so those two sibling repos are cloned here to match that
# layout. amei_exercises itself comes from the build context (not a fresh clone), so
# the image reflects whatever commit/PR actually triggered the build.
#
# Known limitation: the "maricopa" task's --path_to_channel currently points at a
# Windows channel.exe built from a 4th, separate repo (monica), which is not part of
# this image. Override that argument (e.g. via `docker run ... pixi run maricopa
# --path_to_channel=/path/to/channel`) once a Linux-built channel binary is available.
FROM debian:13

ARG MAS_PYTHON_FBP_REF=main
ARG MAS_PYTHON_COMMON_REF=main

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl git && \
    rm -rf /var/lib/apt/lists/*

# Install pixi directly under /usr/local, which is already part of the base image's
# default PATH. Do NOT let it land under /root: some runtimes (notably Singularity/
# Apptainer) run the container as the invoking host user rather than root, even
# though the image itself was built as root, and /root defaults to mode 700 (owner
# only) - a non-root user can't traverse into it at all, no matter what PATH or
# symlinks point there. PIXI_HOME controls where the installer places the binary.
ENV PIXI_HOME=/usr/local
RUN curl -fsSL https://pixi.sh/install.sh | sh && \
    test -x /usr/local/bin/pixi

WORKDIR /workspace

# sibling repos amei_exercises' editable pypi-dependencies expect at ../mas_python_fbp
# and ../mas_python_common
RUN git clone --depth 1 --branch "${MAS_PYTHON_FBP_REF}" https://github.com/zalf-rpm/mas_python_fbp.git
RUN git clone --depth 1 --branch "${MAS_PYTHON_COMMON_REF}" https://github.com/zalf-rpm/mas_python_common.git

COPY . /workspace/amei_exercises
WORKDIR /workspace/amei_exercises

RUN pixi install

# Use an absolute --manifest-path rather than relying on the container's working
# directory: Docker starts new processes at the image's WORKDIR, but Singularity/
# Apptainer instead carries over the *host's* current directory into the container
# by default, so a bare `pixi run` there looks for pyproject.toml in the wrong place.
#
# --as-is (= --frozen --no-install) stops `pixi run` from trying to verify/update
# the environment before running the task, which by default requires acquiring a
# write lock on .pixi/envs/default. The environment was already fully installed
# above during the build and never changes afterwards, so there's nothing to check
# or install at runtime - but the environment can be running from a read-only
# filesystem (e.g. a Singularity/Apptainer SIF image), where even that check-only
# lock acquisition fails outright.
ENTRYPOINT ["pixi", "run", "--manifest-path", "/workspace/amei_exercises/pyproject.toml", "--as-is"]
CMD ["maricopa"]
