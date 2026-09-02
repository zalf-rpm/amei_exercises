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

# install pixi
RUN curl -fsSL https://pixi.sh/install.sh | sh
ENV PATH="/root/.pixi/bin:${PATH}"

WORKDIR /workspace

# sibling repos amei_exercises' editable pypi-dependencies expect at ../mas_python_fbp
# and ../mas_python_common
RUN git clone --depth 1 --branch "${MAS_PYTHON_FBP_REF}" https://github.com/zalf-rpm/mas_python_fbp.git
RUN git clone --depth 1 --branch "${MAS_PYTHON_COMMON_REF}" https://github.com/zalf-rpm/mas_python_common.git

COPY . /workspace/amei_exercises
WORKDIR /workspace/amei_exercises

RUN pixi install

ENTRYPOINT ["pixi", "run"]
CMD ["maricopa"]
