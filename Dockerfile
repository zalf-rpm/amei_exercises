# Generic runtime for running an FBP flow built on amei_exercises/zalfmas_fbp components.
#
# amei_exercises' own dependencies (zalfmas-fbp, zalfmas-common, ...) come straight from
# PyPI (see pyproject.toml), so this image only ever needs amei_exercises itself - no sibling
# repos to clone. It provides the Python components; the actual flow/config/data is expected
# to live outside the image on a writable filesystem (e.g. a fresh amei_exercises checkout,
# or any other project shaped like it) and be pointed at via `flow -e /path/to/run_config.toml`
# at `docker run`/`singularity run` time - see run_config.toml's own header comment, and
# run_fbp_flow.py's '${FLOW_DIR}'/'${PKG:<package>}' placeholders for making that config
# portable regardless of where it's checked out or how its dependencies were installed.
FROM debian:13

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
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
#
# CMD is just the default task name, not baked-in args - override it (and/or append
# "-e /path/to/run_config.toml") at `docker run`/`singularity run` time to run any flow.
ENTRYPOINT ["pixi", "run", "--manifest-path", "/workspace/amei_exercises/pyproject.toml", "--as-is"]
CMD ["flow"]
