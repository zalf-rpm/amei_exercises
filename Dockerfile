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
FROM debian:13 AS build

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

COPY --from=zalfrpm/monica-cluster:3.6.51 /run/monica /usr/local/bin

COPY . /workspace/amei_exercises
WORKDIR /workspace/amei_exercises

RUN pixi install --locked
RUN pixi run regenerate-components-cache
RUN pixi shell-hook > /shell-hook.sh && echo 'exec "$@"' >> /shell-hook.sh && chmod +x /shell-hook.sh

# Run as a non-root user; the service rewrites its components cache at startup, so
# the configs directory (where the cache lives) must stay writable for it.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /workspace/amei_exercises/configs
USER appuser

EXPOSE 9990

FROM build AS dev
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]
CMD ["python", "-u", "-m", "zalfmas_fbp.run.local_components_service", "./configs/local_components_service.toml"]

FROM build AS prod
ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]
CMD ["python", "-u", "-m", "zalfmas_fbp.run.local_components_service", "./configs/local_components_service.toml"]
