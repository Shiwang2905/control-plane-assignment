# One image, two roles: the control plane (`control-plane`) and the worker
# simulator (`worker-sim`). The `dev` stage adds test/lint tooling.

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
# Install, then drop the build/ leftovers so only real sources remain in /app.
RUN pip install . && rm -rf build src/*.egg-info

# Never run as root inside the container.
RUN useradd --create-home --uid 10001 app
USER app
EXPOSE 8000
CMD ["control-plane"]

FROM runtime AS dev
USER root
RUN pip install -e ".[dev]" && rm -rf build
COPY tests ./tests
COPY scripts ./scripts
RUN chown -R app /app
USER app
CMD ["sh", "scripts/check.sh"]
