FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DEADMAN_STATE_DIR=/data

WORKDIR /app

# No third-party dependencies: the package is stdlib-only on purpose, so the
# image stays tiny and there is no supply chain to audit.
COPY deadman ./deadman
COPY README.md ./

RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin deadman \
    && mkdir -p /data \
    && chown -R deadman:deadman /app /data \
    && chmod 700 /data

USER deadman

VOLUME ["/data"]

# The daemon itself is the health check: `status` reads state without requiring
# a token and exits 0 even when no state exists yet.
HEALTHCHECK --interval=5m --timeout=20s --start-period=60s --retries=3 \
    CMD ["python", "-m", "deadman", "status"]

ENTRYPOINT ["python", "-m", "deadman"]
CMD ["run"]
