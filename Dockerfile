FROM python:3.12-alpine

COPY exporter.py /app/exporter.py

EXPOSE 9840

# /healthz only proves the HTTP server is alive - it does not touch the databases.
# Shell form so LISTEN_PORT is substituted.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q --spider "http://127.0.0.1:${LISTEN_PORT:-9840}/healthz" || exit 1

CMD ["python", "/app/exporter.py"]
