FROM cccs/assemblyline:stable

COPY --chown=assemblyline:assemblyline python-client /tmp/client
RUN pip install --user -U pip wheel build
RUN pip install --user /tmp/client

CMD ["python", "-m", "hauntedhouse.ingest", "config.json"]