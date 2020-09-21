FROM python:3.8-slim-buster

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && \
    rm -rf /root/.cache/pip

COPY run.py /flywheel/v0/run.py
COPY utils /flywheel/v0/utilspip
# Configure entrypoint
ENTRYPOINT ["/bin/bash"]
