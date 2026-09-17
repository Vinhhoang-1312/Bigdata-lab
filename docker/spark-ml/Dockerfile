FROM apache/spark:4.2.0-python3

USER root

RUN python3 -m pip install --no-cache-dir \
    numpy==2.2.6 \
    pandas==2.3.3 \
    PyArrow==21.0.0 \
    PyYAML==6.0.3

RUN python3 -m pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    torch==2.8.0

RUN python3 -m pip install --no-cache-dir boto3==1.40.46
