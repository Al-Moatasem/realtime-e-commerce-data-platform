# Flink Version: 2.2.0 # use "./bin/flink --version" in the container
FROM apache/flink:2.2-scala_2.12-java17

USER root

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    pip3 install apache-flink==2.2.0 --break-system-packages && \
    # create a symbolic link so 'python' command points to 'python3'
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN cp /opt/flink/opt/flink-python*.jar /opt/flink/lib/

USER flink
