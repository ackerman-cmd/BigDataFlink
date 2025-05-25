FROM flink:1.19.2-java17

USER root
RUN apt-get update && \
    apt-get install -y netcat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER flink
