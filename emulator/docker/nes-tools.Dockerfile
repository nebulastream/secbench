ARG BASE_IMAGE=nebulastream/worker:tt-tcp-sink
FROM ${BASE_IMAGE}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash iproute2 net-tools procps iputils-ping coreutils \
    && rm -rf /var/lib/apt/lists/*
