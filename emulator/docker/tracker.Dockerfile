FROM gcc:12-bookworm AS build
WORKDIR /src
COPY result-tracker/rtracker.cpp .
RUN g++ -std=c++17 -O3 -pthread rtracker.cpp -o rtracker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 bash iproute2 net-tools procps iputils-ping coreutils \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/rtracker /usr/local/bin/rtracker
WORKDIR /app
