FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates cmake ninja-build && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY include include
COPY src src
COPY redis redis
RUN cmake -S . -B /build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
    && cmake --build /build --parallel 2

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/aegisgate /usr/local/bin/aegisgate
COPY config/compose.conf /etc/aegisgate/gateway.conf
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/aegisgate", "/etc/aegisgate/gateway.conf"]
