# Chronicle Unit Dockerfile
#   docker build -t chronicle-unit:dev .

FROM rust:1.92

RUN apt-get update && apt-get install -y protobuf-compiler clang libclang-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY catalog/ catalog/
COPY chronicled/ chronicled/
COPY chronicles/ chronicles/

RUN cargo build --release -p chronicle-cli

RUN cp target/release/chronicle /usr/local/bin/chronicle && \
    rm -rf /build/target

COPY chronicled.toml /etc/chronicle/chronicled.toml

RUN mkdir -p /data/wal /data/storage /data/segments

EXPOSE 7070 7071

ENTRYPOINT ["chronicle"]
CMD ["unit", "start", "--config", "/etc/chronicle/chronicled.toml"]
