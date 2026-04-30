FROM rust:1.95-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY qscheduler/ qscheduler/
COPY runner/ runner/
COPY service/ service/

RUN cargo build --release -p qscheduler

FROM debian:bookworm-slim

COPY --from=builder /build/target/release/qscheduler /usr/local/bin/qscheduler

ENTRYPOINT ["qscheduler"]
