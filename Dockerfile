FROM rust:1.81.0 AS builder
RUN apt-get update && apt-get install protobuf-compiler -y
WORKDIR /firehose-grpc
COPY ./ .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /firehose-grpc
COPY --from=builder /firehose-grpc/target/release/firehose-grpc ./firehose-grpc
ENTRYPOINT ["/firehose-grpc/firehose-grpc"]
EXPOSE 13042
