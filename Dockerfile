FROM rust:1.72.0 AS builder
RUN apt-get update && apt-get install protobuf-compiler -y
WORKDIR /firehose-grpc
COPY ./ .
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /firehose-grpc
COPY --from=builder /firehose-grpc/target/release/firehose-grpc ./firehose-grpc
ENTRYPOINT ["/firehose-grpc/firehose-grpc"]
EXPOSE 3000
