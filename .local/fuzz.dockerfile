FROM rust:1-slim-buster
WORKDIR /app
RUN rustup install nightly && \
    rustup default nightly && \
    cargo install cargo-fuzz
RUN apt update && \
    apt install -y build-essential
COPY . .

ENTRYPOINT ["cargo", "fuzz", "run", "fuzz_path"]