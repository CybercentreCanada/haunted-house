FROM rust:1.67-bullseye AS builder

# Add more build tools
RUN apt-get update && apt-get install -yy libclang-dev

# Copy in the source to build
WORKDIR /usr/src/haunted-house
COPY ./src ./src
COPY Cargo.lock Cargo.toml ./

# Build the executable
RUN cargo build --release --target-dir /out/
RUN test -f "/out/release/haunted-house"

# Start over with an empty image
FROM debian:bullseye-slim

# Get required apt packages
# RUN apt-get update && apt-get install -yy libssl1.1 && rm -rf /var/lib/apt/lists/*
# (Already installed on this base image)
RUN apt-get update && apt-get install -yy ca-certificates && rm -rf /var/lib/apt/lists/*

# lock root
RUN passwd -l root

# add a non root user
RUN useradd -b /home -U -m user
WORKDIR /home/user
USER user

# Copy in the executable for this container
COPY --from=builder /out/release/haunted-house /usr/bin/haunted-house
RUN ulimit -n 262144
CMD ["/usr/bin/haunted-house"]