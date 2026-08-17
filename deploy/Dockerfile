FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p registryd -p storectl

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/registryd /usr/local/bin/registryd
COPY --from=build /src/target/release/storectl /usr/local/bin/storectl
VOLUME /var/lib/registryd
EXPOSE 8080/tcp
# iroh binds a dynamic UDP port; run with host networking (compose file)
# so peer-to-peer traffic is not NATed twice.
ENV REGISTRYD_DATA_DIR=/var/lib/registryd
ENTRYPOINT ["/usr/local/bin/registryd"]
CMD ["run"]
