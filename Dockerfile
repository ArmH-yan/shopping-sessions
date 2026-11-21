# Shopping Sessions: a Rust pipeline, with the DuckDB pipeline as a fallback.
# Stage 1 builds the Rust binary; stage 2 is the Python image from the starter plus that binary.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY fast/Cargo.toml fast/Cargo.lock ./
COPY fast/src ./src
# x86-64-v3 (AVX2) runs on every Cascade Lake or newer Xeon the scorer uses.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=build /src/target/release/shopping /app/shopping
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/shopping
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/app/run.sh"]
