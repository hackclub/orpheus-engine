ARG BASE_IMAGE=postgres:17-alpine

## Custom Alpine Postgres docker file with pg_xxhash extension
FROM ${BASE_IMAGE} AS builder

# Install required dependencies for building the extension
RUN apk --no-cache add \
    make \
    gcc \
    g++ \
    clang19 \
    llvm19 \
    postgresql-dev

# Copy the vendored pg_xxhash source
COPY vendor/pg_xxhash /xxhash

WORKDIR /xxhash

# Build and install the extension
RUN make && make install

## Final image - start fresh from base
FROM ${BASE_IMAGE}

## Copy all extensions from the builder stage
COPY --from=builder /usr/local/lib/postgresql/* /usr/local/lib/postgresql/
COPY --from=builder /usr/local/share/postgresql/extension/* /usr/local/share/postgresql/extension/

