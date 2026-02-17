ARG BASE_IMAGE=postgres:17-alpine

## Custom Alpine Postgres docker file with pg_rowhash extension
## pg_rowhash: Fast row hashing using xxHash - hashes binary tuples directly
FROM ${BASE_IMAGE} AS builder

# Install required dependencies for building the extension
RUN apk --no-cache add \
    make \
    gcc \
    g++ \
    clang19 \
    llvm19 \
    postgresql-dev

# Copy pg_rowhash source
COPY pg_rowhash /rowhash

WORKDIR /rowhash

# Build and install pg_rowhash
RUN make && make install

## Final image - start fresh from base
FROM ${BASE_IMAGE}

## Copy all extensions from the builder stage
COPY --from=builder /usr/local/lib/postgresql/* /usr/local/lib/postgresql/
COPY --from=builder /usr/local/share/postgresql/extension/* /usr/local/share/postgresql/extension/

# Add pgbackrest client for WAL archiving
RUN apk add --no-cache pgbackrest

# Configure PostgreSQL for WAL archiving on first init
COPY conf.sh /docker-entrypoint-initdb.d/conf.sh
