FROM ubuntu:22.04 AS builder

# Prevent tzdata from asking for geographic location during install
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Set up the working directory
WORKDIR /app

# Copy the source code and build config
COPY CMakeLists.txt .
COPY src/ ./src/

# Compile the C++ application
RUN mkdir build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make

FROM ubuntu:22.04 AS runner

WORKDIR /app

# Copy ONLY the compiled binary from the builder stage
COPY --from=builder /app/build/mini-redis /usr/local/bin/mini-redis

# Expose the Redis port
EXPOSE 6379

# Run the server
ENTRYPOINT ["mini-redis"]