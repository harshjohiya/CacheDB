import asyncio
import random
import string
import time
import os

# Configuration
HOST = os.getenv("REDIS_HOST", "127.0.0.1")
PORT = 6379
TOTAL_REQUESTS = 100000
CONCURRENCY = 50
PIPELINE_SIZE = 32

# Pre-generated value pool — avoids random.choices() on every SET
_VALUE_POOL = [
    ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    for _ in range(1024)
]

def random_value() -> str:
    return _VALUE_POOL[random.randint(0, len(_VALUE_POOL) - 1)]

def encode_resp(*args) -> bytes:
    """Encode arguments as a RESP array."""
    resp = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        resp += f"${len(arg_str)}\r\n{arg_str}\r\n"
    return resp.encode()

def build_payload(is_set: bool, key: str) -> bytes:
    if is_set:
        return encode_resp("SET", key, random_value())
    return encode_resp("GET", key)

async def read_response(reader: asyncio.StreamReader) -> None:
    """Consume one RESP response from the stream."""
    response = await reader.readline()
    if response.startswith(b'$') and response != b'$-1\r\n':
        await reader.readline()

# Pass 1 - per-request latency (one drain per command, accurate timings)
async def worker_latency(worker_id: int, requests_per_worker: int, latencies: list):
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
    except Exception as e:
        print(f"  [latency] Worker {worker_id} failed to connect: {e}")
        return

    for _ in range(requests_per_worker):
        is_set = random.random() < 0.2
        key = f"key:{random.randint(1, 1000)}"
        payload = build_payload(is_set, key)

        # Timer starts before write so drain() time is included
        start = time.perf_counter()
        writer.write(payload)
        await writer.drain()
        await read_response(reader)
        latencies.append(time.perf_counter() - start)

    writer.close()
    await writer.wait_closed()

# Pass 2 - pipelined throughput (batch flush, no per-request timing)
async def worker_pipeline(worker_id: int, requests_per_worker: int):
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
    except Exception as e:
        print(f"  [pipeline] Worker {worker_id} failed to connect: {e}")
        return 0

    completed = 0
    i = 0
    while i < requests_per_worker:
        batch = min(PIPELINE_SIZE, requests_per_worker - i)

        # Write a full batch before draining
        for _ in range(batch):
            is_set = random.random() < 0.2
            key = f"key:{random.randint(1, 1000)}"
            writer.write(build_payload(is_set, key))

        await writer.drain()

        # Read all responses for this batch
        for _ in range(batch):
            await read_response(reader)

        completed += batch
        i += batch

    writer.close()
    await writer.wait_closed()
    return completed

# Stats helper
def print_stats(label: str, duration: float, latencies: list):
    count = len(latencies)
    ops = count / duration if duration > 0 else 0
    print(f"\n--- {label} ---")
    print(f"Total time:    {duration:.2f}s")
    print(f"Requests done: {count}")
    print(f"Throughput:    {ops:.0f} ops/sec")
    if latencies:
        sl = sorted(latencies)
        avg = sum(sl) / count
        p50 = sl[int(count * 0.50)] * 1000
        p95 = sl[int(count * 0.95)] * 1000
        p99 = sl[int(count * 0.99)] * 1000
        print(f"Avg latency:   {avg * 1000:.3f} ms")
        print(f"p50 latency:   {p50:.3f} ms")
        print(f"p95 latency:   {p95:.3f} ms")
        print(f"p99 latency:   {p99:.3f} ms")

# Main
async def main():
    print(f"Benchmark target: {HOST}:{PORT}")
    print(f"Total requests: {TOTAL_REQUESTS} | Concurrency: {CONCURRENCY} | Pipeline size: {PIPELINE_SIZE}")

    rpw = TOTAL_REQUESTS // CONCURRENCY

    # Pass 1: latency
    print(f"\n[1/2] Latency pass (per-request drain)...")
    latencies: list = []
    t0 = time.perf_counter()
    await asyncio.gather(*[worker_latency(i, rpw, latencies) for i in range(CONCURRENCY)])
    print_stats("Latency Pass Results", time.perf_counter() - t0, latencies)

    # Brief pause between passes
    await asyncio.sleep(0.5)

    # Pass 2: pipelined throughput
    print(f"\n[2/2] Throughput pass (pipeline size={PIPELINE_SIZE})...")
    t0 = time.perf_counter()
    results = await asyncio.gather(*[worker_pipeline(i, rpw) for i in range(CONCURRENCY)])
    duration = time.perf_counter() - t0
    total_done = sum(results)
    ops = total_done / duration if duration > 0 else 0
    print(f"\n--- Throughput Pass Results ---")
    print(f"Total time:    {duration:.2f}s")
    print(f"Requests done: {total_done}")
    print(f"Throughput:    {ops:.0f} ops/sec")
    print(f"(No per-request latency stats in pipelined mode)")

if __name__ == "__main__":
    asyncio.run(main())
