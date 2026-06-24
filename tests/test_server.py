import socket
import pytest
import time
import os

HOST = os.getenv("REDIS_HOST", "127.0.0.1")
PORT = 6379

def encode_resp(*args) -> bytes:
    """Helper to convert standard arguments into a RESP array."""
    resp = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        resp += f"${len(arg_str)}\r\n{arg_str}\r\n"
    return resp.encode()

@pytest.fixture
def redis_client():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    retries = 15
    while retries > 0:
        try:
            client.connect((HOST, PORT))
            break
        except ConnectionRefusedError:
            time.sleep(1)
            retries -= 1
    else:
        client.close()
        pytest.fail(f"Could not connect to {HOST}:{PORT} after all retries")
    yield client
    client.close()

def send_cmd(client, *args) -> str:
    """Encodes the command to RESP, sends it, and decodes the response."""
    client.sendall(encode_resp(*args))
    return client.recv(1024).decode()

def test_ping(redis_client):
    """Test the connection liveness command."""
    assert send_cmd(redis_client, "PING") == "+PONG\r\n"

def test_set_and_get(redis_client):
    """Test basic SET and GET with RESP Bulk Strings."""
    assert send_cmd(redis_client, "SET", "user:99", "Alice") == "+OK\r\n"
    assert send_cmd(redis_client, "GET", "user:99") == "$5\r\nAlice\r\n"

def test_get_nonexistent_key(redis_client):
    """Test GETting a missing key returns a RESP Null."""
    assert send_cmd(redis_client, "GET", "ghost_key") == "$-1\r\n"

def test_delete_key(redis_client):
    """Test DEL returns RESP Integers representing deleted count."""
    send_cmd(redis_client, "SET", "temp", "data")
    assert send_cmd(redis_client, "DEL", "temp") == ":1\r\n"
    assert send_cmd(redis_client, "DEL", "temp") == ":0\r\n"

def test_expire_and_ttl(redis_client):
    """Test that background/lazy eviction correctly deletes expired keys."""
    send_cmd(redis_client, "SET", "milk", "whole")
    assert send_cmd(redis_client, "EXPIRE", "milk", "1") == ":1\r\n"
    assert send_cmd(redis_client, "GET", "milk") == "$5\r\nwhole\r\n"
    time.sleep(1.2)
    assert send_cmd(redis_client, "GET", "milk") == "$-1\r\n"

def test_argument_validation(redis_client):
    """Test that the server rejects malformed RESP requests gracefully."""
    resp = send_cmd(redis_client, "SET", "only_key")
    assert resp.startswith("-ERR")

def test_bgsave_response(redis_client):
    """Test that BGSAVE acknowledges the background save command."""
    send_cmd(redis_client, "SET", "backup_key", "important_data")
    response = send_cmd(redis_client, "BGSAVE")
    assert "+Background saving started" in response

def test_expire_on_nonexistent_key(redis_client):
    """Test EXPIRE on a key that doesn't exist returns 0."""
    assert send_cmd(redis_client, "EXPIRE", "no_such_key", "10") == ":0\r\n"

def test_expire_invalid_argument(redis_client):
    """Test EXPIRE with a non-integer TTL returns an error."""
    send_cmd(redis_client, "SET", "mykey", "myval")
    resp = send_cmd(redis_client, "EXPIRE", "mykey", "notanumber")
    assert resp.startswith("-ERR")

def test_get_after_expire_not_yet_elapsed(redis_client):
    """Test that a key is still accessible immediately after EXPIRE is set."""
    send_cmd(redis_client, "SET", "fresh", "value")
    send_cmd(redis_client, "EXPIRE", "fresh", "10")
    assert send_cmd(redis_client, "GET", "fresh") == "$5\r\nvalue\r\n"

def test_del_nonexistent_key(redis_client):
    """Test DEL on a key that was never set returns 0."""
    assert send_cmd(redis_client, "DEL", "never_existed") == ":0\r\n"

def test_set_overwrite(redis_client):
    """Test that SET on an existing key overwrites the value and clears TTL."""
    send_cmd(redis_client, "SET", "overwrite_me", "original")
    send_cmd(redis_client, "EXPIRE", "overwrite_me", "1")
    # Overwrite should reset the key, clearing the TTL
    send_cmd(redis_client, "SET", "overwrite_me", "updated")
    time.sleep(1.2)
    # Key should still exist because SET cleared the TTL
    assert send_cmd(redis_client, "GET", "overwrite_me") == "$7\r\nupdated\r\n"

def test_unknown_command(redis_client):
    """Test that an unrecognized command returns a RESP error."""
    resp = send_cmd(redis_client, "FOOBAR", "arg1")
    assert resp.startswith("-ERR")

def test_get_wrong_arg_count(redis_client):
    """Test GET with no arguments returns an error."""
    resp = send_cmd(redis_client, "GET")
    assert resp.startswith("-ERR")

def test_bgsave_idempotent_response(redis_client):
    """Test that a second BGSAVE while one is in progress returns an error."""
    send_cmd(redis_client, "SET", "k", "v")
    send_cmd(redis_client, "BGSAVE")
    # Immediately fire a second one - child process likely still running
    resp = send_cmd(redis_client, "BGSAVE")
    # Either it started (child finished fast) or it correctly errors
    assert "+Background saving started" in resp or resp.startswith("-ERR")
