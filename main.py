import uvicorn
import socket


def find_free_port(start = 8080, end = 8090):
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', port))
                return port
            except OSError:
                print(f"Port {port} is in use, trying next one...")
                continue

    raise IOError(f"No free ports found between {start} and {end}.")


if __name__ == "__main__":
    port = find_free_port()
    print(f"Starting server on port http://localhost:{port}")

    uvicorn.run("server:app", host="127.0.0.1", port=port,
                reload=True
                )