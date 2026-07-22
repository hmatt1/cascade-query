"""Demonstrates that cached exceptions can be persisted across process restarts.

Error caching is fully supported by the persistent disk cache. If a query throws
an exception and its dependencies haven't changed, the next run of the program
will load the cached error from disk and re-raise it without doing any work.
"""

import tempfile
from cascade.engine import Engine


class CustomNetworkError(Exception):
    pass


def run_session(cache_dir: str, attempt: int, simulate_fix: bool = False):
    print(f"\nStep {attempt}: Session {attempt} (simulating a new process run)")

    # Initialize an engine pointing at the shared disk cache directory.
    engine = Engine(cache_dir=cache_dir)

    # Simulate a network endpoint that might be broken
    @engine.input
    def api_endpoint() -> str:
        # In a real app, this might read from a config file
        return (
            "https://broken-api.example.com"
            if not simulate_fix
            else "https://api.example.com"
        )

    @engine.query
    def fetch_data(endpoint: str) -> dict:
        print(f"--> [Fetching data from {endpoint}]")
        if "broken" in endpoint:
            raise CustomNetworkError(f"Failed to connect to {endpoint}")
        return {"data": "success"}

    @engine.query
    def process_data() -> dict:
        endpoint = api_endpoint()
        data = fetch_data(endpoint)
        return {"processed": data["data"].upper()}

    try:
        result = process_data()
        print(f"Success! Result: {result}")
    except CustomNetworkError as e:
        print(f"Caught network error: {e}")

    engine.shutdown()


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Session 1: Encounters an error and persists it
        run_session(tmpdir, attempt=1, simulate_fix=False)

        # Session 2: Instantly fails via the disk cache (no fetching happens)
        run_session(tmpdir, attempt=2, simulate_fix=False)

        # Session 3: We "fix" the config; the cache is invalidated and data is fetched
        run_session(tmpdir, attempt=3, simulate_fix=True)
        print("Example complete.")


if __name__ == "__main__":
    main()
