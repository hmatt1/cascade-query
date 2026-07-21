"""Demonstrates how to use exception caching for robust interactive tools like IDEs.

In incremental systems like an IDE language server, the source code is frequently in
an invalid state. If a parse query throws an exception and that exception bypasses the
memoization engine, every keystroke will trigger a massive re-evaluation and re-throwing
of the error, degrading performance.

By catching and caching exceptions (via cache_exceptions=True), subsequent calls to the
failed query with the same inputs will instantly re-raise the cached exception without
running the function body, preserving incremental speed even during errors.
"""

from cascade.engine import Engine

engine = Engine()


@engine.input
def source_file(path: str) -> str:
    return ""


class SyntaxError(Exception):
    pass


# By default, engine.query() now has cache_exceptions=True.
# You can also pass a tuple of exception types to be selective:
# @engine.query(cache_exceptions=(SyntaxError,))
@engine.query
def parse_ast(path: str) -> dict:
    source = source_file(path)
    print(f"--> [Parsing {path}]")
    if "{" in source and "}" not in source:
        raise SyntaxError("missing closing brace")
    return {"type": "Module", "body": source}


@engine.query
def analyze_imports(path: str) -> list[str]:
    # This will propagate the SyntaxError if parse_ast fails
    ast = parse_ast(path)
    return ["sys", "os"]


def main():
    print("Step 1: Setting up invalid source code")
    source_file.set("main.py", value="def foo() {")

    print("\nStep 2: First analysis (should fail and parse)")
    try:
        analyze_imports("main.py")
    except SyntaxError as e:
        print(f"Caught expected error: {e}")

    print("\nStep 3: Second analysis (should hit cache and instantly fail)")
    try:
        analyze_imports("main.py")
    except SyntaxError as e:
        print(f"Caught expected error: {e}")

    print("\nStep 4: Fixing the source code")
    source_file.set("main.py", value="def foo() {}")

    print("\nStep 5: Third analysis (should parse successfully)")
    imports = analyze_imports("main.py")
    print(f"Success! Imports: {imports}")
    print("Example complete.")


if __name__ == "__main__":
    main()
