import time
from dataclasses import dataclass
from cascade import Engine

# Incremental compilation.
# Re-parses and type-checks only when the source input changes.

@dataclass(frozen=True)
class AST:
    tokens: tuple[str, ...]

engine = Engine()
warnings = engine.accumulator("warnings")

@engine.input
def source_code() -> str:
    return "x = 1; y = 2"

@engine.query
def parse() -> AST:
    print("⏳ Parsing source code...")
    time.sleep(1)
    return AST(tokens=tuple(source_code().split(";")))

@engine.query
def typecheck() -> str:
    print("⏳ Running type-checker...")
    time.sleep(1)
    ast = parse()
    
    for token in ast.tokens:
        if "warning" in token:
            warnings.push(f"Potential issue in: {token}")
            
    return f"Checked {len(ast.tokens)} definitions"

# --- Demo ---

print("Step 1: Initial Compilation")
effects: dict = {}
print(typecheck(effects=effects))
print(f"Warnings: {effects.get('warnings', [])}")

print("\nStep 2: Add a warning (Triggers re-evaluation)")
source_code.set("x = 1; y = 2; warning_here")
effects = {}
print(typecheck(effects=effects))
print(f"Warnings: {effects.get('warnings', [])}")

print("\nStep 3: Cached access (Replays stored warnings)")
effects = {}
print(typecheck(effects=effects))
print(f"Warnings: {effects.get('warnings', [])}")

print("Example complete.")
