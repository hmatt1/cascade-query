from cascade.engine import Engine

engine = Engine()

@engine.input
def config(key: str) -> str:
    return ""

@engine.query
def render_ui() -> str:
    print("  [EXEC] render_ui() is running...")
    theme = config("theme")
    layout = config("layout")
    return f"UI(theme={theme}, layout={layout})"

if __name__ == "__main__":
    print("=== First Run ===")
    config.set("theme", "light")
    config.set("layout", "grid")
    print("Result:", render_ui())

    print("\n=== Without Transactions (Flapping) ===")
    print("If we set theme and layout separately, an intermediate read might observe inconsistent state.")
    config.set("theme", "dark")
    # Imagine a concurrent read happens here! It sees theme="dark", layout="grid"
    config.set("layout", "list")
    print("Result:", render_ui())

    print("\n=== With Transactions ===")
    print("Setting both inputs in a transaction ensures they are committed atomically.")
    with engine.transaction():
        config.set("theme", "blue")
        config.set("layout", "sidebar")
    
    # The UI only re-evaluates once observing both changes
    print("Result:", render_ui())
    
    print("\nStep 1")
    print("Example complete.")
