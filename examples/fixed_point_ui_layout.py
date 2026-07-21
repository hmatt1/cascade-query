"""
Example demonstrating Fixed-Point Cycle Solving for UI Layout constraints.

In UI rendering, a parent container's size often depends on its children's sizes,
but a child's size can also be defined as a percentage of its parent's size.
This creates a circular dependency.

Using a fixed-point solver, the layout engine can iterate until the dimensions stabilize.
"""

from cascade import Engine

engine = Engine()

# UI Layout Scenario:
# - A Parent Container holds two items: a Sidebar and a Main Content area.
# - The Parent Container's width automatically expands to fit its largest child.
# - The Sidebar has a fixed width of 200px.
# - The Main Content area wants to be exactly 80% of the Parent Container's width.

# Circular constraint:
# Parent Width -> depends on -> Main Content Width -> depends on -> Parent Width

@engine.query(fixed_point=0.0)
def parent_container_width() -> float:
    # Parent width is the maximum of its children's widths
    return max(sidebar_width(), main_content_width())

@engine.query
def sidebar_width() -> float:
    # Sidebar is fixed at 200px
    return 200.0

@engine.query
def main_content_width() -> float:
    # Main content dynamically scales to 80% of the parent container
    return parent_container_width() * 0.8


def main() -> None:
    print("Step 1: Evaluating UI Layout Dimensions...")
    
    # We expect the solver to seamlessly resolve the constraints:
    # Iteration 1:
    #   parent_guess = 0
    #   main_content = 0 * 0.8 = 0
    #   sidebar = 200
    #   parent evaluates to max(200, 0) = 200. Loops!
    # 
    # Iteration 2:
    #   parent_guess = 200
    #   main_content = 200 * 0.8 = 160
    #   sidebar = 200
    #   parent evaluates to max(200, 160) = 200. Matches guess, converged!

    width = parent_container_width()
    print(f"  Parent Container Width: {width}px")
    print(f"  Sidebar Width: {sidebar_width()}px")
    print(f"  Main Content Width: {main_content_width()}px")
    
    print("\nExample complete.")


if __name__ == "__main__":
    main()
