from cascade.engine import Engine
from cascade._graph_export import export_mermaid

engine = Engine()


@engine.query
def process_item(item_id: int):
    return f"Processed {item_id}"


@engine.query
def query_a():
    # Only depends on item 0
    return process_item(0)


@engine.query
def query_b():
    # Only depends on item 1
    return process_item(1)


if __name__ == "__main__":
    # Execute the queries to build the graph
    query_a()
    query_b()

    print("Step 1: Generating Detailed Graph")
    print("--- Detailed Graph (Precision Intact) ---")
    print("Notice how query_a clearly only depends on process_item(0),")
    print("and query_b only depends on process_item(1).")
    graph_dict = engine.inspect_graph(condense=False)
    print(export_mermaid(graph_dict))

    print("\nStep 2: Generating Condensed Graph")
    print("--- Condensed Graph (Visual Precision Loss) ---")
    print(
        "Notice how query_a and query_b both appear to depend on the same grouped process_item block."
    )
    print(
        "A visual inspection might lead someone to falsely believe they share a dependency,"
    )
    print("when they actually depend on completely distinct instances of process_item.")
    graph_dict_condensed = engine.inspect_graph(condense=True)
    print(export_mermaid(graph_dict_condensed))

    print("Example complete.")

"""
Example graph output:

--- Detailed Graph (Precision Intact) ---
Notice how query_a clearly only depends on process_item(0),
and query_b only depends on process_item(1).
flowchart TD
    n0["query:__main__:process_item(0,)"]
    n1["query:__main__:process_item(1,)"]
    n2["query:__main__:query_a()"]
    n3["query:__main__:query_b()"]
    n2 --> n0
    n3 --> n1

--- Condensed Graph (Visual Precision Loss) ---
Notice how query_a and query_b both appear to depend on the same grouped process_item block.
A visual inspection might lead someone to falsely believe they share a dependency,
when they actually depend on completely distinct instances of process_item.
flowchart TD
    n0["query:__main__:process_item (2 nodes)"]
    n1["query:__main__:query_a()"]
    n2["query:__main__:query_b()"]
    n1 --> n0
    n2 --> n0
"""
