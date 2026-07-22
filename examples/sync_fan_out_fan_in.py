from cascade.engine import Engine
from cascade._graph_export import export_mermaid

engine = Engine()


@engine.query
def process_item_sync(item_id: int):
    return item_id * 2


@engine.input
def range_count():
    return 1


@engine.query
def aggregate_sync():
    return sum(process_item_sync(i) for i in range(range_count()))


if __name__ == "__main__":
    result = aggregate_sync()

    graph_dict = engine.inspect_graph()
    mermaid_str = export_mermaid(graph_dict)

    print("Step 1: Count is 1")
    print(f"Result: {result}")
    print(mermaid_str)

    print("Setting count to 5...")
    range_count.set(5)

    result = aggregate_sync()

    graph_dict = engine.inspect_graph(condense=True)
    mermaid_str = export_mermaid(graph_dict)

    print("Step 2: Count is 5")
    print(f"Result: {result}")
    print(mermaid_str)
    print("Example complete.")

"""
Example graph output:
Step 1: Count is 1
Result: 0
flowchart TD
    n0["input:__main__:range_count()"]
    n1["query:__main__:aggregate_sync()"]
    n2["query:__main__:process_item_sync(0,)"]
    n1 --> n0
    n1 --> n2
Setting count to 5...
Step 2: Count is 5
Result: 20
flowchart TD
    n0["input:__main__:range_count()"]
    n1["query:__main__:aggregate_sync()"]
    n2["query:__main__:process_item_sync (5 nodes)"]
    n1 --> n0
    n1 --> n2
Example complete.
"""
