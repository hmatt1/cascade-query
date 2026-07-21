from cascade.engine import Engine
from cascade._graph_export import export_mermaid

engine = Engine()

@engine.query
def process_item_sync(item_id: int):
    return item_id * 2

@engine.query
def aggregate_sync():
    # Reduced to 5 for a readable diagram
    return sum(process_item_sync(i) for i in range(5))

if __name__ == "__main__":
    result = aggregate_sync()
    
    graph_dict = engine.inspect_graph()
    mermaid_str = export_mermaid(graph_dict)
    
    print("Step 1")
    print(mermaid_str)
    print("Example complete.")
"""
Example graph output:
    flowchart TD
        n0["query:__main__:aggregate_sync()"]
        n1["query:__main__:process_item_sync(0,)"]
        n2["query:__main__:process_item_sync(1,)"]
        n3["query:__main__:process_item_sync(2,)"]
        n4["query:__main__:process_item_sync(3,)"]
        n5["query:__main__:process_item_sync(4,)"]
        n0 --> n1
        n0 --> n2
        n0 --> n3
        n0 --> n4
        n0 --> n5
"""
