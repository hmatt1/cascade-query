import os
from dataclasses import dataclass
from cascade import Engine, CascadeDict

# A sample dataclass to use as a key, similar to what you might use in a real project
@dataclass(frozen=True)
class MyKey:
    category: str
    id_num: int

def main():
    cache_directory = "./my_cascade_cache"

    # 1. Initialize the Cascade Engine with a cache directory for persistence
    # If cache_dir is provided, CascadeDict will automatically read/write to MDBX.
    engine = Engine(cache_dir=cache_directory)

    # 2. Create the CascadeDict
    # The 'name' parameter is critical: it identifies the collection in the on-disk log.
    my_dict = CascadeDict(engine, name="my_persistent_dict")

    print(f"Step 1: Initial dictionary state loaded from disk: {dict(my_dict)}")

    # 3. Create some keys and add data
    key1 = MyKey(category="user", id_num=101)
    key2 = MyKey(category="session", id_num=999)

    if key1 not in my_dict:
        print(f"Adding {key1} to the dictionary...")
        my_dict[key1] = {"name": "Alice", "role": "admin"}
        my_dict[key2] = {"active": True, "token": "xyz"}
    else:
        print(f"Keys already exist! {key1} -> {my_dict[key1]}")

    # 4. Modify existing data (this appends an 'upsert' to the MDBX log)
    my_dict[key1] = {"name": "Alice", "role": "superadmin"}

    # 5. Delete data (this appends a 'remove' to the MDBX log)
    if key2 in my_dict:
        del my_dict[key2]

    print(f"Final dictionary state: {dict(my_dict)}")

    # 6. (Optional) Force the disk log to compact itself down to just the current snapshot
    # This prevents the MDBX append-only log from growing infinitely if you make many small edits.
    my_dict.compact()

    # Note: query-cascade's Engine does not require an explicit 'close' or 'stop' method.
    # It automatically flushes and handles the MDBX environment lifecycle when garbage collected.
    print("Example complete.")
    print("Example complete.")

if __name__ == "__main__":
    main()