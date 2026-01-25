# linepipe

`linepipe` sits between ad-hoc Python scripts and full orchestration frameworks.
It is best suited for batch pipelines, feature engineering, and reproducible data workflows where simplicity and clarity matter more than scale.

It focuses on:

* clear data dependencies
* reproducible execution (linear pipelines)
* fast iteration and debugging thanks to (local / in-memory) caching
* simple mental models

---

## Why linepipe?

Many pipeline tools are powerful but come with:

* large configuration surfaces
* implicit data catalogs
* steep learning curves

`linepipe` intentionally keeps things simple:

* **Explicit > implicit**
* **Minimal surface area**
* **Easy to reason about**
* **Easy to debug**

### Non-goals

* Distributed execution
* Dynamic DAG scheduling
* Dataset catalogs
* Orchestration / scheduling

`linepipe` is a *building block*, not a platform.

---

## Core concepts

### Node

A `Node` wraps a Python callable and declares:

* which **inputs** it consumes
* which **outputs** it produces

```python
from linepipe.node import Node

def add(a, b):
    return a + b

node = Node(
    func=add,
    inputs=["x", "y"],
    outputs=["sum"],
)
```

Nodes are executed sequentially inside a `Pipeline`.

---

### Pipeline

A `Pipeline` executes a list of `Node`s in order, resolving inputs from:

1. configuration
2. cached / in-memory outputs
3. runtime objects

```python
from linepipe.pipeline import Pipeline

pipeline = Pipeline(
    nodes=[node],
    config={},
    x=2,
    y=3,
)

pipeline.run()
```

Intermediate results can be cached automatically.

---

## Inputs and outputs

### Explicit data flow

Each node declares **string-named inputs and outputs**:

```python
Node(
    func=transform,
    inputs=["raw_data"],
    outputs=["features"],
)
```

---

### Config injection

Configuration values can be injected using `config.<attr>` or you can pass whole `config` object with `"config"`:

```python
class Config:
    window = 5

Node(
    func=rolling_average,
    inputs=["data", "config.window"],
    outputs=["features"],
)
```

---

## Caching

### Persistent cache

You can turn on a disk-backed cache (`shelve`) to store intermediate results with `use_persistant_cache=True`.

```python
pipeline = Pipeline(
    nodes=nodes,
    config=config,
    cache_storage_path="./.cache/pipeline.db",
    use_persistant_cache=True,
)
```

* Cached outputs can be reused across runs. Ideal for debugging and development - if pipeline fails at some point, it is easy to load the inputs, and debug the function.
* The cache is closed automatically at the end of execution

You can reopen it for inspection:

```python
ds = pipeline.open_cache()
print(ds.keys())
ds.close()
```

---

## Side-effect nodes (writers)

Nodes may have **no outputs**, useful for:

* database writes
* file exports
* logging

```python
def write_to_db(df):
    ...

Node(
    func=write_to_db,
    inputs=["features"],
    outputs=[],
)
```

---

## Named partial functions

`linepipe` provides a helper to adapt generic functions into distinct pipeline nodes.

```python
from linepipe.node import create_named_partial_function
from my_package.pipelines.outputs import nodes

write_val_metrics = Node(
    func=create_named_partial_function(
        func=nodes.write_pipeline_output,
        func_name="write_val_metricts",
        table_name="metrics",
        schema="ml"
    ),
    inputs=["val_metrics"],
    outputs=[],
)

write_predictions = Node(
    func=create_named_partial_function(
        func=nodes.write_pipeline_output,
        func_name="write_predictions",
        table_name="predictions",
        schema="ml"
    ),
    inputs=["predictions"],
    outputs=[],
)
```

This:

* pre-binds keyword arguments
* gives the node a stable, readable name
* improves logging, history, and introspection

---

## Pipeline composition

Pipelines can be combined using `+`:

```python
full_pipeline = feature_pipeline + writer_pipeline
full_pipeline.run()
```

Composition:

* preserves execution order
* prevents output name collisions
* merges runtime objects and configuration
* Gives you pipeline reusability, for example, same feature-generation pipeline in train and predict

---

## Pipeline vizualization

`linepipe.viz` provides simple, no-dependency ascii graph drawing function `draw_ascii_pipeline`. It draws nodes in order they are defined along with their inputs and ouputs - good for quick check.

```python

```

Output can look like this:
```
Pipeline (5 nodes)

[load_matches]           # Node name
  outputs:
    - raw_matches
      |
      v
[clean_matches]
  inputs:                # list of inputs
    - raw_matches
  outputs:               # list of outputs
    - clean_matches
      |
      v
[rolling_goals]
  inputs:
    - clean_matches
    - config.window
    - config.min_matches
  outputs:
    - rolling_features

    ...
```

If you need something more vizual for documatation you can install optional dependency - `plotly` (`pip install linepipe[plotly]`) and use `plot_pipeline` function. Which would look like:

# PASTE PNG

---

## History tracking

Enable execution history for debugging:

```python
pipeline = Pipeline(
    nodes=nodes,
    config=config,
    track_history=True,  # in-memory persistant after `run()` is finished
    use_persistant_cache=False,  # (if True) disk persistant after `run()` is finished
)
```

After execution:

```python
pipeline.history
```

Each entry contains:

* node name
* resolved inputs
* outputs

---

## [Optional] Memory profiling

If you wish to profile your nodes you can install optional `memory_profiler` dependency with `pip install linepipe[memory-profiler]`. This gives you ability to log memory usage during run of a node. For example:

```python
pipeline = Pipeline(
    nodes= [
        Node(
            func=nodes.expensive_function,
            inputs=["df"],
            outputs=["df_processed"],
            profile=True
        )
    ]
)
pipeline.run()
```

This will produce logs like:

`[write_features] ΔMem: 0.94 MiB | Peak: 0.94 MiB`

Structrure is `[node_name] ΔMem: {float} MiB | Peak: {float} MiB`

 * ΔMem: delta between first and last recorded value
 * Peak: max(recorded_values) - recorded values[0]

---

## License

MIT License.
