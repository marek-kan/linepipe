from __future__ import annotations

from typing import Any

from linepipe.pipeline import Pipeline


def draw_ascii_pipeline(pipeline: Pipeline) -> str:
    """
    Return a human-readable ASCII representation of a Pipeline.

    The output is intended for inspection, logging, and documentation.
    """
    lines: list[str] = []
    lines.append(f"Pipeline ({len(pipeline.nodes)} nodes)")
    lines.append("")

    for i, node in enumerate(pipeline.nodes):
        name = node.func.__name__
        lines.append(f"[{name}]")

        if node.inputs:
            lines.append("  inputs:")
            for inp in node.inputs:
                lines.append(f"    - {inp}")

        if node.outputs:
            lines.append("  outputs:")
            for out in node.outputs:
                lines.append(f"    - {out}")
        else:
            lines.append("  (no outputs)")

        if i < len(pipeline.nodes) - 1:
            lines.append("      |")
            lines.append("      v")

    return "\n".join(lines)


def plot_pipeline_graph(
    pipeline: Pipeline,
    figsize: tuple[int, int] = (800, 600),
    title: str | None = None,
) -> Any:
    """
    Generate a hierarchical top-to-bottom graph plot for the pipeline.
    Nodes are blue squares, data-sources are orange circles.

    Note:
        This feature is experimantal. Lables might be overlapping.
    """

    try:
        import networkx as nx  # pyright: ignore[reportMissingImports]
        import plotly.graph_objects as go  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError("Plotting extensions is not installed. Install with: pip install linepipe[plot]") from exc

    G = nx.DiGraph()

    for item in pipeline.nodes:
        func_node = item.func

        G.add_node(func_node, type="function")

        for inp in item.inputs:
            if not G.has_node(inp):
                G.add_node(inp, type="data")
            G.add_edge(inp, func_node)

        for out in item.outputs:
            if not G.has_node(out):
                G.add_node(out, type="data")
            G.add_edge(func_node, out)

    levels: dict[str, int] = {}
    for node in list(nx.topological_sort(G)):
        preds = list(G.predecessors(node))
        levels[node] = 0 if not preds else max(levels[p] for p in preds) + 1

    node_positions: dict[int, list[str]] = {}
    for node, level in levels.items():
        node_positions.setdefault(level, []).append(node)

    node_positions = dict(sorted(node_positions.items()))

    # max_width = max(len(nodes) for nodes in node_positions.values())

    pos = {}
    for level, nodes in node_positions.items():
        y = -level * 2
        xs = [(i + 1) / (len(nodes) + 1) for i in range(len(nodes))]
        for node, x in zip(nodes, xs, strict=False):
            pos[node] = (x, y)

    edge_x, edge_y = [], []

    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=2, color="gray"),
        hoverinfo="none",
        mode="lines",
    )

    function_nodes_x, function_nodes_y, function_nodes_text = [], [], []
    data_nodes_x, data_nodes_y, data_nodes_text = [], [], []

    for node in G.nodes():
        x, y = pos[node]
        node_type = G.nodes[node].get("type", "data")

        if node_type == "function":
            function_nodes_x.append(x)
            function_nodes_y.append(y)
            function_nodes_text.append(node.__name__)
        else:
            data_nodes_x.append(x)
            data_nodes_y.append(y)
            data_nodes_text.append(node)

    function_trace = go.Scatter(
        x=function_nodes_x,
        y=function_nodes_y,
        mode="markers+text",
        text=function_nodes_text,
        textposition="middle right",
        hoverinfo="text",
        marker=dict(
            symbol="square",
            color="lightblue",
            size=40,
            line=dict(width=2, color="blue"),
        ),
        name="Function Nodes",
    )

    data_trace = go.Scatter(
        x=data_nodes_x,
        y=data_nodes_y,
        mode="markers+text",
        text=data_nodes_text,
        textposition="middle left",
        hoverinfo="text",
        marker=dict(
            symbol="circle",
            color="orange",
            size=30,
            line=dict(width=2, color="darkorange"),
        ),
        name="Data Nodes",
    )

    fig = go.Figure(
        data=[edge_trace, function_trace, data_trace],
        layout=go.Layout(
            title="Pipeline Graph" if title is None else title,
            font_size=18,
            showlegend=True,
            legend=dict(x=1, y=1),
            hovermode="closest",
            width=figsize[0],
            height=figsize[1],
            margin=dict(b=40, l=40, r=40, t=60),
            xaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
            ),
        ),
    )

    return fig
