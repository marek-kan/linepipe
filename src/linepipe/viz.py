from __future__ import annotations

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
