"""Deprecated pipeline finalizer.

Pipeline-level OpenLineage START/COMPLETE/FAIL events are emitted directly by
openlineage_emitter from executor.py. This module intentionally does not emit a
second pipeline event, because duplicate pipeline events with empty datasets can
flatten the Datadog Lineage graph.
"""

import logging

logger = logging.getLogger("controlm-sim.pipeline_finalizer")


def emit_pipeline_complete(pipeline_result: dict) -> bool:
    """Compatibility shim for older callers; intentionally no-op."""
    logger.info(
        "Skipping deprecated pipeline_finalizer for run_id=%s",
        pipeline_result.get("run_id"),
    )
    return True
