"""Generated generic case adapter; no domain-specific enrichment was declared."""

def _department(frame, source, config):
    return frame

def dimension_lookup_keys(frame, spec):
    return list(spec["keys"])
