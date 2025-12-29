# app/baseline_curve.py

BASELINE_HALFTIME_PROBS = [
    {"low": -20, "high": -16, "p": 0.1331, "weight": 0.85},
    {"low": -15, "high": -11, "p": 0.1868, "weight": 0.93},
    {"low": -10, "high":  -6, "p": 0.3065, "weight": 0.94},
    {"low":  -5, "high":  -1, "p": 0.4966, "weight": 0.94},
    {"low":   0, "high":   0, "p": 0.5526, "weight": 0.77},
    {"low":   1, "high":   5, "p": 0.6862, "weight": 0.94},
    {"low":   6, "high":  10, "p": 0.8188, "weight": 0.94},
    {"low":  11, "high":  15, "p": 0.9132, "weight": 0.94},
    {"low":  16, "high":  20, "p": 0.9508, "weight": 0.93},
]


def cap_margin(margin: int) -> int:
    if margin < -20:
        return -20
    if margin > 20:
        return 20
    return margin


def lookup_baseline_prob(halftime_margin: int): # (got rid of) -> float
    capped = cap_margin(halftime_margin)

    for bucket in BASELINE_HALFTIME_PROBS:
        if bucket["low"] <= capped <= bucket["high"]:
            return bucket["p"], bucket["weight"]

    return 0.5, 1.0  # defensive fallback: neutral prob. and weight
