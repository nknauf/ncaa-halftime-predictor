# Confidence increases when: stats agree with margin
# Confidence decreases when:margin is driven by hot shooting

# FG% diff      → shooting sustainability
# 3P% diff      → volatility (lower weight)
# FTA diff      → aggression
# TO diff       → possession quality (inverted)
# ORB diff      → extra chances
# REB diff      → control

# confidence =
#     base_margin_confidence
#   × agreement_factor
#   × shooting_penalty
#   × (1 + hqs_strength_boost)

# app/confidence_model.py

from typing import Dict, Optional


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and "%" in x:
            return float(x.replace("%", "")) / 100.0
        return float(x)
    except Exception:
        return None


def compute_halftime_quality(
    home: Dict,
    away: Dict,
) -> Dict:
    """
    Computes a Halftime Quality Score (HQS) using only
    first-half boxscore stats currently available.

    Returns a dict with:
      - hqs (float)
      - shooting_extreme (bool)
    """

    # Parse values safely
    h_fg  = _safe_float(home.get("fg_pct"))
    a_fg  = _safe_float(away.get("fg_pct"))
    h_3p  = _safe_float(home.get("fg3_pct"))
    a_3p  = _safe_float(away.get("fg3_pct"))

    h_fta = _safe_float(home.get("ft_att"))
    a_fta = _safe_float(away.get("ft_att"))

    h_to  = _safe_float(home.get("turnovers"))
    a_to  = _safe_float(away.get("turnovers"))

    h_orb = _safe_float(home.get("off_reb"))
    a_orb = _safe_float(away.get("off_reb"))

    h_reb = _safe_float(home.get("tot_reb"))
    a_reb = _safe_float(away.get("tot_reb"))

    # Default missing values to zero *difference*
    fg_diff  = (h_fg  - a_fg)  if h_fg  is not None and a_fg  is not None else 0.0
    fg3_diff = (h_3p  - a_3p)  if h_3p  is not None and a_3p  is not None else 0.0

    ft_diff  = ((h_fta - a_fta) / 20.0) if h_fta is not None and a_fta is not None else 0.0
    orb_diff = ((h_orb - a_orb) / 10.0) if h_orb is not None and a_orb is not None else 0.0
    reb_diff = ((h_reb - a_reb) / 15.0) if h_reb is not None and a_reb is not None else 0.0

    # Turnovers inverted: fewer TOs is better
    to_diff = ((a_to - h_to) / 10.0) if h_to is not None and a_to is not None else 0.0

    # Halftime Quality Score (Bayesian-lite efficiency proxy)
    hqs = (
        0.30 * fg_diff +
        0.15 * fg3_diff +
        0.20 * to_diff +
        0.15 * orb_diff +
        0.10 * reb_diff +
        0.10 * ft_diff
    )

    # Fluke detection: extreme shooting
    shooting_extreme = (
        abs(fg_diff) > 0.15 or
        abs(fg3_diff) > 0.20
    )

    return {
        "hqs": hqs,
        "shooting_extreme": shooting_extreme,
    }


def compute_confidence_with_stats(
    p_baseline: float,
    baseline_weight: float,
    halftime_margin: int,
    stats_home: Dict,
    stats_away: Dict,
) -> float:
    """
    Final confidence score combining:
      - empirical halftime margin strength
      - first-half statistical quality

    Returns a numeric confidence score in ~[0.0, 0.45]
    """

    # Base confidence from margin (your existing logic)
    base_conf = abs(p_baseline - 0.5) * baseline_weight

    quality = compute_halftime_quality(stats_home, stats_away)
    hqs = quality["hqs"]

    # Agreement between margin and stat profile
    agreement = 1.0 if _sign(hqs) == _sign(halftime_margin) else 0.6

    # Shooting fluke penalty
    shooting_penalty = 0.85 if quality["shooting_extreme"] else 1.0

    # Strength boost (stats can enhance confidence, not dominate)
    strength_boost = 1.0 + min(0.75 * abs(hqs), 0.30)

    confidence = (
        base_conf
        * agreement
        * shooting_penalty
        * strength_boost
    )

    return round(confidence, 4)
