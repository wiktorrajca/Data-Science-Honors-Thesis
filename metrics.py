import networkx as nx
import ast
import re
from scipy.stats import norm

def parse_screening(node_attrs):
    """
    Parse the 'screening' attribute (dict) into a float score.
    Now uses the maximum score across datasets if match_found=True.
    Returns 0.0 if no match.
    """
    s = node_attrs.get('screening', '')
    try:
        # No need to literal_eval if it's already a dict, but support both
        if isinstance(s, str):
            data = ast.literal_eval(s)
        else:
            data = s
        
        if not data.get('match_found', False):
            return 0.0
        
        scores = data.get('scores', {})
        if isinstance(scores, dict) and scores:
            return max(float(v) for v in scores.values() if v is not None)
        else:
            return 0.0
    except (ValueError, SyntaxError):
        return 0.0

def parse_stake(node_attrs, stake_attrs):
    """
    Extract an ownership stake from node attributes.
    """
    for attr in stake_attrs:
        val = node_attrs.get(attr)
        if val is not None:
            m = re.search(r'\d+\.?\d*', str(val))
            if m:
                try:
                    return float(m.group()) / 100.0
                except ValueError:
                    pass
    return 1.0

def compute_channel_prob(G, candidate, relationship, stake_attrs, omega):
    """
    Compute the probability that at least one connected entity
    (via the specified relationship) is flagged.
    """
    prod = 1.0
    for u, v, data in G.in_edges(candidate, data=True):
        if data.get('relationship') == relationship:
            p_entity = parse_screening(G.nodes[u])
            w = parse_stake(G.nodes[u], stake_attrs)
            prod *= (1 - omega * w * p_entity)
    return 1 - prod

def compute_shadiness(G,
                      omega_sub=0.5, omega_sh=0.6,
                      omega_duo=0.4, omega_guo=0.7):
    """
    For each node marked with bid_winner='True', compute and set the 'shadiness' attribute.
    """
    for node, attrs in G.nodes(data=True):
        if attrs.get('bid_winner') == 'True':
            # Direct flag
            p_dir = parse_screening(attrs)

            # Channel probabilities
            P_sub = compute_channel_prob(G, node, 'SUBSIDIARY_OF',
                                         ['subsidiarydirect', 'subsidiarytotal'], omega_sub)
            P_sh  = compute_channel_prob(G, node, 'SHAREHOLDER_OF',
                                         ['shareholderdirect', 'shareholdertotal'], omega_sh)
            P_duo = compute_channel_prob(G, node, 'DOMESTIC_ULTIMATE_OWNER_OF',
                                         ['duodirect', 'duototal'], omega_duo)
            P_guo = compute_channel_prob(G, node, 'GLOBAL_ULTIMATE_OWNER_OF',
                                         ['guodirect', 'guototal'], omega_guo)

            # OR-style union merge
            S = 1 - (1 - p_dir) * (1 - P_sub) * (1 - P_sh) * (1 - P_duo) * (1 - P_guo)

            # Assign back to node
            G.nodes[node]['shadiness'] = S

def compute_expected_shadiness(G):
    """
    For each procurement node in G, compute and set the 'expected_shadiness' attribute.
    'expected_shadiness' = sum over all winner candidates of (likelyhood * shadiness).
    """
    for node, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            total = 0.0
            # incoming edges from winner candidates
            for u, v, data in G.in_edges(node, data=True):
                if data.get('relationship') == 'WON' and G.nodes[u].get('bid_winner') == 'True':
                    likelyhood = float(G.nodes[u].get('likelyhood', 0.0))  # keep typo
                    shadiness = float(G.nodes[u].get('shadiness', 0.0))
                    total += likelyhood * shadiness
            G.nodes[node]['expected_shadiness'] = min(max(total, 0.0), 1.0)  # safely clipped

import numpy as np
import re

def parse_numeric_value(val):
    """
    Parse a procurement value which may be a string with commas or other characters.
    Returns a float or None if parsing fails.
    """
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = re.sub(r'[^0-9.]', '', val)
        try:
            return float(s)
        except ValueError:
            return None
    return None

def compute_urgency_from_winner_values(G, 
                                       value_attrs=('AWARD_VALUE_EURO_FIN_1', 
                                                    'AWARD_EST_VALUE_EURO', 
                                                    'AWARD_VALUE_EURO'),
                                       a=0.5, b=2.0, median_fill=True,
                                       value_cap=1_000_000_000):
    """
    Compute and set 'urgency' (nonlinear) and 'urgency_linear' (linear) for each Procurement node,
    using z-score normalization and standard normal CDF. Values > value_cap are discarded.
    """
    raw_vals = []
    procurement_winner_values = {}

    for p, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            vals = []
            for u, v, data in G.in_edges(p, data=True):
                if data.get('relationship') == 'WON' and G.nodes[u].get('bid_winner') == 'True':
                    for attr in value_attrs:
                        num = parse_numeric_value(G.nodes[u].get(attr))
                        if num is not None and num <= value_cap:
                            vals.append(num)
                            break
            if vals:
                val = float(np.mean(vals))
                raw_vals.append(val)
                procurement_winner_values[p] = val

    if not raw_vals:
        raise ValueError(f"No valid numeric values found (all exceeded {value_cap}) under {value_attrs}.")

    mean_v = np.mean(raw_vals)
    std_v = np.std(raw_vals)

    if std_v == 0:
        raise ValueError("Standard deviation of procurement values is zero — cannot z-score.")

    # 2) Compute urgency scores
    for p, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            v = procurement_winner_values.get(p)
            if v is None:
                if median_fill:
                    v = np.median(raw_vals)
                else:
                    G.nodes[p]['urgency'] = 0.0
                    G.nodes[p]['urgency_linear'] = 0.0
                    continue

            z = (v - mean_v) / std_v
            x = norm.cdf(z)  # maps z to [0, 1]

            gamma = a + (b - a) * x
            v_scaled = x ** gamma if x > 0 else 0.0

            r = float(attrs.get('expected_shadiness', 0.0))
            G.nodes[p]['urgency'] = r * v_scaled
            G.nodes[p]['urgency_linear'] = r * x


###--------CHECKS--------###
def verify_shadiness_computed_and_range(G):
    """
    Check that every winner node in G has a 'shadiness' attribute.
    Prints a warning if any are missing, and reports the range of shadiness values.
    Returns:
      all_computed (bool): True if every winner has shadiness.
      missing_list (list): list of winner node IDs missing the attribute.
      (min_val, max_val) (tuple): range of shadiness values if computed, else (None, None).
    """
    # Identify winner nodes
    winners = [n for n, attrs in G.nodes(data=True) if attrs.get('bid_winner') == 'True']
    
    # Check for missing shadiness
    missing = [n for n in winners if 'shadiness' not in G.nodes[n]]
    all_computed = len(missing) == 0

    if not all_computed:
        print(f"Warning: {len(missing)} winner nodes missing 'shadiness': {missing}")
    else:
        print("All winner nodes have 'shadiness'.")

    # Compute range of shadiness values
    values = [G.nodes[n]['shadiness'] for n in winners if 'shadiness' in G.nodes[n]]
    if values:
        min_val = min(values)
        max_val = max(values)
        print(f"Shadiness range: {min_val:.3f} to {max_val:.3f}")
    else:
        min_val = max_val = None
        print("No shadiness values found to compute range.")

    return all_computed, missing, (min_val, max_val)

def verify_expected_shadiness_computed(G):
    """
    Check that every Procurement node in G has an 'expected_shadiness' attribute.
    Prints a warning if any are missing and reports the range of computed values.
    Returns:
      all_computed (bool): True if every Procurement has expected_shadiness.
      missing_list (list): list of Procurement node IDs missing the attribute.
      (min_val, max_val) (tuple): range of expected_shadiness if computed, else (None, None).
    """
    missing = [
        n for n, attrs in G.nodes(data=True)
        if attrs.get('type') == 'Procurement' and 'expected_shadiness' not in attrs
    ]
    all_computed = len(missing) == 0

    if not all_computed:
        print(f"Warning: {len(missing)} procurement nodes missing 'expected_shadiness': {missing}")
    else:
        print("All procurement nodes have 'expected_shadiness'.")

    # Compute range if at least one computed
    values = [
        attrs['expected_shadiness']
        for n, attrs in G.nodes(data=True)
        if attrs.get('type') == 'Procurement' and 'expected_shadiness' in attrs
    ]

    if values:
        min_val = min(values)
        max_val = max(values)
        print(f"Expected shadiness range: {min_val:.3f} to {max_val:.3f}")
    else:
        min_val = max_val = None
        print("No expected shadiness values found to compute range.")

    return all_computed, missing, (min_val, max_val)

def verify_urgency_computed_and_range(G):
    """
    Verify every Procurement node has 'urgency' and 'urgency_linear' and report their ranges.
    Returns (all_computed, missing_list, (min_val, max_val)).
    """
    missing = []
    urgency_values = []
    urgency_linear_values = []
    
    for n, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            if 'urgency' not in attrs or 'urgency_linear' not in attrs:
                missing.append(n)
            else:
                urgency_values.append(attrs['urgency'])
                urgency_linear_values.append(attrs['urgency_linear'])
    
    all_computed = len(missing) == 0
    if not all_computed:
        print(f"Warning: {len(missing)} procurement nodes missing 'urgency' or 'urgency_linear': {missing}")
    else:
        print("✅ All procurement nodes have both 'urgency' and 'urgency_linear'.")

    if urgency_values:
        print(f"Urgency range: {min(urgency_values):.3f} to {max(urgency_values):.3f}")
    else:
        print("No urgency values found.")

    if urgency_linear_values:
        print(f"Urgency_linear range: {min(urgency_linear_values):.3f} to {max(urgency_linear_values):.3f}")
    else:
        print("No urgency_linear values found.")

    return all_computed, missing, (min(urgency_values) if urgency_values else None, max(urgency_values) if urgency_values else None)
