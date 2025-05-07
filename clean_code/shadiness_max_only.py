import re
import numpy as np
import networkx as nx
from scipy.stats import norm

### ---- Helper Functions (as you already have) ---- ###
def parse_numeric(val):
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = re.sub(r'[^0-9.]', '', val)
        try:
            return float(s)
        except ValueError:
            return None
    return None

def parse_screening(node_attrs):
    s = node_attrs.get('screening', '')
    try:
        data = eval(s) if isinstance(s, str) else s
        if not data.get('match_found', False):
            return 0.0
        scores = data.get('scores', {})
        return max(float(v) for v in scores.values() if v is not None) if scores else 0.0
    except:
        return 0.0

def parse_stake(node_attrs, stake_attrs):
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

### ---- Core Steps ---- ###

def compute_shadiness_max_only(G,
                                omega_sub=0.5, omega_sh=0.6,
                                omega_duo=0.4, omega_guo=0.7):
    for node, attrs in G.nodes(data=True):
        if attrs.get('bid_winner') == 'True':
            cand_score = parse_screening(attrs)
            ownership = []
            for u, v, d in G.in_edges(node, data=True):
                rel = d.get('relationship')
                if rel == 'SUBSIDIARY_OF':
                    ownership.append((u, parse_screening(G.nodes[u]), omega_sub, ['subsidiarydirect','subsidiarytotal']))
                elif rel == 'SHAREHOLDER_OF':
                    ownership.append((u, parse_screening(G.nodes[u]), omega_sh, ['shareholderdirect','shareholdertotal']))
                elif rel == 'DOMESTIC_ULTIMATE_OWNER_OF':
                    ownership.append((u, parse_screening(G.nodes[u]), omega_duo, ['duodirect','duototal']))
                elif rel == 'GLOBAL_ULTIMATE_OWNER_OF':
                    ownership.append((u, parse_screening(G.nodes[u]), omega_guo, ['guodirect','guototal']))

            # pick the single highest‐risk node
            choices = [(node, cand_score, None)] + [(u, sc, om) for u,sc,om,_ in ownership]
            best_node, best_score, _ = max(choices, key=lambda x: x[1])

            prod = 1.0
            # candidate itself
            if best_node == node:
                prod *= (1 - best_score)
            else:
                prod *= 1.0

            # ownership contributions
            for u, sc, om, stake_attrs in ownership:
                w = parse_stake(G.nodes[u], stake_attrs)
                if u == best_node:
                    prod *= (1 - om * w * sc)
                else:
                    prod *= 1.0

            G.nodes[node]['shadiness_max'] = min(max(1 - prod, 0.0), 1.0)

def compute_expected_shadiness(G):
    for node, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            total = 0.0
            for u, v, d in G.in_edges(node, data=True):
                if d.get('relationship') == 'WON' and G.nodes[u].get('bid_winner') == 'True':
                    likely = float(G.nodes[u].get('likelyhood', 0.0))
                    sh = float(G.nodes[u].get('shadiness', 0.0))
                    total += likely * sh
            G.nodes[node]['expected_shadiness_max'] = min(max(total, 0.0), 1.0)

def compute_urgency(G,
                    value_attrs=('AWARD_VALUE_EURO_FIN_1','AWARD_EST_VALUE_EURO','AWARD_VALUE_EURO'),
                    a=0.5, b=2.0, denom_percentile=99, median_fill=True):
    vals = []
    proc_vals = {}
    for p, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            vlist = []
            for u, v, d in G.in_edges(p, data=True):
                if d.get('relationship')=='WON' and G.nodes[u].get('bid_winner')=='True':
                    for attr in value_attrs:
                        num = parse_numeric(G.nodes[u].get(attr))
                        if num is not None:
                            vlist.append(num)
                            break
            if vlist:
                m = float(np.mean(vlist))
                proc_vals[p] = m
                vals.append(m)

    if not vals:
        raise ValueError("No numeric values found.")

    vmax = np.percentile(vals, denom_percentile)
    med = np.median(vals)
    mean, std = np.mean(vals), np.std(vals)
    if std == 0:
        raise ValueError("Zero std; cannot z-score.")

    for p, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            v = proc_vals.get(p, med if median_fill else 0.0)
            z = (v - mean) / std
            x = norm.cdf(z)
            gamma = a + (b - a) * x
            scaled = x**gamma if x>0 else 0.0
            r = float(attrs.get('expected_shadiness', 0.0))
            G.nodes[p]['urgency_max'] = r * scaled
            G.nodes[p]['urgency_linear_max'] = r * x

### ---- Single‐Function Pipeline ---- ###
def run_shadiness_pipeline(G,
                           omega_sub=0.5, omega_sh=0.6,
                           omega_duo=0.4, omega_guo=0.7,
                           urgency_args=None):
    """
    Executes the full pipeline on G in‐place:
      1) compute_shadiness_max_only
      2) compute_expected_shadiness
      3) compute_urgency

    Parameters:
      G            : networkx.Graph or DiGraph
      omega_*      : weights for the four ownership types
      urgency_args : dict of arguments for compute_urgency()
    """
    compute_shadiness_max_only(G, omega_sub, omega_sh, omega_duo, omega_guo)
    compute_expected_shadiness(G)
    ua = urgency_args or {}
    compute_urgency(G, **ua)
    return G