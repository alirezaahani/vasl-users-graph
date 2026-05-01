#!/usr/bin/env python3
"""
Vasl social graph visualiser.
Reads the SQLite crawler DB and produces a graph visualisation.

Usage examples:
  # Interactive HTML (default, good for exploration)
  python graph_viz.py --db vasl_crawler.db --max-nodes 2000 --output graph.html

  # Static high-res PNG (for very large graphs)
  python graph_viz.py --db vasl_crawler.db --max-nodes 20000 --output graph.png --backend datashader

  # Export for Gephi
  python graph_viz.py --db vasl_crawler.db --output graph.gexf
"""

import argparse
import sqlite3
import sys
import warnings
from pathlib import Path

import networkx as nx
import numpy as np


# =============================================================================
#  Database loading
# =============================================================================

def load_graph(db_path, max_nodes=None):
    """Load directed graph from the crawler's SQLite database."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get all edges
    cur.execute("SELECT follower_id, followee_id FROM edges")
    edges = cur.fetchall()

    # Get user names (optional)
    cur.execute("SELECT user_id, display_name FROM users")
    user_names = dict(cur.fetchall())
    conn.close()

    G = nx.DiGraph()
    for follower, followee in edges:
        G.add_edge(follower, followee)

    # Add node attributes
    nx.set_node_attributes(G, user_names, "label")

    # Filter by top degree if requested
    if max_nodes is not None and max_nodes < G.number_of_nodes():
        # Use total degree (in + out) for filtering
        degrees = dict(G.degree())
        top_nodes = sorted(degrees, key=degrees.get, reverse=True)[:max_nodes]
        G = G.subgraph(top_nodes).copy()
        print(f"Reduced graph to {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")

    print(f"Loaded graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


# =============================================================================
#  Layout algorithms
# =============================================================================

def compute_layout(G, method="fa2"):
    """Compute node positions using a scalable layout algorithm."""
    if method == "fa2":
        try:
            from fa2 import ForceAtlas2
            import scipy.sparse as sp
        except ImportError:
            print("fa2 or scipy not installed, falling back to spring layout.")
            method = "spring"
        else:
            print("Computing ForceAtlas2 layout...")
            # Convert to undirected – fa2 needs a symmetric matrix
            G_undirected = G.to_undirected()
            A = nx.to_scipy_sparse_array(G_undirected, dtype='f', format='csr')

            forceatlas2 = ForceAtlas2(
                outboundAttractionDistribution=True,
                linLogMode=True,
                verbose=True,
                gravity=1.0,
                scalingRatio=2.0,
            )

            positions = forceatlas2.forceatlas2(A, pos=None, iterations=100)

            # Map (x,y) array back to the original node IDs
            node_list = list(G.nodes())
            pos = {node_list[i]: (float(positions[i][0]), float(positions[i][1]))
                   for i in range(len(node_list))}
            return pos

    if method == "spring":
        print("Computing spring layout...")
        pos = nx.spring_layout(G, seed=42, k=0.5, iterations=50)
    elif method == "random":
        print("Using random layout")
        pos = {n: (np.random.rand(), np.random.rand()) for n in G.nodes()}
    else:
        raise ValueError(f"Unknown layout method: {method}")
    return pos

# =============================================================================
#  Backend: pyvis interactive HTML
# =============================================================================

def draw_pyvis(G, pos, output_path, physics_enabled=True):
    """Create an interactive HTML graph using pyvis."""
    try:
        from pyvis.network import Network
    except ImportError:
        print("pyvis is not installed. Install with: pip install pyvis")
        sys.exit(1)

    net = Network(height="800px", width="100%", directed=True, notebook=False)
    net.toggle_physics(physics_enabled)
    # Set physics parameters for better performance with many nodes
    net.set_options("""
    var options = {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.005,
          "springLength": 100,
          "springConstant": 0.18,
          "damping": 0.4,
          "avoidOverlap": 0.5
        },
        "solver": "forceAtlas2Based"
      }
    }
    """)

    # Add nodes
    for node_id in G.nodes():
        label = G.nodes[node_id].get("label", node_id)
        x, y = pos.get(node_id, (None, None))
        net.add_node(node_id, label=f"{label} ({node_id})", x=x, y=y)

    # Add edges
    for src, dst in G.edges():
        net.add_edge(src, dst)

    net.save_graph(str(output_path))
    print(f"Interactive HTML saved to {output_path}")


# =============================================================================
#  Backend: datashader static image (scalable)
# =============================================================================

def draw_datashader(G, pos, output_path, cmap=None, bgcolor="black"):
    """Render a high-resolution static graph using datashader."""
    try:
        import datashader as ds
        import datashader.transfer_functions as tf
        from datashader.bundling import hammer_bundle
    except ImportError:
        print("datashader not installed. Install with: pip install datashader")
        sys.exit(1)

    # Try to use colorcet if available, otherwise a simple built-in list
    if cmap is None:
        try:
            import colorcet as cc
            cmap = cc.fire   # colorcet's fire palette
        except ImportError:
            cmap = ["black","darkblue","blue","cyan","green","yellow","orange","red","white"]

    import pandas as pd

    node_list = list(G.nodes())
    xs = [pos[n][0] for n in node_list]
    ys = [pos[n][1] for n in node_list]
    nodes_df = pd.DataFrame({"x": xs, "y": ys}, index=pd.Index(node_list, name="id"))

    edges = list(G.edges())
    edges_df = pd.DataFrame(edges, columns=["source", "target"])

    print("Bundling edges (this may take a while)...")
    bundled = hammer_bundle(nodes_df, edges_df, tension=0.3, accuracy=500)

    canvas = ds.Canvas(plot_width=1000, plot_height=1000)

    # Edge image
    agg = canvas.line(bundled, "x", "y", agg=ds.count())
    img = tf.shade(agg, cmap=cmap, how="eq_hist")

    # Node overlay
    node_agg = canvas.points(nodes_df, "x", "y")
    node_img = tf.shade(node_agg, cmap=["white","red"], how="eq_hist")

    combined = tf.stack(img, node_img)
    combined = tf.set_background(combined, bgcolor)

    # Export
    if output_path.suffix.lower() == ".png":
        ds.utils.export_image(combined, str(output_path))
        print(f"Static image saved to {output_path}")
    else:
        # fallback to PNG just in case
        ds.utils.export_image(combined, str(output_path.with_suffix(".png")))

# =============================================================================
#  Backend: GEXF export for Gephi
# =============================================================================

def export_gexf(G, output_path):
    """Save the graph in GEXF format for Gephi."""
    nx.write_gexf(G, output_path)
    print(f"GEXF file saved to {output_path}")


# =============================================================================
#  Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Visualise Vasl social graph")
    parser.add_argument("--db", default="vasl_crawler.db", help="SQLite database path")
    parser.add_argument("--max-nodes", type=int, default=2000,
                        help="Only keep top N nodes by degree (0 for no limit)")
    parser.add_argument("--layout", choices=["fa2", "spring", "random"], default="fa2",
                        help="Layout algorithm (fa2 is fast and scalable)")
    parser.add_argument("--backend", choices=["pyvis", "datashader", "gexf"], default="pyvis",
                        help="Backend: pyvis=interactive HTML, datashader=static image, gexf=export")
    parser.add_argument("--output", help="Output file path (default: graph.html)")
    args = parser.parse_args()

    # Determine output filename
    if args.output:
        out = Path(args.output)
    else:
        ext = {"pyvis": "html", "datashader": "png", "gexf": "gexf"}[args.backend]
        out = Path(f"graph.{ext}")

    max_nodes = args.max_nodes if args.max_nodes > 0 else None

    # Load graph
    G = load_graph(args.db, max_nodes)
    if G.number_of_nodes() == 0:
        print("Graph is empty. Exiting.")
        return

    # For GEXF we don't need a layout
    if args.backend == "gexf":
        export_gexf(G, out)
        return

    # Compute layout
    pos = compute_layout(G, args.layout)

    # Draw with selected backend
    if args.backend == "pyvis":
        draw_pyvis(G, pos, out, physics_enabled=(G.number_of_nodes() < 5000))
    elif args.backend == "datashader":
        draw_datashader(G, pos, out)
    else:
        print("Unknown backend")
        sys.exit(1)


if __name__ == "__main__":
    main()
