# compression_test.py

import numpy as np
import matplotlib.pyplot as plt
import lz4.frame
import io
import time
import pandas as pd
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_srcs.mock_gen import generate_naive_batches
from utils.connection import get_db_connection


# ---------------- HELPERS ---------------- #

def batch_to_tsv_bytes(batch):
    """Serialize a batch to TSV bytes (what you'd insert via copy_from)."""
    df = pd.DataFrame(batch, columns=['symbol', 'price', 'volume', 'ts'])
    f = io.StringIO()
    df.to_csv(f, sep='\t', header=False, index=False)
    return f.getvalue().encode('utf-8')


def compress_lz4(raw_bytes):
    return lz4.frame.compress(raw_bytes, compression_level=0)  # level 0 = fast mode


# ---------------- MAIN TEST ---------------- #

def run_compression_test(duration=5, speed=5):
    raw_sizes = []        # bytes, no compression
    lz4_sizes = []        # bytes, after LZ4
    ratios = []           # compression ratio per batch
    batch_ids = []

    data_stream = generate_naive_batches(duration, speed)

    for i, (batch, v_time, gen_time, total) in enumerate(data_stream):
        raw = batch_to_tsv_bytes(batch)
        compressed = compress_lz4(raw)

        raw_kb = len(raw) / 1024
        lz4_kb = len(compressed) / 1024
        ratio = len(raw) / max(len(compressed), 1)

        raw_sizes.append(raw_kb)
        lz4_sizes.append(lz4_kb)
        ratios.append(ratio)
        batch_ids.append(i)

        print(
            f"Batch {i:>3} | Raw: {raw_kb:6.2f} KB | "
            f"LZ4: {lz4_kb:6.2f} KB | "
            f"Ratio: {ratio:.3f}x | "
            f"Savings: {(1 - lz4_kb/raw_kb)*100:.1f}%"
        )

    return batch_ids, raw_sizes, lz4_sizes, ratios


# ---------------- PLOT ---------------- #

def plot_compression(batch_ids, raw_sizes, lz4_sizes, ratios):
    cumulative_raw = np.cumsum(raw_sizes)
    cumulative_lz4 = np.cumsum(lz4_sizes)

    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle("Compression Analysis: LZ4 vs No Compression", fontsize=14, fontweight='bold')

    # ── Plot 1: Cumulative storage growth ── #
    ax = axes[0]
    ax.plot(batch_ids, cumulative_raw, label="No Compression", color="#1f77b4", linewidth=2)
    ax.plot(batch_ids, cumulative_lz4, label="LZ4 Compression", color="#d62728", linewidth=2)

    # Shade savings area
    ax.fill_between(batch_ids, cumulative_lz4, cumulative_raw,
                    alpha=0.15, color="green", label="Space saved")

    # Avg lines
    ax.axhline(np.mean(cumulative_raw), color="#1f77b4", linestyle='--',
               linewidth=1.2, alpha=0.7, label=f"Raw avg: {np.mean(cumulative_raw):.1f} KB")
    ax.axhline(np.mean(cumulative_lz4), color="#d62728", linestyle='--',
               linewidth=1.2, alpha=0.7, label=f"LZ4 avg: {np.mean(cumulative_lz4):.1f} KB")

    total_saving_pct = (1 - cumulative_lz4[-1] / cumulative_raw[-1]) * 100
    ax.set_title(f"Cumulative Storage Growth  |  Total saving: {total_saving_pct:.1f}%",
                 fontweight='bold')
    ax.set_xlabel("Batch #")
    ax.set_ylabel("Cumulative Size (KB)")
    ax.legend(fontsize=9, ncol=2)
    ax.grid(True, alpha=0.3)

    # ── Plot 2: Per-batch compression ratio ── #
    ax = axes[1]
    ax.plot(batch_ids, ratios, color="#2ca02c", linewidth=1.5, alpha=0.7, label="Ratio per batch")
    ax.axhline(np.mean(ratios), color="#2ca02c", linestyle='--', linewidth=1.8,
               label=f"Mean ratio: {np.mean(ratios):.3f}x")
    ax.axhline(1.0, color='gray', linestyle=':', linewidth=1.2, label="Break-even (1.0x)")

    ax.set_title("Per-Batch Compression Ratio  (>1 = LZ4 saves space)", fontweight='bold')
    ax.set_xlabel("Batch #")
    ax.set_ylabel("Compression Ratio (raw / compressed)")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    os.makedirs("./plots", exist_ok=True)
    plt.savefig("./plots/compression_comparison.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("\nSaved → ./plots/compression_comparison.png")


# ---------------- ENTRY ── #

if __name__ == "__main__":
    batch_ids, raw_sizes, lz4_sizes, ratios = run_compression_test(duration=5, speed=5)

    print(f"\n{'='*50}")
    print(f"Total Raw:  {sum(raw_sizes):.2f} KB")
    print(f"Total LZ4:  {sum(lz4_sizes):.2f} KB")
    print(f"Overall savings: {(1 - sum(lz4_sizes)/sum(raw_sizes))*100:.1f}%")
    print(f"Mean compression ratio: {np.mean(ratios):.3f}x")

    plot_compression(batch_ids, raw_sizes, lz4_sizes, ratios)