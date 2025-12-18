import argparse
from typing import Dict, List, Tuple, cast
import json
import os

import pyarrow as pa
import pyarrow.ipc as ipc
import polars as pl


_NT_COLS: List[Tuple[str, str]] = [
	("GABA", "gaba_avg"),
	("Acetylcholine", "ach_avg"),
	("Glutamate", "glut_avg"),
	("Octopamine", "oct_avg"),
	("Serotonin", "ser_avg"),
	("Dopamine", "da_avg"),
]


def _parse_args() -> argparse.Namespace:
	p = argparse.ArgumentParser(
		description="Summarize the FlyWire proofread connections table (streaming via PyArrow)"
	)
	p.add_argument(
		"--input",
		"-i",
		default="data/raw/proofread_connections_783.feather",
		help="Path to input Feather/IPC file",
	)
	p.add_argument(
		"--max-rows",
		type=int,
		default=None,
		help="Optional max number of rows to scan (for quick tests)",
	)
	p.add_argument(
		"--quantiles",
		default="0.25,0.5,0.75,0.9,0.95,0.99",
		help="Comma-separated quantiles for syn_count (values in [0,1])",
	)
	p.add_argument(
		"--out-json",
		default=None,
		help="Optional path to write a JSON summary (includes syn_count histogram)",
	)
	return p.parse_args()


def _parse_quantiles(spec: str) -> List[float]:
	quantiles: list[float] = []
	for part in spec.split(","):
		part = part.strip()
		if not part:
			continue
		q = float(part)
		if q < 0.0 or q > 1.0:
			raise ValueError(f"Invalid quantile {q}; must be in [0,1]")
		quantiles.append(q)
	if not quantiles:
		return [0.25, 0.5, 0.75]
	return quantiles


def _nearest_quantile_from_hist(hist: Dict[int, int], q: float, total: int) -> int:
	"""Exact quantile for discrete integer data using a histogram.

	Uses a 'nearest' style index: round(q*(total-1)).
	"""

	if total <= 0:
		raise ValueError("Cannot compute quantile with total=0")
	target_index = int(round(q * (total - 1)))
	running = 0
	for value in sorted(hist.keys()):
		running += hist[value]
		if running - 1 >= target_index:
			return value
	return max(hist.keys())


def main() -> None:
	args = _parse_args()
	path = args.input
	quantiles = _parse_quantiles(args.quantiles)

	pre_ids: set[int] = set()
	post_ids: set[int] = set()
	neuropils: set[str] = set()
	syn_hist: Dict[int, int] = {}

	# Only needed for the interactive dashboard JSON.
	neuropil_syn_hist: Dict[str, Dict[int, int]] | None = {} if args.out_json is not None else None
	# Needed for coloring circles by dominant neurotransmitter at a given threshold.
	# Structure: neuropil -> neurotransmitter -> syn_count -> count
	neuropil_nt_syn_hist: Dict[str, Dict[str, Dict[int, int]]] | None = (
		{} if args.out_json is not None else None
	)
	# Global breakdown for interactive filtering (no neuropil): neurotransmitter -> syn_count -> count
	global_nt_syn_hist: Dict[str, Dict[int, int]] | None = (
		{} if args.out_json is not None else None
	)

	total_rows = 0

	with open(path, "rb") as f:
		reader = ipc.open_file(f)
		print("Input:", path)
		print("Schema:", reader.schema)

		for i in range(reader.num_record_batches):
			if args.max_rows is not None and total_rows >= args.max_rows:
				break

			batch = reader.get_batch(i)
			if args.max_rows is not None:
				remaining = args.max_rows - total_rows
				if remaining <= 0:
					break
				if batch.num_rows > remaining:
					batch = batch.slice(0, remaining)

			total_rows += batch.num_rows

			# Unique counts (exact, streaming): update sets using per-batch uniques
			t_full = pa.Table.from_batches([batch]).select(
				["pre_pt_root_id", "post_pt_root_id", "neuropil", "syn_count"]
			)
			df_full = cast(pl.DataFrame, pl.from_arrow(t_full))

			# ---- Unique IDs (exact, streaming-safe) ----
			pre_ids.update(
				df_full.select("pre_pt_root_id")
				.drop_nulls()
				.unique()
				.to_series()
				.to_list()
			)

			post_ids.update(
				df_full.select("post_pt_root_id")
				.drop_nulls()
				.unique()
				.to_series()
				.to_list()
			)

			neuropils.update(
				df_full.select("neuropil")
				.drop_nulls()
				.unique()
				.to_series()
				.to_list()
			)

			# ---- syn_count histogram ----
			g_syn = (
				df_full.select("syn_count")
				.drop_nulls()
				.group_by("syn_count")
				.len()
			)

			for syn_count, count in g_syn.iter_rows():
				syn = int(syn_count)
				syn_hist[syn] = syn_hist.get(syn, 0) + int(count)

			# Per-neuropil syn_count histogram (for circle dashboard).
			if neuropil_syn_hist is not None:
				# Group counts by (neuropil, syn_count) within this record batch.
				t = pa.Table.from_batches([batch]).select(["neuropil", "syn_count"])
				df = cast(pl.DataFrame, pl.from_arrow(t))
				df = df.drop_nulls(["neuropil", "syn_count"])
				g = (
					df.group_by(["neuropil", "syn_count"])  # type: ignore[attr-defined]
					.len()
					.rename({"len": "count"})
				)
				for neuropil, syn_count, count in g.iter_rows():
					neuropil_key = str(neuropil)
					syn = int(syn_count)
					neuropil_syn_hist.setdefault(neuropil_key, {})
					neuropil_syn_hist[neuropil_key][syn] = neuropil_syn_hist[neuropil_key].get(syn, 0) + int(count)

			# Per-neuropil-per-neurotransmitter syn_count histogram.
			# This enables determining the dominant neurotransmitter *after* applying the synapse-count threshold.
			if neuropil_nt_syn_hist is not None:
				select_cols = ["neuropil", "syn_count"] + [col for _, col in _NT_COLS]
				t2 = pa.Table.from_batches([batch]).select(select_cols)
				df2 = cast(pl.DataFrame, pl.from_arrow(t2)).drop_nulls(["neuropil", "syn_count"])

				# Compute dominant neurotransmitter list (ties allowed) based on max across *_avg columns.
				max_expr = pl.max_horizontal([pl.col(col) for _, col in _NT_COLS]).alias("_nt_max")
				df2 = df2.with_columns([max_expr])
				dom_list = pl.concat_list(
					[
						pl.when(pl.col(col) == pl.col("_nt_max")).then(pl.lit(name)).otherwise(None)
						for name, col in _NT_COLS
					]
				).list.drop_nulls().alias("dominant_nt_list")
				df2 = df2.with_columns([dom_list]).select(["neuropil", "syn_count", "dominant_nt_list"])
				df2 = df2.explode("dominant_nt_list").drop_nulls(["dominant_nt_list"])  # count ties toward each

				g2 = (
					df2.group_by(["neuropil", "dominant_nt_list", "syn_count"])  # type: ignore[attr-defined]
					.len()
					.rename({"len": "count"})
				)
				for neuropil, nt, syn_count, count in g2.iter_rows():
					neuropil_key = str(neuropil)
					nt_key = str(nt)
					syn = int(syn_count)
					neuropil_nt_syn_hist.setdefault(neuropil_key, {})
					neuropil_nt_syn_hist[neuropil_key].setdefault(nt_key, {})
					cur = neuropil_nt_syn_hist[neuropil_key][nt_key].get(syn, 0)
					neuropil_nt_syn_hist[neuropil_key][nt_key][syn] = cur + int(count)

				if global_nt_syn_hist is not None:
					g3 = (
						df2.select(["dominant_nt_list", "syn_count"])
						.group_by(["dominant_nt_list", "syn_count"])  # type: ignore[attr-defined]
						.len()
						.rename({"len": "count"})
					)
					for nt, syn_count, count in g3.iter_rows():
						nt_key = str(nt)
						syn = int(syn_count)
						global_nt_syn_hist.setdefault(nt_key, {})
						global_nt_syn_hist[nt_key][syn] = global_nt_syn_hist[nt_key].get(syn, 0) + int(count)

			if (i + 1) % 10 == 0:
				print(f"  scanned {total_rows} rows...")

	print("Total rows:", total_rows)
	print("Unique pre_pt_root_id:", len(pre_ids))
	print("Unique post_pt_root_id:", len(post_ids))
	print("Unique neuron ids (pre ∪ post):", len(pre_ids | post_ids))
	print("Unique neuropil:", len(neuropils))

	if syn_hist:
		syn_min = min(syn_hist.keys())
		syn_max = max(syn_hist.keys())
		print("syn_count range:", syn_min, "..", syn_max)

		median = _nearest_quantile_from_hist(syn_hist, 0.5, total_rows)
		print("syn_count median:", median)

		print("syn_count quantiles:")
		for q in quantiles:
			print(f"  q={q}: {_nearest_quantile_from_hist(syn_hist, q, total_rows)}")
	else:
		print("syn_count: no data")

	# Optional: write an offline JSON summary for dashboards/API.
	if args.out_json is not None:
		out_path = args.out_json
		out_dir = os.path.dirname(out_path)
		if out_dir:
			os.makedirs(out_dir, exist_ok=True)

		hist_items = [
			{"syn_count": int(v), "count": int(c)}
			for v, c in sorted(syn_hist.items(), key=lambda x: x[0])
		]

		by_neuropil: Dict[str, Dict[str, object]] = {}
		if neuropil_syn_hist is not None:
			for neuropil, h in sorted(neuropil_syn_hist.items(), key=lambda x: x[0]):
				h_items = [
					{"syn_count": int(v), "count": int(c)}
					for v, c in sorted(h.items(), key=lambda x: x[0])
				]
				by_neuropil[neuropil] = {
					"total_pairs": int(sum(h.values())),
					"histogram": h_items,
				}

		# Attach per-neuropil per-neurotransmitter histograms (optional but used by dashboard coloring).
		if neuropil_nt_syn_hist is not None:
			for neuropil, by_nt in neuropil_nt_syn_hist.items():
				if neuropil not in by_neuropil:
					# If for some reason neuropil histogram is missing, still create the container.
					by_neuropil[neuropil] = {"total_pairs": 0, "histogram": []}

				by_neurotransmitter_neuropil: Dict[str, Dict[str, object]] = {}
				for nt_name, h in by_nt.items():
					h_items = [
						{"syn_count": int(v), "count": int(c)}
						for v, c in sorted(h.items(), key=lambda x: x[0])
					]
					by_neurotransmitter_neuropil[nt_name] = {
						"total_pairs": int(sum(h.values())),
						"histogram": h_items,
					}
				by_neuropil[neuropil]["by_neurotransmitter"] = by_neurotransmitter_neuropil

		# Global by-neurotransmitter histograms (used for filtering the bar chart and circles).
		by_neurotransmitter: Dict[str, Dict[str, object]] = {}
		if global_nt_syn_hist is not None:
			for nt_name, h in sorted(global_nt_syn_hist.items(), key=lambda x: x[0]):
				h_items = [
					{"syn_count": int(v), "count": int(c)}
					for v, c in sorted(h.items(), key=lambda x: x[0])
				]
				by_neurotransmitter[nt_name] = {
					"total_pairs": int(sum(h.values())),
					"histogram": h_items,
				}

		summary = {
			"input": path,
			"max_rows": args.max_rows,
			"total_rows": total_rows,
			"neurotransmitters": [
				"GABA",
				"Acetylcholine",
				"Glutamate",
				"Octopamine",
				"Serotonin",
				"Dopamine",
			],
			"unique": {
				"pre_pt_root_id": len(pre_ids),
				"post_pt_root_id": len(post_ids),
				"neuron_ids_union": len(pre_ids | post_ids),
				"neuropil": len(neuropils),
			},
			"syn_count": {
				"min": int(min(syn_hist.keys())) if syn_hist else None,
				"max": int(max(syn_hist.keys())) if syn_hist else None,
				"median": int(_nearest_quantile_from_hist(syn_hist, 0.5, total_rows)) if syn_hist else None,
				"quantiles": {
					str(q): int(_nearest_quantile_from_hist(syn_hist, q, total_rows))
					for q in quantiles
				}
				if syn_hist
				else {},
				"histogram": hist_items,
			},
			"by_neuropil": by_neuropil,
			"by_neurotransmitter": by_neurotransmitter,
		}

		with open(out_path, "w", encoding="utf-8") as f:
			json.dump(summary, f, ensure_ascii=False)
		print("Wrote JSON summary:", out_path)


if __name__ == "__main__":
	main()
