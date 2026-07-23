"""Guard against DEF_KEYS drifting between the pipeline and the exporter.

DEF_KEYS is defined in two places that must stay identical:
  - src/pipelines/scenario.py     (builds gold_nfl_defense_stats / gold_scenario_input)
  - exporter/build_app_data.py    (filters def_stats before shipping to the app)

If they diverge, the app keeps/drops different defensive stats than the pipeline
scored against — a silent scoring mismatch, not an error. We can't import
scenario.py here (it needs dlt/pyspark), so extract each literal via ast.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _def_keys(rel_path):
    tree = ast.parse((ROOT / rel_path).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "DEF_KEYS" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"DEF_KEYS assignment not found in {rel_path}")


def test_def_keys_match_between_pipeline_and_exporter():
    pipeline = _def_keys("src/pipelines/scenario.py")
    exporter = _def_keys("exporter/build_app_data.py")
    assert pipeline == exporter, (
        "DEF_KEYS drifted between scenario.py and build_app_data.py:\n"
        f"  only in pipeline: {sorted(set(pipeline) - set(exporter))}\n"
        f"  only in exporter: {sorted(set(exporter) - set(pipeline))}"
    )
