
from pathlib import Path
import sys

# Find project directory
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

current = Path("/Workspace" + notebook_path)

project_root = next(
    p for p in current.parents
    if p.name == "enterprise-ecommerce-platform"
)

sys.path.insert(0, str(project_root / "src"))

from ecommerce.metadata import MetadataRegistry

# Load YAML registry
registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

# Check all 9 datasets
results = []

for ds in registry.all_datasets():
    results.append({
        "dataset": ds.name,
        "bronze_table": ds.full_bronze_table,
        "silver_table": ds.full_silver_table,
        "load_type": ds.load_type,
        "columns": len(ds.target_struct_type.fields),
        "status": "PASS"
    })

display(spark.createDataFrame(results))

assert len(registry) == 9
assert len({d.checkpoint_location for d in registry.all_datasets()}) == 9
assert len({d.schema_location for d in registry.all_datasets()}) == 9

print("SUCCESS: All 9 metadata contracts loaded.")
