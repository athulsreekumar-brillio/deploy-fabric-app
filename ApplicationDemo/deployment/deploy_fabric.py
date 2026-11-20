import argparse
import ast
import json
import os
import re
import tempfile
import uuid
from pathlib import Path


APPLICATION_DIR = Path(__file__).resolve().parents[1]
PIPELINE_FILE = APPLICATION_DIR / "pipelines" / "customer_360_pipeline.json"
LAKEHOUSE_LOGICAL_ID = str(uuid.uuid5(uuid.NAMESPACE_URL, "hcdm-esdf:Customer360Lakehouse"))
ZERO_GUID = "00000000-0000-0000-0000-000000000000"


def logical_id(item_type, item_name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"hcdm-esdf:{item_type}:{item_name}"))


def write_json(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, indent=2) + "\n", encoding="utf-8")


def platform_metadata(item_type, display_name, item_logical_id, description=None):
    metadata = {
        "type": item_type,
        "displayName": display_name,
    }
    if description:
        metadata["description"] = description
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": metadata,
        "config": {
            "version": "2.0",
            "logicalId": item_logical_id,
        },
    }


def notebook_content(source_path):
    source = source_path.read_text(encoding="utf-8")
    lines = source.splitlines()
    if lines and lines[0].strip() == "# Fabric notebook source":
        lines = lines[1:]
    lines = [line for line in lines if line.strip() != "# Attach Customer360Lakehouse as the default lakehouse before running."]

    parameter_assignment = 'run_id = "local-demo"'
    if lines.count(parameter_assignment) != 1:
        raise ValueError(f"{source_path.name} must contain one run_id assignment")
    lines.remove(parameter_assignment)
    code = "\n".join(lines).strip()

    header = f"""# Fabric notebook source

# METADATA ********************

# META {{
# META   "kernel_info": {{
# META     "name": "synapse_pyspark"
# META   }},
# META   "dependencies": {{
# META     "lakehouse": {{
# META       "default_lakehouse": "{LAKEHOUSE_LOGICAL_ID}",
# META       "default_lakehouse_name": "Customer360Lakehouse",
# META       "default_lakehouse_workspace_id": "{ZERO_GUID}",
# META       "known_lakehouses": [
# META         {{
# META           "id": "{LAKEHOUSE_LOGICAL_ID}"
# META         }}
# META       ]
# META     }}
# META   }}
# META }}

# PARAMETERS CELL ********************

run_id = "local-demo"

# METADATA ********************

# META {{
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }}

# CELL ********************

"""
    return header + code + "\n"


def load_json(path, description):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid {description}: {error}") from error
    if not isinstance(value, dict):
        raise TypeError(f"{description} must be a JSON object")
    return value


def notebook_table_references(source, source_path):
    try:
        tree = ast.parse(source, filename=str(source_path))
    except SyntaxError as error:
        raise ValueError(f"Invalid notebook Python in {source_path.name}: {error}") from error

    reads = set()
    writes = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method = node.func.attr
        if method not in ("table", "saveAsTable"):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
            raise ValueError(
                f"{source_path.name} must use a string literal with spark.{method}()"
            )
        table_name = node.args[0].value
        (reads if method == "table" else writes).add(table_name)
    return reads, writes


def validate_source_definitions():
    pipeline = load_json(PIPELINE_FILE, "pipeline blueprint")
    catalog_file = APPLICATION_DIR / "lakehouse" / "lakehouse-catalog.json"
    catalog = load_json(catalog_file, "lakehouse catalog")

    if pipeline.get("schemaVersion") != "1.0" or pipeline.get("kind") != "fabric-data-pipeline-blueprint":
        raise ValueError("Pipeline schema version or kind is not supported")
    if catalog.get("schemaVersion") != "1.0" or catalog.get("kind") != "fabric-lakehouse-demo-catalog":
        raise ValueError("Lakehouse catalog schema version or kind is not supported")
    if not pipeline.get("name") or pipeline.get("lakehouse", {}).get("logicalName") != catalog.get("name"):
        raise ValueError("Pipeline name and lakehouse catalog binding must be defined and consistent")
    if pipeline.get("parameters", {}).get("run_id", {}).get("type") != "string":
        raise ValueError("Pipeline must declare the string run_id parameter")

    layers = catalog.get("layers")
    if catalog.get("storageFormat") != "Delta" or not isinstance(layers, dict) or not layers:
        raise ValueError("Lakehouse catalog must define Delta storage and at least one layer")

    layer_tables = []
    for layer_name, layer in layers.items():
        if not isinstance(layer_name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", layer_name):
            raise ValueError("Lakehouse layer names must use lowercase snake_case")
        tables = layer.get("tables") if isinstance(layer, dict) else None
        if not isinstance(tables, list) or not tables:
            raise ValueError(f"Lakehouse layer {layer_name} must define tables")
        if any(not isinstance(table, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", table) for table in tables):
            raise ValueError("Lakehouse table names must use lowercase snake_case")
        layer_tables.extend(tables)
    if len(layer_tables) != len(set(layer_tables)):
        raise ValueError("Lakehouse table names must be unique across layers")
    declared_tables = set(layer_tables)

    lineage = catalog.get("lineage")
    if not isinstance(lineage, list) or not lineage:
        raise ValueError("Lakehouse catalog must define table lineage")
    lineage_targets = set()
    lineage_sources_by_target = {}
    for entry in lineage:
        if not isinstance(entry, dict) or not isinstance(entry.get("target"), str):
            raise TypeError("Every lineage entry must have a target table")
        target = entry["target"]
        if target not in declared_tables:
            raise ValueError(f"Lineage target is not declared in a lakehouse layer: {target}")
        if target in lineage_targets:
            raise ValueError(f"Lineage target is declared more than once: {target}")
        lineage_targets.add(target)
        sources = entry.get("source")
        if isinstance(sources, str):
            sources = [sources]
        if not isinstance(sources, list) or not sources:
            raise ValueError(f"Lineage target {target} must define one or more sources")
        for source in sources:
            if not isinstance(source, str) or not source:
                raise ValueError("Lineage sources must be non-empty strings")
            if source not in declared_tables and not source.startswith("synthetic "):
                raise ValueError(f"Lineage source is neither a declared table nor synthetic input: {source}")
        lineage_sources_by_target[target] = {
            source for source in sources if source in declared_tables
        }
    if lineage_targets != declared_tables:
        missing = sorted(declared_tables - lineage_targets)
        raise ValueError(f"Lakehouse tables missing lineage: {', '.join(missing)}")

    lineage_visited = set()
    lineage_active = set()

    def visit_lineage(table):
        if table in lineage_active:
            raise ValueError(f"Lakehouse lineage cycle includes table {table}")
        if table in lineage_visited:
            return
        lineage_active.add(table)
        for source in lineage_sources_by_target[table]:
            visit_lineage(source)
        lineage_active.remove(table)
        lineage_visited.add(table)

    for table in declared_tables:
        visit_lineage(table)

    activities = pipeline.get("activities")
    if not isinstance(activities, list) or not activities:
        raise ValueError("Pipeline must define at least one activity")

    activity_by_name = {}
    notebook_writes = set()
    reads_by_activity = {}
    writer_by_table = {}
    used_notebooks = set()
    for activity in activities:
        if not isinstance(activity, dict):
            raise TypeError("Pipeline activities must be JSON objects")
        name = activity.get("name")
        notebook_name = activity.get("notebook")
        if not isinstance(name, str) or not name or name in activity_by_name:
            raise ValueError("Pipeline activity names must be present and unique")
        if activity.get("type") != "Notebook":
            raise ValueError(f"Activity {name} must be a Notebook activity")
        if activity.get("parameters", {}).get("run_id") != "@pipeline().RunId":
            raise ValueError(f"Activity {name} must pass the pipeline run_id to its notebook")
        if not isinstance(notebook_name, str) or not notebook_name or notebook_name in used_notebooks:
            raise ValueError("Notebook references must be present and unique")
        used_notebooks.add(notebook_name)
        notebook_path = APPLICATION_DIR / "notebooks" / (notebook_name + ".py")
        if not notebook_path.is_file():
            raise ValueError(f"Notebook source not found: {notebook_path}")
        source = notebook_path.read_text(encoding="utf-8")
        if source.count('run_id = "local-demo"') != 1:
            raise ValueError(f"{notebook_path.name} must define exactly one run_id parameter")
        reads, writes = notebook_table_references(source, notebook_path)
        unknown_tables = (reads | writes) - declared_tables
        if unknown_tables:
            raise ValueError(
                f"{notebook_path.name} references tables missing from the lakehouse catalog: "
                f"{', '.join(sorted(unknown_tables))}"
            )
        notebook_writes.update(writes)
        reads_by_activity[name] = reads
        for table in writes:
            if table in writer_by_table:
                raise ValueError(f"Lakehouse table {table} is written by multiple notebooks")
            writer_by_table[table] = name
        activity_by_name[name] = activity

        timeout = activity.get("timeoutMinutes")
        retries = activity.get("retry")
        if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
            raise ValueError(f"Activity {name} must have a positive integer timeoutMinutes")
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise ValueError(f"Activity {name} must have a non-negative integer retry count")

    available_notebooks = {
        notebook_path.stem
        for notebook_path in (APPLICATION_DIR / "notebooks").glob("*.py")
    }
    if available_notebooks != used_notebooks:
        missing = sorted(used_notebooks - available_notebooks)
        orphaned = sorted(available_notebooks - used_notebooks)
        raise ValueError(
            f"Notebook files and pipeline references differ (missing: "
            f"{', '.join(missing) or 'none'}; unreferenced: "
            f"{', '.join(orphaned) or 'none'})"
        )

    if notebook_writes != declared_tables:
        missing = sorted(declared_tables - notebook_writes)
        unlisted = sorted(notebook_writes - declared_tables)
        raise ValueError(
            f"Notebook writes do not match catalog tables (missing: "
            f"{', '.join(missing) or 'none'}; unlisted: "
            f"{', '.join(unlisted) or 'none'})"
        )

    for activity in activities:
        dependencies = activity.get("dependsOn", [])
        if not isinstance(dependencies, list):
            raise TypeError("Activity dependencies must be a list")
        dependency_names = set()
        for dependency in dependencies:
            if not isinstance(dependency, dict):
                raise TypeError("Dependencies must be objects with activity and condition")
            dependency_name = dependency.get("activity")
            if dependency_name not in activity_by_name:
                raise ValueError(
                    f"{activity['name']} depends on unknown activity {dependency_name}"
                )
            if dependency_name == activity["name"] or dependency_name in dependency_names:
                raise ValueError(f"Activity {activity['name']} has a self or duplicate dependency")
            dependency_names.add(dependency_name)
            if dependency.get("condition") not in ("Succeeded", "Failed", "Skipped", "Completed"):
                raise ValueError("Activity dependency has an unsupported condition")

    visited = set()
    active = set()

    def visit(name):
        if name in active:
            raise ValueError(f"Pipeline dependency cycle includes activity {name}")
        if name in visited:
            return
        active.add(name)
        for dependency in activity_by_name[name].get("dependsOn", []):
            visit(dependency["activity"])
        active.remove(name)
        visited.add(name)

    for activity_name in activity_by_name:
        visit(activity_name)

    if set(writer_by_table) != declared_tables:
        raise ValueError("Every catalog table must have exactly one notebook writer")

    for table, writer in writer_by_table.items():
        if not lineage_sources_by_target[table].issubset(reads_by_activity[writer]):
            raise ValueError(
                f"Lakehouse lineage for {table} does not match notebook inputs"
            )

    ancestors_by_activity = {}

    def ancestors(name):
        if name in ancestors_by_activity:
            return ancestors_by_activity[name]
        result = set()
        for dependency in activity_by_name[name].get("dependsOn", []):
            dependency_name = dependency["activity"]
            result.add(dependency_name)
            result.update(ancestors(dependency_name))
        ancestors_by_activity[name] = result
        return result

    for activity_name, reads in reads_by_activity.items():
        available_activities = ancestors(activity_name)
        for table in reads:
            writer = writer_by_table.get(table)
            if writer != activity_name and writer not in available_activities:
                raise ValueError(
                    f"Pipeline activity {activity_name} reads {table} before its writer activity {writer}"
                )
    return pipeline


def build_workspace(repository_directory):
    pipeline = validate_source_definitions()
    activities = pipeline["activities"]

    activity_names = {activity["name"] for activity in activities}
    notebooks = {}
    for activity in activities:
        notebook_name = activity.get("notebook")
        if not notebook_name or notebook_name in notebooks:
            raise ValueError("Notebook references must be present and unique")
        notebook_path = APPLICATION_DIR / "notebooks" / (notebook_name + ".py")
        if not notebook_path.is_file():
            raise FileNotFoundError(f"Notebook source not found: {notebook_path}")
        notebook_id = logical_id("Notebook", notebook_name)
        notebooks[notebook_name] = notebook_id

        notebook_directory = repository_directory / (notebook_name + ".Notebook")
        write_json(
            notebook_directory / ".platform",
            platform_metadata("Notebook", notebook_name, notebook_id),
        )
        (notebook_directory / "notebook-content.py").write_text(
            notebook_content(notebook_path), encoding="utf-8"
        )

    lakehouse_directory = repository_directory / "Customer360Lakehouse.Lakehouse"
    write_json(
        lakehouse_directory / ".platform",
        platform_metadata(
            "Lakehouse",
            "Customer360Lakehouse",
            LAKEHOUSE_LOGICAL_ID,
            "Customer 360 demo lakehouse",
        ),
    )
    write_json(lakehouse_directory / "lakehouse.metadata.json", {})

    native_activities = []
    for activity in activities:
        notebook_name = activity["notebook"]
        dependencies = []
        for dependency in activity.get("dependsOn", []):
            dependency_name = dependency["activity"]
            if dependency_name not in activity_names:
                raise ValueError(
                    f"{activity['name']} depends on unknown activity {dependency_name}"
                )
            dependencies.append(
                {
                    "activity": dependency_name,
                    "dependencyConditions": [dependency.get("condition", "Succeeded")],
                }
            )

        timeout_minutes = int(activity.get("timeoutMinutes", 30))
        hours, minutes = divmod(timeout_minutes, 60)
        native_activities.append(
            {
                "name": activity["name"],
                "type": "TridentNotebook",
                "dependsOn": dependencies,
                "policy": {
                    "timeout": f"0.{hours:02d}:{minutes:02d}:00",
                    "retry": int(activity.get("retry", 0)),
                    "retryIntervalInSeconds": 30,
                    "secureInput": False,
                    "secureOutput": False,
                },
                "typeProperties": {
                    "notebookId": notebooks[notebook_name],
                    "workspaceId": ZERO_GUID,
                    "parameters": {
                        "run_id": {
                            "value": "@pipeline().RunId",
                            "type": "Expression",
                        }
                    },
                },
            }
        )

    pipeline_directory = repository_directory / (pipeline["name"] + ".DataPipeline")
    write_json(
        pipeline_directory / ".platform",
        platform_metadata(
            "DataPipeline",
            pipeline["name"],
            logical_id("DataPipeline", pipeline["name"]),
            pipeline.get("description"),
        ),
    )
    write_json(
        pipeline_directory / "pipeline-content.json",
        {"properties": {"activities": native_activities}},
    )


def validate_workspace(repository_directory):
    seen_logical_ids = set()
    item_directories = [
        path for path in repository_directory.iterdir() if path.is_dir()
    ]
    expected_notebooks = {
        activity["notebook"] for activity in validate_source_definitions()["activities"]
    }
    catalog = load_json(
        APPLICATION_DIR / "lakehouse" / "lakehouse-catalog.json",
        "lakehouse catalog",
    )
    pipeline = load_json(PIPELINE_FILE, "pipeline blueprint")
    expected_names = {
        catalog["name"] + ".Lakehouse",
        pipeline["name"] + ".DataPipeline",
    }
    expected_names.update(name + ".Notebook" for name in expected_notebooks)
    if {path.name for path in item_directories} != expected_names:
        raise ValueError("Generated Fabric items do not match the application definitions")

    for item_directory in item_directories:
        platform_file = item_directory / ".platform"
        if not platform_file.is_file():
            raise ValueError(f"Missing .platform file: {item_directory.name}")
        platform = load_json(platform_file, platform_file.name)
        if platform.get("metadata", {}).get("displayName") != item_directory.name.rsplit(".", 1)[0]:
            raise ValueError(f"Fabric item name does not match platform metadata: {item_directory.name}")
        item_type = item_directory.name.rsplit(".", 1)[-1]
        if platform.get("metadata", {}).get("type") != item_type:
            raise ValueError(f"Fabric item type does not match platform metadata: {item_directory.name}")
        item_logical_id = platform["config"]["logicalId"]
        try:
            uuid.UUID(item_logical_id)
        except (ValueError, TypeError) as error:
            raise ValueError(f"Fabric item has an invalid logicalId: {item_directory.name}") from error
        if item_logical_id in seen_logical_ids:
            raise ValueError(f"Duplicate logicalId: {item_logical_id}")
        seen_logical_ids.add(item_logical_id)

        if item_directory.name.endswith(".Notebook"):
            source = (item_directory / "notebook-content.py").read_text(
                encoding="utf-8"
            )
            notebook_table_references(source, item_directory / "notebook-content.py")
            if '"default_lakehouse_name": "Customer360Lakehouse"' not in source:
                raise ValueError(f"Notebook is not bound to Customer360Lakehouse: {item_directory.name}")
        elif item_directory.name.endswith(".Lakehouse"):
            load_json(item_directory / "lakehouse.metadata.json", "Fabric Lakehouse metadata")
        elif item_directory.name.endswith(".DataPipeline"):
            definition = load_json(item_directory / "pipeline-content.json", "Fabric pipeline definition")
            activities = definition.get("properties", {}).get("activities")
            if not isinstance(activities, list) or not activities:
                raise ValueError("Fabric pipeline must contain activities")
            names = {
                activity.get("name")
                for activity in activities
            }
            if len(names) != len(activities) or None in names:
                raise ValueError("Fabric pipeline activity names must be present and unique")
            for activity in activities:
                for dependency in activity["dependsOn"]:
                    if dependency["activity"] not in names:
                        raise ValueError(
                            f"Unknown dependency in {item_directory.name}"
                        )
                properties = activity.get("typeProperties", {})
                notebook_id = properties.get("notebookId")
                notebook_name = activity.get("name")
                expected_notebook_id = next(
                    (
                        logical_id("Notebook", definition["notebook"])
                        for definition in pipeline["activities"]
                        if definition["name"] == notebook_name
                    ),
                    None,
                )
                if notebook_id != expected_notebook_id:
                    raise ValueError(f"Fabric pipeline references the wrong notebook for {notebook_name}")
                if properties.get("workspaceId") != ZERO_GUID:
                    raise ValueError("Fabric pipeline workspace references must use the deployment placeholder")


def deploy(repository_directory):
    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID")
    if not workspace_id:
        raise ValueError("Set the GitHub Actions variable FABRIC_WORKSPACE_ID")

    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items

    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=os.environ.get("FABRIC_ENVIRONMENT", "DEV"),
        repository_directory=str(repository_directory),
        item_type_in_scope=["Lakehouse", "Notebook", "DataPipeline"],
        token_credential=AzureCliCredential(),
    )
    publish_all_items(workspace)


def main():
    parser = argparse.ArgumentParser(
        description="Build and publish the Customer 360 Fabric demo."
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Build and validate Fabric-native item files without signing in.",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="fabric-customer360-") as directory:
        repository_directory = Path(directory)
        build_workspace(repository_directory)
        validate_workspace(repository_directory)
        if args.validate_only:
            print("Fabric item package validated.")
        else:
            deploy(repository_directory)


if __name__ == "__main__":
    main()
