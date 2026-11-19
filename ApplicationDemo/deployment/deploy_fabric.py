import argparse
import json
import os
import tempfile
import uuid
from pathlib import Path


APPLICATION_DIR = Path(__file__).resolve().parents[1]
PIPELINE_FILE = APPLICATION_DIR / "pipelines" / "customer_360_pipeline.json"
LAKEHOUSE_LOGICAL_ID = str(uuid.uuid5(uuid.NAMESPACE_URL, "hcdm-esdf:Customer360Lakehouse"))
ZERO_GUID = "00000000-0000-0000-0000-000000000000"


def logical_id(item_type, item_name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "hcdm-esdf:{}:{}".format(item_type, item_name)))


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
        raise ValueError("{} must contain one run_id assignment".format(source_path.name))
    lines.remove(parameter_assignment)
    code = "\n".join(lines).strip()

    header = """# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "%s",
# META       "default_lakehouse_name": "Customer360Lakehouse",
# META       "default_lakehouse_workspace_id": "%s",
# META       "known_lakehouses": [
# META         {
# META           "id": "%s"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

run_id = "local-demo"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

""" % (LAKEHOUSE_LOGICAL_ID, ZERO_GUID, LAKEHOUSE_LOGICAL_ID)
    return header + code + "\n"


def build_workspace(repository_directory):
    pipeline = json.loads(PIPELINE_FILE.read_text(encoding="utf-8"))
    activities = pipeline.get("activities")
    if not isinstance(activities, list) or not activities:
        raise ValueError("The pipeline must define at least one activity")

    activity_names = {activity["name"] for activity in activities}
    notebooks = {}
    for activity in activities:
        notebook_name = activity.get("notebook")
        if not notebook_name or notebook_name in notebooks:
            raise ValueError("Notebook references must be present and unique")
        notebook_path = APPLICATION_DIR / "notebooks" / (notebook_name + ".py")
        if not notebook_path.is_file():
            raise FileNotFoundError("Notebook source not found: {}".format(notebook_path))
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
                    "{} depends on unknown activity {}".format(
                        activity["name"], dependency_name
                    )
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
                    "timeout": "0.{:02d}:{:02d}:00".format(hours, minutes),
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
    if len(item_directories) != 7:
        raise ValueError("Expected one Lakehouse, five Notebooks, and one DataPipeline")

    for item_directory in item_directories:
        platform_file = item_directory / ".platform"
        if not platform_file.is_file():
            raise ValueError("Missing .platform file: {}".format(item_directory.name))
        platform = json.loads(platform_file.read_text(encoding="utf-8"))
        item_logical_id = platform["config"]["logicalId"]
        if item_logical_id in seen_logical_ids:
            raise ValueError("Duplicate logicalId: {}".format(item_logical_id))
        seen_logical_ids.add(item_logical_id)

        if item_directory.name.endswith(".Notebook"):
            source = (item_directory / "notebook-content.py").read_text(
                encoding="utf-8"
            )
            compile(source, str(item_directory / "notebook-content.py"), "exec")
        elif item_directory.name.endswith(".DataPipeline"):
            definition = json.loads(
                (item_directory / "pipeline-content.json").read_text(
                    encoding="utf-8"
                )
            )
            names = {
                activity["name"]
                for activity in definition["properties"]["activities"]
            }
            for activity in definition["properties"]["activities"]:
                for dependency in activity["dependsOn"]:
                    if dependency["activity"] not in names:
                        raise ValueError(
                            "Unknown dependency in {}".format(item_directory.name)
                        )


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
