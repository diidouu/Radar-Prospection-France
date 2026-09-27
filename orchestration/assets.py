from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from pathlib import Path

from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, AssetKey

import ingest
RACINE = RACINE = Path(__file__).resolve().parent.parent
DBT_PROJECT = RACINE / "radar"
DBT_MANIFEST = DBT_PROJECT / "target" / "manifest.json"
partitions_jour = DailyPartitionsDefinition(start_date='2026-09-01', timezone='Europe/Paris')

#———————————————————————————————————————————————————————————————————————————————————————————————#

class RadarTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

@asset(partitions_def=partitions_jour)
def annonces_bronze(context: AssetExecutionContext) -> None:
    date = context.partition_key

    folder = RACINE / "data" / "bronze" / f'bodacc_annonces_{date}'
    path = folder / f'bodacc_annonces_{date}.json'

    folder.mkdir(parents=True, exist_ok=True)

    ingest.telecharger_annonce(date, path)

    size = path.stat().st_size
    if size <= 2:
        context.log.warning("No data available for the specified date. (day with no parution?)")
    else:
        context.log.info(f"File downloaded successfully: {path}, size: {size} bytes")

@dbt_assets(manifest=DBT_MANIFEST, dagster_dbt_translator=RadarTranslator())
def radar_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


