import os
from dagster import Definitions, define_asset_job, build_schedule_from_partitioned_job
from dagster_dbt import DbtCliResource

from orchestration.assets import annonces_bronze, radar_dbt_assets, partitions_jour, RACINE, DBT_PROJECT

os.environ["BRONZE_GLOB"] = str(RACINE / "data" / "bronze" / "*" / "*.json")

pipeline_job = define_asset_job(name='pipeline_radar', selection='*', partitions_def=partitions_jour)
schedule_quotidien = build_schedule_from_partitioned_job(pipeline_job, hour_of_day=6)
defs = Definitions(
    assets=[annonces_bronze, radar_dbt_assets], 
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT)},
    jobs=[pipeline_job],
    schedules=[schedule_quotidien]
)