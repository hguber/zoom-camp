from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = path = f"/home/hguber/week2/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block= GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../week2/")
    return Path(f"{gcs_path}")

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcs")
    df.to_gbq(
        destination_table="dezoomcamp.rides2019",
        project_id="dtc-de-397009",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int] = [1,2], year: int = 2021, color: str = "yellow"):
    """Main ETL flow to load data into Big Query Data Warehouse"""
    size = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(path)
        write_bq(df)
        print(f"Total Rows: {len(df)} for {color} taxi {year} {month}")
        size += len(df)
    print(f'Total Rows: {size}')

if __name__ == "__main__":
    color = 'yellow'
    months = [2,3]
    year = 2019
    etl_gcs_to_bq(months, year, color)