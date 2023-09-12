from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""


    df = pd.read_csv(dataset_url)
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Dataframe out locally as a parquet file"""
    path = Path(f"/home/hguber/week2/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return
                                                 
@flow()
def etl_web_to_gcs() -> None:
    """The Main ETL function"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    #df_clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()