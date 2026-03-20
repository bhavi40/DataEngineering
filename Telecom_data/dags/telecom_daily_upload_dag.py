from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import os
import glob
import logging

default_args = {
    "owner":            "telecom-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key = Variable.get("AWS_SECRET_KEY"),
        region_name           = Variable.get("AWS_REGION", default_var="us-east-1")
    )


def upload_cdr_to_s3(**context):
    """
    Upload all CDR CSV files for today to S3.
    Pattern: cdr_YYYYMMDD_001.csv ... cdr_YYYYMMDD_400.csv
    Destination: s3://bucket/raw/cdr/
    """
    execution_date = context["execution_date"]
    date_str       = execution_date.strftime("%Y%m%d")
    date_full      = execution_date.strftime("%Y-%m-%d")
    local_dir      = Variable.get("LOCAL_DATA_DIR", default_var="/opt/airflow/data")
    bucket         = Variable.get("S3_BUCKET")
    s3             = get_s3_client()

    pattern = os.path.join(local_dir, "raw", "cdr", f"cdr_{date_str}_*.csv")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No CDR files found for {date_full}\n"
            f"Pattern: {pattern}\n"
            f"Run generate_test_data.py first."
        )

    logging.info(f"Uploading {len(files)} CDR files for {date_full}...")

    for local_path in files:
        filename = os.path.basename(local_path)
        s3_key   = f"raw/cdr/{filename}"
        size_mb  = os.path.getsize(local_path) / 1024 / 1024
        s3.upload_file(local_path, bucket, s3_key)
        logging.info(f"  ✓ {filename} ({size_mb:.2f} MB)")

    logging.info(f"CDR upload complete! {len(files)} files.")
    context["ti"].xcom_push(key="cdr_files_count", value=len(files))
    context["ti"].xcom_push(key="date_str",         value=date_str)
    return len(files)


def upload_events_to_s3(**context):
    """
    Upload all Events JSON files for today to S3.
    Pattern: events_YYYYMMDD_001.json ... events_YYYYMMDD_400.json
    Destination: s3://bucket/raw/network_events/
    """
    execution_date = context["execution_date"]
    date_str       = execution_date.strftime("%Y%m%d")
    date_full      = execution_date.strftime("%Y-%m-%d")
    local_dir      = Variable.get("LOCAL_DATA_DIR", default_var="/opt/airflow/data")
    bucket         = Variable.get("S3_BUCKET")
    s3             = get_s3_client()

    pattern = os.path.join(local_dir, "raw", "network_events", f"events_{date_str}_*.json")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No Events files found for {date_full}\n"
            f"Pattern: {pattern}\n"
            f"Run generate_test_data.py first."
        )

    logging.info(f"Uploading {len(files)} Events files for {date_full}...")

    for local_path in files:
        filename = os.path.basename(local_path)
        s3_key   = f"raw/network_events/{filename}"
        size_mb  = os.path.getsize(local_path) / 1024 / 1024
        s3.upload_file(local_path, bucket, s3_key)
        logging.info(f"  ✓ {filename} ({size_mb:.2f} MB)")

    logging.info(f"Events upload complete! {len(files)} files.")
    context["ti"].xcom_push(key="events_files_count", value=len(files))
    return len(files)


def verify_s3_upload(**context):
    """
    Verify file counts in S3 match what was uploaded.
    """
    execution_date  = context["execution_date"]
    date_str        = execution_date.strftime("%Y%m%d")
    date_full       = execution_date.strftime("%Y-%m-%d")
    bucket          = Variable.get("S3_BUCKET")
    s3              = get_s3_client()
    ti              = context["ti"]

    cdr_count    = ti.xcom_pull(task_ids="upload_cdr_to_s3",    key="cdr_files_count")
    events_count = ti.xcom_pull(task_ids="upload_events_to_s3", key="events_files_count")

    cdr_s3    = s3.list_objects_v2(Bucket=bucket, Prefix=f"raw/cdr/cdr_{date_str}_").get("KeyCount", 0)
    events_s3 = s3.list_objects_v2(Bucket=bucket, Prefix=f"raw/network_events/events_{date_str}_").get("KeyCount", 0)

    logging.info(f"CDR:    uploaded={cdr_count} in S3={cdr_s3}")
    logging.info(f"Events: uploaded={events_count} in S3={events_s3}")

    errors = []
    if cdr_s3 != cdr_count:
        errors.append(f"CDR mismatch: uploaded {cdr_count} but S3 has {cdr_s3}")
    if events_s3 != events_count:
        errors.append(f"Events mismatch: uploaded {events_count} but S3 has {events_s3}")

    if errors:
        raise Exception(f"Verification failed for {date_full}:\n" + "\n".join(errors))

    logging.info(f"Verified! CDR={cdr_count} Events={events_count} files in S3.")
    return {"date": date_full, "cdr": cdr_count, "events": events_count}


def move_to_processed(**context):
    """
    Archive all files for today — both locally and in S3.

    Local:  data/raw/cdr/cdr_YYYYMMDD_NNN.csv
                → data/processed/cdr/YYYY/MM/cdr_YYYYMMDD_NNN.csv

            data/raw/network_events/events_YYYYMMDD_NNN.json
                → data/processed/network_events/YYYY/MM/events_YYYYMMDD_NNN.json

    S3:     raw/cdr/cdr_YYYYMMDD_NNN.csv
                → processed/cdr/YYYY/MM/cdr_YYYYMMDD_NNN.csv

            raw/network_events/events_YYYYMMDD_NNN.json
                → processed/network_events/YYYY/MM/events_YYYYMMDD_NNN.json
    """
    execution_date = context["execution_date"]
    date_str       = execution_date.strftime("%Y%m%d")
    year_month     = execution_date.strftime("%Y/%m")
    local_dir      = Variable.get("LOCAL_DATA_DIR", default_var="/opt/airflow/data")
    bucket         = Variable.get("S3_BUCKET")
    s3             = get_s3_client()

    cdr_files    = sorted(glob.glob(os.path.join(local_dir, "raw", "cdr",            f"cdr_{date_str}_*.csv")))
    events_files = sorted(glob.glob(os.path.join(local_dir, "raw", "network_events", f"events_{date_str}_*.json")))
    all_files    = cdr_files + events_files

    logging.info(f"Archiving {len(all_files)} files for {date_str}...")
    archived = 0

    for local_path in all_files:
        filename = os.path.basename(local_path)

        if filename.startswith("cdr_"):
            s3_src    = f"raw/cdr/{filename}"
            s3_dst    = f"processed/cdr/{year_month}/{filename}"
            local_dst = os.path.join(local_dir, "processed", "cdr", year_month, filename)
        else:
            s3_src    = f"raw/network_events/{filename}"
            s3_dst    = f"processed/network_events/{year_month}/{filename}"
            local_dst = os.path.join(local_dir, "processed", "network_events", year_month, filename)

        # S3: copy to processed/ then delete from raw/
        s3.copy_object(
            Bucket     = bucket,
            CopySource = {"Bucket": bucket, "Key": s3_src},
            Key        = s3_dst
        )
        s3.delete_object(Bucket=bucket, Key=s3_src)

        # Local: move to processed/ subfolder
        os.makedirs(os.path.dirname(local_dst), exist_ok=True)
        os.rename(local_path, local_dst)

        archived += 1

    logging.info(f"Archive complete! {archived} files moved to processed/")
    return {"date": date_str, "archived": archived}


# ── DAG DEFINITION ────────────────────────────────────────────────────────────
with DAG(
    dag_id            = "telecom_daily_upload",
    description       = "Upload 400 CDR + 400 Events files to S3, verify, archive",
    default_args      = default_args,
    start_date        = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    schedule_interval = "0 6 * * *",
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["telecom", "s3", "ingestion"],
) as dag:

    upload_cdr = PythonOperator(
        task_id         = "upload_cdr_to_s3",
        python_callable = upload_cdr_to_s3,
    )

    upload_events = PythonOperator(
        task_id         = "upload_events_to_s3",
        python_callable = upload_events_to_s3,
    )

    verify = PythonOperator(
        task_id         = "verify_s3_upload",
        python_callable = verify_s3_upload,
    )

    archive = PythonOperator(
        task_id         = "move_to_processed",
        python_callable = move_to_processed,
    )

    # upload_cdr    ──→ verify ──→ archive
    # upload_events ──↗
    [upload_cdr, upload_events] >> verify >> archive