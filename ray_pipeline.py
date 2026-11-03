import hashlib
import os
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import ray
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, NestedField, StringType
from sklearn.linear_model import LinearRegression


@ray.remote(num_cpus=1)
def ingest_data(file_path: str) -> pd.DataFrame:
    """Reads an existing CSV file into a pandas DataFrame."""
    print(f"--- [Task] Ingesting data from {file_path} ---")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")
    return pd.read_csv(file_path)


# Scheduled onto specialized node group using custom Ray resource
@ray.remote(resources={"resolver_node": 1})
class EntityResolver:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY", "NOT_SET")
        if self.api_key == "NOT_SET":
            print("CRITICAL WARNING: LLM_API_KEY not found in environment. Resolution will fail.")
        else:
            print("LLM_API_KEY loaded successfully.")

    def resolve_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"--- [Actor] Processing batch of {len(df)} records via simulated LLM ---")
        time.sleep(2)

        def simple_clean(name):
            name = str(name).lower()
            for suffix in ["corp", "corporation", "inc", "llc", "co"]:
                name = name.replace(suffix, "")
            return name.strip()

        df["canonical_name"] = df["corporate_name"].apply(simple_clean)
        df["canonical_id"] = df["canonical_name"].apply(
            lambda x: f"UID-{int(hashlib.md5(x.encode()).hexdigest(), 16) % 10000}"
        )
        df["is_resolved"] = True
        return df


def run_pipeline():
    # Define paths to your existing CSV files
    # These can be customized via environment variables or default to local filenames
    file_s1 = os.getenv("CSV_PATH_S1", "data_s1.csv")
    file_s2 = os.getenv("CSV_PATH_S2", "data_s2.csv")

    if not ray.is_initialized():
        ray.init(address=os.getenv("RAY_ADDRESS", "auto"), ignore_reinit_error=True)
    print("Pipeline Execution Started.")

    # 1. Distributed Ingestion of Existing Files
    future_a = ingest_data.remote(file_s1)
    future_b = ingest_data.remote(file_s2)
    data_sources = ray.get([future_a, future_b])
    combined_df = pd.concat(data_sources, ignore_index=True)
    print(f"Ingested total of {len(combined_df)} records.")

    # 2. Entity Resolution via Actor
    resolver = EntityResolver.remote()
    print("Starting LLM-based Entity Resolution...")
    resolved_df = ray.get(resolver.resolve_batch.remote(combined_df))

    # 3. Model Training (Predict Annual Revenues)
    print("--- Training Revenue Prediction Model ---")
    resolved_df["name_len"] = resolved_df["corporate_name"].apply(lambda x: len(str(x)))
    X = resolved_df[["name_len"]]
    y = resolved_df["revenue"]

    model = LinearRegression()
    model.fit(X, y)
    resolved_df["predicted_revenue"] = model.predict(X)
    print(f"Model trained. Sample predicted revenue: {resolved_df['predicted_revenue'].iloc[0]:.2f}")

    print("--- Harmonized Data Summary ---")
    print(resolved_df[["corporate_name", "canonical_id", "predicted_revenue", "is_resolved"]].head())

    # 4. Iceberg Sink
    print("--- Final Step: Upserting to Apache Iceberg (corporate_registry) ---")
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    if s3_bucket:
        try:
            catalog = load_catalog(
                "glue",
                **{
                    "type": "glue",
                    "warehouse": f"s3://{s3_bucket}/iceberg-warehouse",
                },
            )
            schema = Schema(
                NestedField(1, "canonical_id", StringType(), False),
                NestedField(2, "corporate_name", StringType(), False),
                NestedField(3, "predicted_revenue", DoubleType(), False),
                NestedField(4, "is_resolved", BooleanType(), False),
            )
            table_name = "default.corporate_registry"
            try:
                table = catalog.load_table(table_name)
            except Exception:
                table = catalog.create_table(table_name, schema=schema)

            sink_df = resolved_df[["canonical_id", "corporate_name", "predicted_revenue", "is_resolved"]]
            arrow_table = pa.Table.from_pandas(sink_df)
            table.append(arrow_table)
            print("Successfully committed transaction to Iceberg table via AWS Glue.")
        except Exception as e:
            print(f"Iceberg commit failed or skipped: {e}")
    else:
        print("S3_BUCKET_NAME not set. Local execution completed successfully.")


if __name__ == "__main__":
    run_pipeline()