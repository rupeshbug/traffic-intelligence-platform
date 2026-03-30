import sys
import time
from pathlib import Path

import joblib
import pandas as pd
import pyarrow as pa

from src.pipelines.ml.train_speed_model import FEATURE_COLUMNS, FACT_TRAFFIC_PATH
from src.utils.exception import TrafficPipelineException
from src.utils.logger import logger


BASE_DIR = Path(__file__).resolve().parents[3]
MODEL_PATH = BASE_DIR / "artifacts" / "ml" / "speed_estimation" / "best_speed_model.joblib"
PREDICTION_OUTPUT_PATH = BASE_DIR / "artifacts" / "ml" / "speed_estimation" / "live_predictions.csv"
POLL_INTERVAL_SECONDS = 5
READ_RETRY_SECONDS = 2


def load_model():
    logger.info("Loading saved speed estimation model")

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Best model not found at {MODEL_PATH}. Train the model first."
        )

    return joblib.load(MODEL_PATH)


def load_fact_traffic():
    parquet_files = sorted(
        file_path
        for file_path in FACT_TRAFFIC_PATH.glob("part-*.parquet")
        if file_path.stat().st_size > 0
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"No readable parquet files found under {FACT_TRAFFIC_PATH}"
        )

    try:
        return pd.read_parquet(parquet_files)
    except (pa.ArrowInvalid, OSError) as error:
        logger.warning(
            "Parquet files are still being committed. Retrying in %s seconds. Details: %s",
            READ_RETRY_SECONDS,
            error,
        )
        time.sleep(READ_RETRY_SECONDS)

        parquet_files = sorted(
            file_path
            for file_path in FACT_TRAFFIC_PATH.glob("part-*.parquet")
            if file_path.stat().st_size > 0
        )

        if not parquet_files:
            raise FileNotFoundError(
                f"No readable parquet files found under {FACT_TRAFFIC_PATH}"
            ) from error

        return pd.read_parquet(parquet_files)


def build_record_key(dataframe):
    return (
        dataframe["vehicle_id"].astype(str)
        + "|"
        + dataframe["event_ts"].astype(str)
    )


def append_predictions(prediction_df):
    PREDICTION_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    write_header = not PREDICTION_OUTPUT_PATH.exists()
    prediction_df.to_csv(
        PREDICTION_OUTPUT_PATH,
        mode="a",
        header=write_header,
        index=False,
    )


def score_new_rows(model, batch_df):
    inference_df = batch_df.copy()
    inference_df["predicted_speed"] = model.predict(inference_df[FEATURE_COLUMNS]).round(2)
    inference_df["prediction_error"] = (
        inference_df["speed_int"] - inference_df["predicted_speed"]
    ).round(2)
    inference_df["prediction_ts"] = pd.Timestamp.utcnow()

    result_df = inference_df[
        [
            "vehicle_id",
            "road_id",
            "city_zone",
            "weather",
            "event_ts",
            "traffic_volume_int",
            "congestion_level_int",
            "speed_int",
            "predicted_speed",
            "prediction_error",
            "prediction_ts",
        ]
    ].copy()

    print("\nNew speed predictions:")
    print(result_df.head(10).to_string(index=False))

    append_predictions(result_df)

    logger.info(
        "Scored %s new records and appended predictions to %s",
        len(result_df),
        PREDICTION_OUTPUT_PATH,
    )


def main():
    try:
        model = load_model()
        processed_keys = set()

        initial_df = load_fact_traffic()
        processed_keys.update(build_record_key(initial_df))
        logger.info(
            "Initialized streaming scorer with %s existing fact_traffic rows skipped",
            len(processed_keys),
        )
        print(
            f"Streaming scorer started. Skipping {len(processed_keys)} existing rows and waiting for new fact_traffic records..."
        )

        while True:
            fact_df = load_fact_traffic()
            fact_df["record_key"] = build_record_key(fact_df)

            new_rows = fact_df.loc[~fact_df["record_key"].isin(processed_keys)].copy()

            if not new_rows.empty:
                logger.info("Found %s new rows for live scoring", len(new_rows))
                processed_keys.update(new_rows["record_key"])
                score_new_rows(model, new_rows)

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Live speed prediction stopped by user")
        print("\nLive speed prediction stopped.")

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e


if __name__ == "__main__":
    main()
