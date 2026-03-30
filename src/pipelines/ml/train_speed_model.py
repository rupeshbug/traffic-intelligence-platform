import json
import sys
from pathlib import Path

import joblib
import mlflow
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeRegressor

from src.utils.exception import TrafficPipelineException
from src.utils.logger import logger


BASE_DIR = Path(__file__).resolve().parents[3]
FACT_TRAFFIC_PATH = BASE_DIR / "warehouse" / "fact_traffic"
ML_ARTIFACT_DIR = BASE_DIR / "artifacts" / "ml" / "speed_estimation"
MLRUNS_DIR = BASE_DIR / "mlruns"

TARGET_COLUMN = "speed_int"
NUMERIC_FEATURES = [
    "hour",
    "peak_flag",
    "traffic_volume_int",
    "congestion_level_int",
    "incident_flag_int",
]
CATEGORICAL_FEATURES = [
    "road_id",
    "city_zone",
    "weather",
    "traffic_band",
]
FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def load_fact_traffic_dataset():
    logger.info("Loading fact_traffic parquet files for ML training")

    parquet_files = sorted(FACT_TRAFFIC_PATH.glob("part-*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found under {FACT_TRAFFIC_PATH}"
        )

    dataframe = pd.read_parquet(parquet_files)
    logger.info("Loaded %s records from fact_traffic", len(dataframe))
    return dataframe


def prepare_training_data(dataframe):
    logger.info("Preparing model features and target column")

    model_df = dataframe[FEATURE_COLUMNS + [TARGET_COLUMN]].dropna().copy()

    X = model_df[FEATURE_COLUMNS]
    y = model_df[TARGET_COLUMN]

    return train_test_split(X, y, test_size=0.2, random_state=42)


def build_models():
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                CATEGORICAL_FEATURES,
            ),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ]
    )

    return {
        "Linear Regression": Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", LinearRegression()),
            ]
        ),
        "Decision Tree": Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", DecisionTreeRegressor(max_depth=8, random_state=42)),
            ]
        ),
        "Random Forest": Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                (
                    "model",
                    RandomForestRegressor(
                        n_estimators=200,
                        max_depth=12,
                        random_state=42,
                        n_jobs=1,
                    ),
                ),
            ]
        ),
        "Gradient Boosting": Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                (
                    "model",
                    GradientBoostingRegressor(
                        n_estimators=200,
                        learning_rate=0.05,
                        random_state=42,
                    ),
                ),
            ]
        ),
    }


def evaluate_models(X_train, X_test, y_train, y_test):
    logger.info("Training regression models for speed estimation")

    mlflow.set_tracking_uri(MLRUNS_DIR.as_uri())
    mlflow.set_experiment("traffic_speed_estimation")

    model_registry = build_models()
    leaderboard = []
    best_model_name = None
    best_model = None
    best_r2 = float("-inf")

    for model_name, pipeline in model_registry.items():
        logger.info("Training model: %s", model_name)
        pipeline.fit(X_train, y_train)

        train_predictions = pipeline.predict(X_train)
        test_predictions = pipeline.predict(X_test)

        train_r2 = r2_score(y_train, train_predictions)
        test_r2 = r2_score(y_test, test_predictions)
        test_rmse = root_mean_squared_error(y_test, test_predictions)
        test_mae = mean_absolute_error(y_test, test_predictions)

        leaderboard.append(
            {
                "model_name": model_name,
                "train_r2": train_r2,
                "test_r2": test_r2,
                "test_rmse": test_rmse,
                "test_mae": test_mae,
            }
        )

        with mlflow.start_run(run_name=model_name):
            mlflow.log_param("target_column", TARGET_COLUMN)
            mlflow.log_param("feature_columns", ",".join(FEATURE_COLUMNS))
            mlflow.log_metrics(
                {
                    "train_r2": train_r2,
                    "test_r2": test_r2,
                    "test_rmse": test_rmse,
                    "test_mae": test_mae,
                }
            )
            mlflow.sklearn.log_model(pipeline, artifact_path="model")

        logger.info(
            "%s -> test_r2=%.4f, test_rmse=%.4f, test_mae=%.4f",
            model_name,
            test_r2,
            test_rmse,
            test_mae,
        )

        if test_r2 > best_r2:
            best_r2 = test_r2
            best_model_name = model_name
            best_model = pipeline

    leaderboard_df = pd.DataFrame(leaderboard).sort_values(
        by="test_r2", ascending=False
    )

    return best_model_name, best_model, leaderboard_df


def save_training_artifacts(best_model_name, best_model, leaderboard_df):
    logger.info("Saving best speed model and training metadata")

    ML_ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    model_path = ML_ARTIFACT_DIR / "best_speed_model.joblib"
    leaderboard_path = ML_ARTIFACT_DIR / "leaderboard.csv"
    metadata_path = ML_ARTIFACT_DIR / "training_metadata.json"

    joblib.dump(best_model, model_path)
    leaderboard_df.to_csv(leaderboard_path, index=False)
    metadata_path.write_text(
        json.dumps(
            {
                "target_column": TARGET_COLUMN,
                "feature_columns": FEATURE_COLUMNS,
                "best_model_name": best_model_name,
                "model_path": str(model_path),
            },
            indent=2,
        )
    )


def main():
    try:
        dataframe = load_fact_traffic_dataset()
        X_train, X_test, y_train, y_test = prepare_training_data(dataframe)

        print(
            f"Train rows: {len(X_train)}, Test rows: {len(X_test)}"
        )

        best_model_name, best_model, leaderboard_df = evaluate_models(
            X_train, X_test, y_train, y_test
        )
        save_training_artifacts(best_model_name, best_model, leaderboard_df)
        logger.info("Speed model training finished successfully")

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e


if __name__ == "__main__":
    main()
