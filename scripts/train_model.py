"""
Train Simplified Ridge Regression Model on Cleaned Hadoop Output
CSE3712 — Big Data Analytics
Trains a scikit-learn Ridge regression pipeline on 2022 sales and evaluates on 2023 sales.
Features: county, property_type, tenure, sale_year, sale_month
Calculates MAE in pounds (£) and compares against naive median baseline.
Saves model to models/ridge_pipeline.joblib and metrics to models/model_metrics.json.
"""
import os
import sys
import json
import time
import argparse
import numpy as np
import pandas as pd
import joblib

from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, r2_score

MODELS_DIR = "models"
os.makedirs(MODELS_DIR, exist_ok=True)

COL_NAMES = [
    "transaction_id", "price", "sale_date", "sale_year",
    "sale_month", "county", "property_type", "tenure"
]

def train(mode="sample"):
    start_time = time.time()
    clean_tsv = os.path.join("data", "clean", mode, "clean_sales.tsv")
    
    if not os.path.exists(clean_tsv):
        if mode == "full" and os.path.exists(os.path.join("data", "clean", "sample", "clean_sales.tsv")):
            print(f"[WARN] Full dataset {clean_tsv} not found yet. Falling back to sample mode.")
            mode = "sample"
            clean_tsv = os.path.join("data", "clean", "sample", "clean_sales.tsv")
        else:
            print(f"Error: {clean_tsv} not found. Run scripts/run_local_streaming.py --mode {mode} first.")
            sys.exit(1)

    print("=" * 70)
    print(f"Training Scikit-Learn Ridge Regression Model (Mode: {mode.upper()})")
    print(f"Input Cleaned Dataset: {clean_tsv}")
    print("=" * 70)

    # 1. Load Clean Dataset
    print("[1/5] Loading cleaned property sales data...")
    with open(clean_tsv, "r", encoding="utf-8") as f:
        first_line = f.readline()
    n_cols = len(first_line.strip().split("\t"))
    col_names = [
        "transaction_id", "price", "sale_date", "sale_year",
        "sale_month", "county", "property_type", "tenure"
    ]
    if n_cols >= 10:
        col_names.extend(["postcode", "town"])

    df = pd.read_csv(
        clean_tsv,
        sep="\t",
        names=col_names,
        header=None,
        usecols=["price", "sale_year", "sale_month", "county", "property_type", "tenure"],
        dtype={
            "price": "int64",
            "sale_year": "int32",
            "sale_month": "int32",
            "county": "string",
            "property_type": "string",
            "tenure": "string"
        }
    )
    print(f"      -> Loaded {len(df):,} cleaned sales transactions.")

    # 2. Strict Temporal Split (Train on 2022, Test on 2023)
    print("\n[2/5] Performing temporal train/test split...")
    train_mask = (df["sale_year"] == 2022)
    test_mask = (df["sale_year"] == 2023)

    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()

    feature_cols = ["county", "property_type", "tenure", "sale_year", "sale_month"]
    categorical_cols = ["county", "property_type", "tenure"]
    numerical_cols = ["sale_year", "sale_month"]

    X_train = train_df[feature_cols]
    y_train = train_df["price"].values
    X_test = test_df[feature_cols]
    y_test = test_df["price"].values

    print(f"      -> Train Set (2022 Sales): {len(X_train):,} rows ({len(X_train)/len(df)*100:.1f}%)")
    print(f"      -> Test Set  (2023 Sales): {len(X_test):,} rows ({len(X_test)/len(df)*100:.1f}%)")

    # 3. Build Scikit-Learn Pipeline
    print("\n[3/5] Constructing preprocessing & Ridge regression pipeline...")
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
            ("num", StandardScaler(), numerical_cols)
        ]
    )

    ridge_pipeline = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("regressor", Ridge(alpha=100.0, random_state=42))
    ])

    # Target log-transform to ensure positive, realistic price predictions
    model = TransformedTargetRegressor(
        regressor=ridge_pipeline,
        func=np.log1p,
        inverse_func=np.expm1
    )

    # 4. Train Model
    t_train = time.time()
    print("      -> Fitting model on 2022 training data...")
    model.fit(X_train, y_train)
    print(f"      -> Model training completed in {time.time() - t_train:.2f}s.")

    # 5. Evaluate on 2023 Test Data
    print("\n[4/5] Evaluating on out-of-time 2023 test dataset...")
    y_pred = model.predict(X_test)
    test_mae = mean_absolute_error(y_test, y_pred)
    test_r2_log = r2_score(np.log1p(y_test), np.log1p(y_pred))

    # Naive Baseline: Always predict the median price of the 2022 training data
    train_median_price = float(np.median(y_train))
    baseline_pred = np.full_like(y_test, fill_value=train_median_price)
    baseline_mae = mean_absolute_error(y_test, baseline_pred)

    mae_improvement = baseline_mae - test_mae
    pct_improvement = (mae_improvement / baseline_mae) * 100.0

    viva_sentence = (
        f"The Ridge regression model achieves an out-of-time test MAE of £{test_mae:,.0f}, "
        f"which improves prediction accuracy by £{mae_improvement:,.0f} ({pct_improvement:.1f}% error reduction) "
        f"over the baseline of always predicting the training median price of £{train_median_price:,.0f}."
    )

    print("-" * 70)
    print(f"  Test MAE (Ridge Model):       £{test_mae:,.0f}")
    print(f"  Baseline MAE (Median Price):  £{baseline_mae:,.0f} (Median: £{train_median_price:,.0f})")
    print(f"  MAE Improvement:              £{mae_improvement:,.0f} ({pct_improvement:.1f}% reduction)")
    print(f"  Test R² (Log Scale):          {test_r2_log:.4f}")
    print("-" * 70)
    print(f"  Viva Sentence: \"{viva_sentence}\"")

    # 6. Save Model and Metrics
    print("\n[5/5] Serializing model and metrics artifacts...")
    mode_model_path = os.path.join(MODELS_DIR, f"ridge_pipeline_{mode}.joblib")
    mode_metrics_path = os.path.join(MODELS_DIR, f"model_metrics_{mode}.json")
    default_model_path = os.path.join(MODELS_DIR, "ridge_pipeline.joblib")
    default_metrics_path = os.path.join(MODELS_DIR, "model_metrics.json")

    joblib.dump(model, mode_model_path)
    joblib.dump(model, default_model_path)

    metrics_data = {
        "dataset_mode": mode,
        "train_records_2022": int(len(X_train)),
        "test_records_2023": int(len(X_test)),
        "training_median_price_gbp": round(train_median_price, 2),
        "test_mae_gbp": round(float(test_mae), 2),
        "baseline_mae_gbp": round(float(baseline_mae), 2),
        "mae_improvement_gbp": round(float(mae_improvement), 2),
        "mae_improvement_pct": round(float(pct_improvement), 2),
        "test_r2_log_scale": round(float(test_r2_log), 4),
        "viva_explanation": viva_sentence,
        "input_features": feature_cols,
        "model_file": mode_model_path
    }

    with open(mode_metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=2)

    with open(default_metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=2)

    print(f"      -> Saved mode model to:      {mode_model_path}")
    print(f"      -> Saved mode metrics to:    {mode_metrics_path}")
    print(f"      -> Saved default model to:   {default_model_path}")
    print(f"      -> Total Pipeline Latency:   {time.time() - start_time:.2f}s")
    print("=" * 70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Simplified Ridge Regression Model")
    parser.add_argument("--mode", choices=["sample", "full"], default="sample", help="Mode: sample or full")
    args = parser.parse_args()
    train(mode=args.mode)
