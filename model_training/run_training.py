

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_prep import load_gold_from_s3, prepare_lgbm_data, prepare_prophet_data
from train_lightgbm import train_lightgbm
from train_prophet import train_all_regions


def run_pipeline():
    print("=" * 60)
    print("  STAGE 4 — ML TRAINING PIPELINE")
    print("=" * 60)

    # ── Step 1: Load data ──────────────────────────────────────────
    print("\n[1/3] Loading gold data from S3...")
    df = load_gold_from_s3()

    # ── Step 2: Train LightGBM ─────────────────────────────────────
    print("\n[2/3] Training LightGBM classifier...")
    X_train, X_test, y_train, y_test, spw = prepare_lgbm_data(df)
    train_lightgbm(X_train, X_test, y_train, y_test, spw)

    # ── Step 3: Train Prophet ──────────────────────────────────────
    print("\n[3/3] Training Prophet forecasters...")
    # Reload df — prepare_lgbm_data modifies it in place
    df2 = load_gold_from_s3()
    prophet_dfs = prepare_prophet_data(df2)
    train_all_regions(prophet_dfs)

    print("\n" + "=" * 60)
    print("  STAGE 4 COMPLETE ✅")
    print("=" * 60)


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print(f"\n❌ Stage 4 failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)   