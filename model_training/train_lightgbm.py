# stages/stage4/train_lightgbm.py

import mlflow
import mlflow.lightgbm
import lightgbm as lgb
import shap
import numpy as np
import pandas as pd
from sklearn.metrics import (
    f1_score, precision_score, recall_score,
    roc_auc_score, classification_report
)
import os
import sys
import matplotlib
matplotlib.use("Agg")   
import matplotlib.pyplot as plt


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME     = "malaria_lightgbm"


def train_lightgbm(X_train, X_test, y_train, y_test, scale_pos_weight: float):
    """
    Train a LightGBM classifier and log everything to MLflow.
    Returns the trained model.
    """

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    params = {
        "objective":        "binary",       # binary classification task
        "metric":           "binary_logloss",
        "n_estimators":     500,
        "learning_rate":    0.05,
        "num_leaves":       31,             # controls tree complexity
        "min_child_samples": 20,            # prevents overfitting on small groups
        "scale_pos_weight": scale_pos_weight,  # fixes class imbalance
        "random_state":     42,
        "n_jobs":           -1,             # use all CPU cores
    }

    print(f"\n── Training LightGBM ──")
    print(f"  Params: {params}")

    with mlflow.start_run(run_name="baseline_with_covid_feature"):

        # ── Train ─────────────────────────────────────────────────────────────
        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],    # watch test loss during training
            callbacks=[lgb.early_stopping(50, verbose=False),  
                       lgb.log_evaluation(100)]
            # early_stopping: stop if test loss doesn't improve for 50 rounds
            # This prevents overfitting automatically
        )

        # ── Evaluate ──────────────────────────────────────────────────────────
        y_pred      = model.predict(X_test)
        y_pred_prob = model.predict_proba(X_test)[:, 1]

        f1        = f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall    = recall_score(y_test, y_pred)
        roc_auc   = roc_auc_score(y_test, y_pred_prob)

        print(f"\n── Evaluation Results ──")
        print(f"  F1 Score:  {f1:.4f}")
        print(f"  Precision: {precision:.4f}   "
              f"(of all predicted 'improving', how many really were?)")
        print(f"  Recall:    {recall:.4f}   "
              f"(of all truly improving, how many did we catch?)")
        print(f"  ROC-AUC:   {roc_auc:.4f}   "
              f"(1.0 = perfect, 0.5 = random guess)")
        print(f"\n{classification_report(y_test, y_pred, target_names=['not improving', 'improving'])}")

        # ── Log to MLflow ─────────────────────────────────────────────────────
       
        mlflow.log_params(params)

       
        mlflow.log_metrics({
            "f1":        f1,
            "precision": precision,
            "recall":    recall,
            "roc_auc":   roc_auc,
        })

       
        print("\n── Computing SHAP values ──")
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

      
        if isinstance(shap_values, list):
            shap_vals = shap_values[1]
        else:
            shap_vals = shap_values

        # Save SHAP summary plot as an artifact in MLflow
        plt.figure(figsize=(10, 6))
        shap.summary_plot(shap_vals, X_test, show=False)
        plt.tight_layout()
        shap_plot_path = "/tmp/shap_summary.png"
        plt.savefig(shap_plot_path, dpi=150, bbox_inches="tight")
        plt.close()
        mlflow.log_artifact(shap_plot_path, artifact_path="plots")
        print(f"  SHAP plot saved")

        # ── Save the model ────────────────────────────────────────────────────
        mlflow.lightgbm.log_model(
            model,
            artifact_path="model",
            registered_model_name="malaria_lgbm_classifier",
            # ^ this registers it in MLflow Model Registry so Stage 6
            #   FastAPI can load it by name, not by run ID
        )

        run_id = mlflow.active_run().info.run_id
        print(f"\n✅ Run logged to MLflow — run_id: {run_id}")
        print(f"   View at: {MLFLOW_TRACKING_URI}/#/experiments/")

    return model


if __name__ == "__main__":
    # Allow running this file directly for testing
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from model_training.data_prep import load_gold_from_s3, prepare_lgbm_data

    df = load_gold_from_s3()
    X_train, X_test, y_train, y_test, spw = prepare_lgbm_data(df)
    train_lightgbm(X_train, X_test, y_train, y_test, spw)