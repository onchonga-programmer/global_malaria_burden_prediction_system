

import logging
import traceback
import mlflow
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def run_step(name: str, fn):
  
    log.info("")
    log.info("=" * 60)
    log.info(f"Starting: {name}")
    log.info("=" * 60)
    try:
        fn()
        log.info(f"✓  {name} completed successfully")
        return True
    except Exception:
        log.error(f"✗  {name} FAILED:")
        log.error(traceback.format_exc())
        return False


def main():
    log.info("")
    log.info("╔══════════════════════════════════════════════╗")
    log.info("║   Stage 5 — Model Validation                ║")
    log.info("╚══════════════════════════════════════════════╝")

   
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("malaria_stage5_validation")

    results = {}

    # ── Step 1: SHAP deep analysis ──
    def run_shap():
        from shap_analysis import run_shap_analysis
        run_shap_analysis()

    results["SHAP analysis"] = run_step("SHAP analysis", run_shap)

    # ── Step 2: LightGBM cross validation + bias audit ──
    def run_lgbm():
        from validate_lightgbm import run_validation
        run_validation()

    results["LightGBM validation"] = run_step("LightGBM validation", run_lgbm)

    # ── Step 3: Prophet accuracy + calibration ──
    def run_prophet():
        from validate_prophet import run_prophet_validation
        run_prophet_validation()

    results["Prophet validation"] = run_step("Prophet validation", run_prophet)

    # ── Step 4: Anomaly detection ──
    def run_anomaly():
        from anomaly_detection import run_anomaly_detection
        run_anomaly_detection()

    results["Anomaly detection"] = run_step("Anomaly detection", run_anomaly)

    # ── Final summary ──
    log.info("")
    log.info("=" * 60)
    log.info("Stage 5 — Summary")
    log.info("=" * 60)
    all_passed = True
    for step, passed in results.items():
        status = "✓  OK" if passed else "✗  FAILED"
        log.info(f"  {status}   {step}")
        if not passed:
            all_passed = False

    log.info("")
    if all_passed:
        log.info("All validation steps completed successfully.")
        log.info(f"View results in MLflow at: {MLFLOW_URI}")
        log.info("Experiment: malaria_stage5_validation")
    else:
        failed = [k for k, v in results.items() if not v]
        log.warning(f"Some steps failed: {failed}")
        log.warning("Check logs above for details.")
        log.info(f"Partial results are available in MLflow at: {MLFLOW_URI}")

    log.info("")


if __name__ == "__main__":
    main()