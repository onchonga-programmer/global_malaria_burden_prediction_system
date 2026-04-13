# 🌍 Global Malaria Burden Predicting System

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE)
![MLflow](https://img.shields.io/badge/MLflow-2.11.3-0194E2)
![License](https://img.shields.io/badge/License-MIT-green)

> A production-grade, end-to-end MLOps pipeline for forecasting
> regional malaria burden and classifying improving vs deteriorating
> trends — built to support WHO public health planning.

Malaria kills over 600,000 people annually, with the burden falling
disproportionately on sub-Saharan Africa. Public health organizations
like WHO need forward-looking forecasts — not just historical reports —
to allocate resources, plan interventions, and identify which regions
are deteriorating before the situation becomes a crisis.

This system ingests WHO malaria data, engineers predictive features,
trains and validates two complementary models (Prophet for time-series
forecasting, LightGBM for trend classification), and serves predictions
through a FastAPI backend and Streamlit analyst dashboard — all
orchestrated with Apache Airflow, tracked with MLflow, and containerized
with Docker.

---

## 🏗️ Architecture

The system follows a **medallion data architecture** (Bronze → Silver → Gold)
with a full MLOps loop — from raw data ingestion through model serving,
with automated retraining designed into the pipeline from the start.

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                          │
│              OWID / WHO API  (ourworldindata.org)           │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                STAGE 1 — Bronze Ingestion                   │
│         Raw JSON → S3 (malaria-forecast-bree)               │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              STAGE 2 — Silver / Gold Transform              │
│     Clean → Type → Feature engineer → Parquet on S3        │
│         gold/malaria_features/features.parquet              │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│             STAGE 3 — Airflow Orchestration                 │
│          DockerOperator DAG  — port 8082                    │
│     Schedules full pipeline · triggers retraining loop      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│               STAGE 4 — Model Training                      │
│   LightGBM classifier  (F1=0.63, ROC-AUC=0.93)             │
│   Prophet time-series forecaster                            │
│   MLflow experiment tracking — port 5000                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│               STAGE 5 — Model Validation                    │
│   Walk-forward CV · SHAP interpretability                   │
│   Isolation Forest anomaly detection                        │
│   PSI drift monitoring · MLflow model registry              │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────────────┐
│                  STAGE 6 — Serving Layer                      │
│  ┌─────────────────────────┐   ┌─────────────────────────┐   │
│  │      FastAPI            │──▶│      Streamlit           │   │
│  │  Prediction API         │   │  Analyst Dashboard       │   │
│  │  port 8001              │   │  port 8502               │   │
│  │  Serves LightGBM        │   │  Forecasts · SHAP plots  │   │
│  │  + Prophet models       │   │  Anomaly flags           │   │
│  └─────────────────────────┘   └─────────────────────────┘   │
│          Docker Compose — all services on shared network      │
│          Designed for ECS Fargate + ECR deployment            │
└───────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data source | OWID Chart API | WHO malaria datasets |
| Cloud storage | AWS S3 | Bronze / Silver / Gold data layers |
| Orchestration | Apache Airflow 2.8.1 | Pipeline scheduling and retraining trigger |
| Containerization | Docker + Docker Compose | Reproducible, portable services |
| ML — classification | LightGBM | Trend classification (improving vs deteriorating) |
| ML — forecasting | Prophet + CmdStanPy | Regional death count time-series forecasting |
| Experiment tracking | MLflow 2.11.3 | Model logging, registry, promotion gating |
| Explainability | SHAP | Feature importance for public health interpretability |
| Anomaly detection | Isolation Forest | Surfacing regional outliers independently |
| API serving | FastAPI | Prediction endpoint serving registered models |
| Dashboard | Streamlit | Analyst-facing forecast and anomaly UI |
| Language | Python 3.11 | End to end |

---

## 📋 Pipeline — Stage by Stage

### Stage 1 — Bronze Ingestion
Pulls malaria death, incidence, and case data from the OWID Chart API
and lands raw JSON into S3 as the bronze layer. The ingestion script
handles multiple datasets in a single run and is fully idempotent —
running it twice does not duplicate data.

**Key decision:** Raw data is stored as-is in bronze before any
transformation. This means if a transformation bug is introduced in
Stage 2, the original source data is always recoverable from S3 without
needing to call the API again.

---

### Stage 2 — Silver / Gold Transformation
Cleans and types the raw data in the silver layer, then engineers
predictive features in the gold layer — including lag features
(1, 2, 3-year), rolling averages, year-over-year percentage change,
COVID period flags, and WHO region encodings. Final output is a single
Parquet file at `gold/malaria_features/features.parquet`.

**Key decision:** Regional aggregate rows in the gold data use
`who_region = 'OTHER'` and store the region identity in the `country`
column (e.g. "Africa", "South-East Asia"). This required a mapping
dictionary rather than a direct region filter — a quirk discovered
through exploratory analysis and documented to prevent future confusion.

---

### Stage 3 — Airflow Orchestration
All pipeline stages run as Docker containers orchestrated by an Apache
Airflow DAG using DockerOperator. Each stage is isolated in its own
container with its own dependencies. The DAG can be triggered manually
or on a schedule to support annual retraining when WHO releases new data.

**Key decision:** Using DockerOperator rather than PythonOperator means
each stage runs in a fully isolated environment. A dependency conflict
in the training stage cannot affect the ingestion stage. This mirrors
how production ML pipelines are built on platforms like Vertex AI and
SageMaker.

---

### Stage 4 — Model Training
Trains two complementary models logged to MLflow:

- **LightGBM classifier** — predicts whether a region's malaria trend
  is improving or deteriorating. Handles class imbalance
  (~79% negative / ~21% positive) via `scale_pos_weight ≈ 3.7`.
- **Prophet time-series model** — forecasts absolute malaria death
  counts per region with uncertainty intervals for the next 5 years.

**Key decision — data leakage catch:** An initial training run returned
a perfect F1=1.0, which flagged an investigation. The cause was three
columns (`deaths`, `death_rate_per_100k`, `deaths_yoy_pct`) that
directly encoded the target label. Removing them produced honest
results (F1=0.63, ROC-AUC=0.93). A perfect score on a real-world
public health dataset is a red flag, not a success.

---

### Stage 5 — Model Validation
Four validation checks run before any model is promoted to production:

- **Walk-forward cross-validation** — time-aware CV respecting the
  temporal structure of the data. F1 degraded across windows, traced
  to COVID-19 disrupting malaria programs in the 2020s — interpreted
  as a real-world confounder, not model failure.
- **SHAP interpretability** — `death_rate_per_100k_lag1` identified
  as the dominant feature, consistent with domain knowledge that last
  year's death rate is the strongest predictor of this year's trend.
- **Isolation Forest anomaly detection** — independently surfaced
  India's documented malaria elimination progress and Nigeria as a
  regional outlier, validating that the anomaly detector aligns with
  known public health events.
- **PSI drift monitoring** — Population Stability Index tracks
  distribution shift between training data and new incoming data.
  Triggers a retraining alert when drift exceeds threshold.

**Key decision:** All four validation results are logged to MLflow
experiment `malaria_stage5_validation`. A model must pass all four
checks before being promoted in the MLflow registry — the serving
layer never loads an unvalidated model.

---

### Stage 6 — Serving Layer
A FastAPI prediction API serves the registered LightGBM and Prophet
models from the MLflow model registry. A Streamlit dashboard provides
an analyst-facing interface showing 5-year regional forecasts with
confidence intervals, SHAP feature importance plots, and anomaly flags
— all selectable by WHO region.

Both services run as Docker containers on a shared Docker Compose
network. The architecture is designed for ECS Fargate deployment —
the Docker Compose service definitions map directly to ECS task
definitions.

**Key decision:** FastAPI loads models from the MLflow registry at
startup rather than from hardcoded file paths. This means when a
retrained model is promoted in the registry, the serving layer picks
it up on next restart with no code changes — completing the MLOps
retraining loop.

---

## 🚀 How to Run Locally

### Prerequisites
- Docker and Docker Compose installed
- AWS account with S3 access
- Python 3.11+

---

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/Global_Malaria_burden_predicting_system.git
cd Global_Malaria_burden_predicting_system
```

---

### 2. Configure environment variables
```bash
cp .env.example .env
```
Open `.env` and fill in your values:
```
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
MLFLOW_TRACKING_URI=http://mlflow:5000
```
> ⚠️ Never commit your `.env` file. It is gitignored by default.

---

### 3. Start MLflow and Airflow
```bash
docker compose up airflow mlflow -d
```
| Service | URL |
|---|---|
| Airflow UI | http://localhost:8082 |
| MLflow UI | http://localhost:5000 |

Wait ~30 seconds for Airflow to initialize before triggering any DAGs.

---

### 4. Run the full pipeline
Trigger the DAG manually from the Airflow UI, or run each stage
independently:

```bash
# Stage 1 — Bronze ingestion
docker compose run --rm ingestion

# Stage 2 — Silver / Gold transformation
docker compose run --rm transformation

# Stage 4 — Model training
docker compose run --rm training

# Stage 5 — Model validation
docker compose run --rm validation
```

> Stage 3 (Airflow) orchestrates all of the above automatically
> when the DAG is triggered.

---

### 5. Start the serving layer
```bash
docker compose up fastapi streamlit -d
```
| Service | URL |
|---|---|
| FastAPI docs | http://localhost:8001/docs |
| Streamlit dashboard | http://localhost:8502 |

---

### Port reference
| Service | Port |
|---|---|
| Airflow | 8082 |
| MLflow | 5000 |
| FastAPI | 8001 |
| Streamlit | 8502 |

---

### Stopping all services
```bash
docker compose down
```

---

## 🧠 Key Design Decisions

### 1. Catching data leakage before it mattered
During model training, LightGBM returned a perfect F1=1.0. Rather than
accepting this result, the score was treated as a red flag and
investigated. The cause was three columns — `deaths`,
`death_rate_per_100k`, and `deaths_yoy_pct` — that directly encoded
the target label `improving`. These were removed from the feature set,
producing honest results (F1=0.63, ROC-AUC=0.93).

This matters in a public health context specifically: a model that
appears perfect but leaks the label would produce confidently wrong
predictions on future data — potentially misdirecting WHO resource
allocation.

---

### 2. Treating COVID-19 as a confounder, not a model failure
Walk-forward cross-validation showed F1 degrading across time windows.
Rather than tuning the model to recover the score, the degradation was
investigated and traced to COVID-19 disrupting global malaria programs
in the 2020s — bed net distribution halted, clinic visits dropped,
reporting systems were overwhelmed.

The model was not broken. The world changed. Recognising the difference
between model failure and real-world disruption is critical when
working with health data that spans a global pandemic.

---

### 3. Explainability as a first-class requirement
LightGBM was chosen over a more complex ensemble specifically because
SHAP values can explain every individual prediction to a non-technical
WHO analyst. A black-box model with higher accuracy is less useful in
public health than an interpretable model a decision-maker can trust
and interrogate.

SHAP analysis confirmed `death_rate_per_100k_lag1` as the dominant
feature — consistent with domain knowledge that last year's death rate
is the strongest predictor of this year's trend. This alignment between
model behaviour and domain expertise increases confidence that the model
is learning signal, not noise.

---

### 4. Anomaly detection as an independent validation layer
Isolation Forest was run independently — without access to the
LightGBM predictions — to surface regional outliers. It identified
India's documented malaria elimination progress and Nigeria as a
regional outlier, both consistent with known public health events.

This independent corroboration matters: if the anomaly detector and
the classifier agree on which regions are unusual, that agreement
increases confidence in both. If they disagreed, that would be a
signal worth investigating before trusting either model.

---

### 5. PSI drift monitoring for production reliability
Population Stability Index is calculated between the training data
distribution and any new incoming data. When PSI exceeds the threshold,
the pipeline raises a retraining alert rather than silently serving
predictions from a model that may no longer reflect current conditions.

This is especially important for malaria data because WHO reporting
lags, program disruptions, and elimination campaigns can shift the
data distribution significantly year over year.

---

### 6. MLflow model registry as a promotion gate
No model reaches the serving layer without passing all four validation
checks in Stage 5. The MLflow registry enforces this — FastAPI loads
only models in the `Production` stage of the registry. A model that
passes training but fails walk-forward CV, SHAP sanity checks, or
drift monitoring stays in `Staging` and never serves predictions.

This gating pattern mirrors production MLOps systems at scale and
ensures the retraining loop cannot silently degrade prediction quality.

---

### 7. Designing for the retraining loop from the start
The pipeline is not a one-time training script. When WHO publishes new
annual malaria data, the Airflow DAG reruns the full pipeline —
ingestion through validation — and promotes a new model if it passes
all checks. The serving layer picks up the new model on next restart
with no code changes required.

This means a WHO analyst does not need to touch the codebase to get
updated forecasts. They wait for the scheduled run, or trigger the DAG
manually. The system handles the rest.

---

## 📸 Screenshots

### Regional forecast dashboard
Select any WHO region to see a 5-year malaria death forecast with
95% confidence intervals, trend classification (improving vs
deteriorating), and model confidence score.

![Forecast dashboard](screenshots/dashboard_forecast.png)

---

### SHAP interpretability panel
Every prediction is accompanied by a SHAP feature importance chart
showing which variables drove the model toward its classification.
`death_rate_per_100k_lag1` consistently dominates — consistent with
domain knowledge that last year's death rate is the strongest
predictor of this year's trend.

![SHAP panel](screenshots/dashboard_shap.png)

---

### MLflow experiment tracking
All training runs, validation metrics, and model versions are logged
to MLflow. The model registry enforces promotion gating — only models
that pass all four Stage 5 validation checks reach the Production stage
and are served by FastAPI.

![MLflow tracking](screenshots/mlflow_experiments.png)

---

## ☁️ Cloud Deployment Architecture

While the system runs fully locally via Docker Compose, it is designed
for production deployment on AWS. The Docker Compose service definitions
map directly to ECS task definitions — no code changes are required to
move from local to cloud.

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS CLOUD                            │
│                                                             │
│   ┌─────────────┐          ┌─────────────────────────┐     │
│   │     ECR     │          │       ECS Fargate        │     │
│   │             │          │                          │     │
│   │  FastAPI    │─────────▶│  FastAPI task            │     │
│   │  image      │          │  port 8001               │     │
│   │             │          │                          │     │
│   │  Streamlit  │─────────▶│  Streamlit task          │     │
│   │  image      │          │  port 8502               │     │
│   └─────────────┘          └─────────────────────────┘     │
│                                          │                  │
│   ┌─────────────┐                        │                  │
│   │     S3      │◀───────────────────────┘                  │
│   │             │                                           │
│   │  Bronze     │                                           │
│   │  Silver     │                                           │
│   │  Gold       │                                           │
│   └─────────────┘                                           │
└─────────────────────────────────────────────────────────────┘
```

### Deployment steps (when ready)
```bash
# 1. Build and tag images
docker build -f Dockerfile.fastapi -t malaria-fastapi .
docker build -f Dockerfile.streamlit -t malaria-streamlit .

# 2. Push to ECR
aws ecr create-repository --repository-name malaria-fastapi
aws ecr create-repository --repository-name malaria-streamlit
docker push <account_id>.dkr.ecr.us-east-1.amazonaws.com/malaria-fastapi
docker push <account_id>.dkr.ecr.us-east-1.amazonaws.com/malaria-streamlit

# 3. Deploy on Fargate
# Register task definitions and create ECS services
# pointing to the ECR images above
```

> The deployment steps above are documented for reference.
> The system is currently run locally — see
> [How to Run Locally](#-how-to-run-locally) for setup instructions.

---

## 🗺️ Roadmap

- [ ] ECS Fargate + ECR deployment
- [ ] Automated retraining trigger on new OWID data detection
- [ ] Fairness audit dashboard across WHO regions
- [ ] API authentication for production WHO analyst access
- [ ] Alerting when PSI drift threshold is exceeded

---

## 👩‍💻 Author

Built by **Bee** as a data engineering and MLOps portfolio project
demonstrating production-grade pipeline design, ML model validation,
and health data interpretability.

---

## 📄 License

MIT License — free to use, adapt, and build on.