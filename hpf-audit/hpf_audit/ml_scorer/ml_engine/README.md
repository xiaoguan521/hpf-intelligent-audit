# ML Engine Architecture

## 1. Core Philosophy (Predictive Maintenance)
The ML Engine is designed to move Risk Management from **Reactive** to **Proactive**.
- **Reactive (Skills)**: Detects risks that *have already happened* (e.g., missed payment yesterday).
- **Proactive (ML)**: Predicts risks that *will likely happen* next month, allowing early intervention.

## 2. Dynamic & Meta-Driven
The engine is **Agnostic** to specific risk types. It adapts automatically as you add new Skills.
- **Dynamic Discovery**: It scans the database to find what risks (`FXLB`) are being reported.
- **Dynamic Training**: It trains a separate model for *each* risk type it finds.
- **Dynamic Inference**: It loads all available models and runs them in parallel.

## 3. The "Closed Loop" Workflow

### Phase A: Data Accumulation (Ground Truth)
1. **Agent Execution**: The Agent runs Skills (e.g., `LoanComplianceSkill`, `OrganizationAuditSkill`).
2. **Identification**: Skills identify specific risks (e.g., `malicious_arrears`) and write to `FX_SJ_JL`.
3. **Feedback**: Users/Agents verify these findings (confirming them as True Positives).
   - This data becomes the **Training Set** (Labels).

### Phase B: Learning (Training Pipeline)
**Script**: `ml_engine/train.py`
1. **Feature Extraction**:
   - Reads behavioral data (Balance, Deposit History, etc.).
   - **Crucially**, it reads *Historical Risk Events* (Feature Sync).
2. **Auto-Discovery**:
   - Queries `SELECT DISTINCT FXLB FROM FX_SJ_JL`.
   - Example: Finds `['malicious_arrears', 'high_dti']`.
3. **Model Training**:
   - For each type, trains a **RandomForestClassifier**.
   - Input: Historical behavior (Features).
   - Output: Probability of this specific risk (Label).
   - Saves: `model_malicious_arrears.pkl`, `model_high_dti.pkl`.

### Phase C: Prediction (Inference Pipeline)
**Script**: `ml_engine/main.py` -> `ml_engine/model.py`
1. **Orchestration**: Periodically scans all active users/units.
2. **Multi-Model Inference**:
   - Loads all `model_*.pkl` files.
   - Runs every user through every model.
3. **Alerting**:
   - If `Prob(malicious_arrears) > 50%`:
   - Inserts a **Warning** into `FX_SJ_JL`:
     - Type: `malicious_arrears` (same type, but source is Predictive).
     - Link: "Predicted by ML Model".

## 4. Technical Components
| Component | File | Responsibility |
| :--- | :--- | :--- |
| **Orchestrator** | `main.py` | Runs the daily pipeline. Connects data to models. |
| **Feature Engineer** | `features.py` | Converts raw DB rows into mathematical vectors (Features). |
| **Trainer** | `train.py` | The "Teacher". Reads labels, builds models, saves `.pkl`. |
| **Inference Engine** | `model.py` | The "Predictor". Loads `.pkl`, outputs probabilities. |

## 5. Cold Start Strategy
- **Day 0**: No history. ML cannot train. System falls back to **Legacy Rules** (Hardcoded simple logic) to provide basic protection.
- **Day 30+**: Sufficient data accumulates. `train.py` successfully produces models. System automatically switches to **AI Mode**.
