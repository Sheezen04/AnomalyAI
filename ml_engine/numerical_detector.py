"""
Numerical anomaly detection using Isolation Forest and One-Class SVM.
"""
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler


def detect_numerical_anomalies(num_data: np.ndarray, method: str = 'isolation_forest', contamination: float = 0.1):
    """
    Detect anomalies in numerical data.

    Args:
        num_data: (n_samples, n_features) array of scaled numerical values
        method: 'isolation_forest' or 'one_class_svm'
        contamination: expected fraction of anomalies (0.0 - 0.5)

    Returns:
        scores (ndarray): anomaly scores per row (higher = more anomalous, 0-1)
        labels (ndarray): 1 = anomaly, 0 = normal
    """
    if num_data is None or num_data.shape[0] == 0:
        return np.array([]), np.array([])

    n_samples = num_data.shape[0]
    contamination = min(contamination, 0.5)
    contamination = max(contamination, 0.01)

    # Re-scale for SVM
    scaler = StandardScaler()
    X = scaler.fit_transform(num_data)

    if method == 'one_class_svm' and n_samples > 10:
        model = OneClassSVM(nu=contamination, kernel='rbf', gamma='scale')
        model.fit(X)
        raw_scores = model.decision_function(X)
        labels_raw = model.predict(X)  # -1 = anomaly, 1 = normal
    else:
        # Default: Isolation Forest
        model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X)
        raw_scores = model.decision_function(X)   # negative = more anomalous
        labels_raw = model.predict(X)              # -1 = anomaly, 1 = normal

    # Normalize scores to 0-1 range (1 = most anomalous)
    min_s, max_s = raw_scores.min(), raw_scores.max()
    if max_s > min_s:
        normalized = 1.0 - (raw_scores - min_s) / (max_s - min_s)
    else:
        normalized = np.zeros(len(raw_scores))

    labels = (labels_raw == -1).astype(int)

    return normalized.round(4), labels
