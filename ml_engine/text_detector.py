"""
Text anomaly detection using TF-IDF + statistical reconstruction error (Autoencoder-like approach).
Uses lightweight sklearn-based approach that works without PyTorch/GPU for fast deployment.
For organizations with GPU: swap in sentence-transformers for BERT embeddings.
"""
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import normalize


def detect_text_anomalies(df, text_cols: list):
    """
    Compute anomaly scores for text columns using TF-IDF + SVD reconstruction error.
    This simulates an autoencoder: encode to low-dim, decode back, measure error.

    Args:
        df: cleaned DataFrame
        text_cols: list of text column names

    Returns:
        scores (ndarray): per-row anomaly score (0-1, higher = more anomalous)
    """
    if not text_cols or df.empty:
        return np.zeros(len(df))

    # Combine all text columns into a single corpus per row
    combined_texts = df[text_cols].astype(str).agg(' '.join, axis=1).tolist()
    n = len(combined_texts)

    if n < 3:
        return np.zeros(n)

    # TF-IDF vectorization
    vectorizer = TfidfVectorizer(
        max_features=min(500, n * 10),
        ngram_range=(1, 2),
        min_df=1,
        stop_words='english',
        sublinear_tf=True
    )

    try:
        X = vectorizer.fit_transform(combined_texts)
    except ValueError:
        return np.zeros(n)

    # SVD dimensionality reduction (like encoder)
    n_components = min(20, X.shape[1] - 1, n - 1)
    if n_components < 2:
        return np.zeros(n)

    svd = TruncatedSVD(n_components=n_components, random_state=42)
    X_encoded = svd.fit_transform(X)  # encode
    X_decoded = svd.inverse_transform(X_encoded)  # decode

    # Normalize
    X_orig = normalize(X.toarray(), norm='l2')
    X_recon = normalize(X_decoded, norm='l2')

    # Reconstruction error per row = anomaly score
    errors = np.mean((X_orig - X_recon) ** 2, axis=1)

    # Normalize to 0-1
    min_e, max_e = errors.min(), errors.max()
    if max_e > min_e:
        scores = (errors - min_e) / (max_e - min_e)
    else:
        scores = np.zeros(n)

    return scores.round(4)
