"""
Aggregator: combines numerical + text anomaly scores into final results.
Generates summary statistics for the dashboard.
"""
import numpy as np
import pandas as pd
from typing import Optional


def aggregate_results(
    df: pd.DataFrame,
    num_cols: list,
    text_cols: list,
    num_scores: Optional[np.ndarray],
    num_labels: Optional[np.ndarray],
    text_scores: Optional[np.ndarray],
    threshold: float = 0.5,
    num_weight: float = 0.6,
    text_weight: float = 0.4,
) -> dict:
    """
    Combine numerical and text anomaly scores.

    Returns:
        dict with keys: anomaly_records, summary_stats, column_names,
                        total_rows, anomaly_count
    """
    n = len(df)

    # Build combined anomaly score
    if num_scores is not None and len(num_scores) == n and text_scores is not None and len(text_scores) == n:
        combined = num_weight * num_scores + text_weight * text_scores
    elif num_scores is not None and len(num_scores) == n:
        combined = num_scores.copy()
    elif text_scores is not None and len(text_scores) == n:
        combined = text_scores.copy()
    else:
        combined = np.zeros(n)

    combined = np.clip(combined, 0, 1)

    # Label rows above threshold as anomalies
    is_anomaly = (combined >= threshold).astype(int)
    if num_labels is not None and len(num_labels) == n:
        is_anomaly = np.maximum(is_anomaly, num_labels)

    # Build records list
    anomaly_records = []
    for i in range(n):
        row = df.iloc[i].to_dict()
        # Convert non-serializable types
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, (np.integer,)):
                clean_row[str(k)] = int(v)
            elif isinstance(v, (np.floating, float)):
                clean_row[str(k)] = float(round(v, 4)) if not np.isnan(v) else None
            else:
                clean_row[str(k)] = str(v) if v is not None else ''
        clean_row['anomaly_score'] = float(combined[i])
        clean_row['is_anomaly'] = int(is_anomaly[i])
        clean_row['num_anomaly_score'] = float(num_scores[i]) if num_scores is not None and len(num_scores) == n else None
        clean_row['text_anomaly_score'] = float(text_scores[i]) if text_scores is not None and len(text_scores) == n else None
        clean_row['_row_index'] = i
        anomaly_records.append(clean_row)

    total_rows = n
    anomaly_count = int(is_anomaly.sum())

    # Summary statistics for dashboard
    summary_stats = _build_summary_stats(
        df, num_cols, text_cols, combined, is_anomaly, anomaly_records
    )

    visible_cols = [c for c in df.columns if not c.startswith('_')]
    all_cols = visible_cols + ['anomaly_score', 'is_anomaly']

    return {
        'anomaly_records': anomaly_records,
        'summary_stats': summary_stats,
        'column_names': all_cols,
        'total_rows': total_rows,
        'anomaly_count': anomaly_count,
        'numerical_columns': num_cols,
        'text_columns': text_cols,
    }


def _build_summary_stats(df, num_cols, text_cols, combined_scores, is_anomaly, records):
    n = len(df)
    anomaly_count = int(is_anomaly.sum())
    normal_count = n - anomaly_count

    # Score distribution (histogram buckets)
    hist, bin_edges = np.histogram(combined_scores, bins=10, range=(0, 1))
    score_histogram = {
        'labels': [f"{round(bin_edges[i], 2)}-{round(bin_edges[i+1], 2)}" for i in range(len(hist))],
        'values': hist.tolist()
    }

    # Top anomalous rows (sorted by score)
    sorted_records = sorted(records, key=lambda x: x['anomaly_score'], reverse=True)
    top_anomalies = sorted_records[:min(10, len(sorted_records))]

    # Per-column anomaly contribution (only numerical)
    col_anomaly_scores = {}
    for col in num_cols:
        if col in df.columns:
            anomaly_vals = df[col][is_anomaly == 1].values
            normal_vals = df[col][is_anomaly == 0].values
            col_anomaly_scores[col] = {
                'anomaly_mean': float(np.nanmean(anomaly_vals)) if len(anomaly_vals) > 0 else 0,
                'normal_mean': float(np.nanmean(normal_vals)) if len(normal_vals) > 0 else 0,
                'std': float(np.nanstd(df[col].values)),
                'anomaly_count': int((is_anomaly == 1).sum()),
            }

    # Score by row (scatter data)
    score_scatter = {
        'x': list(range(n)),
        'y': [float(s) for s in combined_scores],
        'is_anomaly': [int(a) for a in is_anomaly],
    }

    # Correlation matrix (numerical cols only, max 10 cols)
    corr_matrix = {}
    if len(num_cols) >= 2:
        sub = df[num_cols[:10]].copy()
        corr = sub.corr().round(3)
        corr_matrix = {
            'columns': list(corr.columns),
            'values': corr.values.tolist()
        }

    # Detect date column for time-series
    time_series = _extract_time_series(df, combined_scores, is_anomaly)

    return {
        'total_rows': n,
        'anomaly_count': anomaly_count,
        'normal_count': normal_count,
        'anomaly_rate': round((anomaly_count / max(n, 1)) * 100, 2),
        'avg_anomaly_score': float(np.mean(combined_scores)),
        'max_anomaly_score': float(np.max(combined_scores)) if len(combined_scores) > 0 else 0,
        'score_histogram': score_histogram,
        'score_scatter': score_scatter,
        'col_anomaly_scores': col_anomaly_scores,
        'corr_matrix': corr_matrix,
        'top_anomalies': top_anomalies,
        'time_series': time_series,
        'num_cols_count': len(num_cols),
        'text_cols_count': len(text_cols),
    }


def _extract_time_series(df, combined_scores, is_anomaly):
    """Try to find a date/time column and build time-series data."""
    date_col = None
    for col in df.columns:
        if col.startswith('_'):
            continue
        try:
            parsed = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')
            if parsed.notna().sum() / max(len(df), 1) > 0.5:
                date_col = col
                df = df.copy()
                df['_parsed_date'] = parsed
                break
        except Exception:
            continue

    if date_col is None:
        return None

    df_ts = df[['_parsed_date']].copy()
    df_ts['anomaly_score'] = combined_scores
    df_ts['is_anomaly'] = is_anomaly
    df_ts = df_ts.dropna(subset=['_parsed_date']).sort_values('_parsed_date')

    return {
        'dates': [str(d)[:10] for d in df_ts['_parsed_date']],
        'scores': [float(s) for s in df_ts['anomaly_score']],
        'is_anomaly': [int(a) for a in df_ts['is_anomaly']],
        'date_col': date_col,
    }
