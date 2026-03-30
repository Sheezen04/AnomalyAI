"""
Preprocessor: cleans and separates numerical vs. textual columns.
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler


def preprocess(df: pd.DataFrame):
    """
    Returns:
        df_clean (DataFrame): cleaned dataframe
        num_cols (list): numerical column names
        text_cols (list): text column names
        num_data (ndarray): scaled numerical matrix
    """
    df = df.copy()

    # Drop fully-empty rows & columns
    df.dropna(how='all', inplace=True)
    df = df.loc[:, df.notna().any()]

    # Reset index
    df.reset_index(drop=True, inplace=True)

    # Identify numerical and categorical/text columns
    num_cols = []
    text_cols = []

    for col in df.columns:
        # Skip internal helper columns
        if col.startswith('_'):
            continue
        # Try converting to numeric
        converted = pd.to_numeric(df[col], errors='coerce')
        non_null_frac = converted.notna().sum() / max(len(df), 1)
        if non_null_frac >= 0.5:
            df[col] = converted
            num_cols.append(col)
        else:
            # Treat as text
            df[col] = df[col].fillna('').astype(str)
            if df[col].str.len().mean() > 2:  # skip trivially short cols
                text_cols.append(col)

    # Fill numerical NaNs with median
    for col in num_cols:
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)

    # Scale numerical data
    num_data = None
    if num_cols:
        scaler = MinMaxScaler()
        num_data = scaler.fit_transform(df[num_cols].values.astype(float))

    return df, num_cols, text_cols, num_data
