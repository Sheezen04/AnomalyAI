"""
Main pipeline orchestrator.
Called synchronously from Django views (no Celery needed).
"""
import os
import io
import csv
import time
import logging
from django.core.files.base import ContentFile

logger = logging.getLogger(__name__)


def run_pipeline(report_id: int) -> dict:
    """
    Full end-to-end anomaly detection pipeline.
    Returns result dict or raises on failure.
    """
    from core.models import UploadedReport
    from reports.models import AnomalyResult, ResultFile
    from ml_engine.loader import load_file
    from ml_engine.preprocessor import preprocess
    from ml_engine.numerical_detector import detect_numerical_anomalies
    from ml_engine.text_detector import detect_text_anomalies
    from ml_engine.aggregator import aggregate_results

    report = UploadedReport.objects.get(id=report_id)
    report.status = 'processing'
    report.save(update_fields=['status'])

    try:
        file_path = report.file.path
        file_type = report.file_type
        pipeline_start = time.time()

        logger.info(f"[Pipeline] Loading file: {file_path} (type={file_type})")

        # Step 1: Load
        df = load_file(file_path, file_type)

        if df.empty or len(df) == 0:
            raise ValueError("The uploaded file contains no data rows.")

        # Limit to reasonable size
        if len(df) > 10_000:
            df = df.head(10_000)

        logger.info(f"[Pipeline] Loaded {len(df)} rows, {len(df.columns)} columns")

        # Step 2: Preprocess
        df_clean, num_cols, text_cols, num_data = preprocess(df)

        logger.info(f"[Pipeline] Numerical cols: {num_cols}, Text cols: {text_cols}")

        # Step 3: Numerical anomaly detection
        contamination = 0.1
        num_method = 'isolation_forest'
        num_scores, num_labels = None, None
        if num_data is not None and len(num_cols) > 0:
            t0 = time.time()
            num_scores, num_labels = detect_numerical_anomalies(
                num_data, method=num_method, contamination=contamination
            )
            logger.info(f"[Pipeline] Numerical anomalies found: {num_labels.sum()} in {time.time()-t0:.2f}s")

        # Step 4: Text anomaly detection
        text_scores = None
        if text_cols:
            t0 = time.time()
            text_scores = detect_text_anomalies(df_clean, text_cols)
            logger.info(f"[Pipeline] Text anomaly scores computed in {time.time()-t0:.2f}s")

        execution_seconds = round(time.time() - pipeline_start, 2)

        # Step 5: Aggregate results
        result_data = aggregate_results(
            df=df_clean,
            num_cols=num_cols,
            text_cols=text_cols,
            num_scores=num_scores,
            num_labels=num_labels,
            text_scores=text_scores,
        )

        # Inject algorithm metadata into summary_stats
        result_data['summary_stats']['execution_seconds'] = execution_seconds
        result_data['summary_stats']['algorithm'] = 'Isolation Forest' if num_method == 'isolation_forest' else 'One-Class SVM'
        result_data['summary_stats']['text_algorithm'] = 'TF-IDF + SVD Autoencoder'
        result_data['summary_stats']['contamination'] = contamination
        result_data['summary_stats']['threshold'] = 0.5

        # Step 6: Save to DB
        anomaly_result, created = AnomalyResult.objects.update_or_create(
            report=report,
            defaults={
                'total_rows': result_data['total_rows'],
                'anomaly_count': result_data['anomaly_count'],
                'anomaly_records': result_data['anomaly_records'],
                'summary_stats': result_data['summary_stats'],
                'column_names': result_data['column_names'],
                'numerical_columns': result_data['numerical_columns'],
                'text_columns': result_data['text_columns'],
            }
        )

        # Step 7: Save result CSV copy
        _save_result_csv(anomaly_result, result_data)

        report.status = 'done'
        report.save(update_fields=['status'])

        logger.info(f"[Pipeline] Done. {result_data['anomaly_count']}/{result_data['total_rows']} anomalies.")
        return result_data

    except Exception as e:
        logger.error(f"[Pipeline] Error: {e}")
        report.status = 'failed'
        report.error_message = str(e)
        report.save(update_fields=['status', 'error_message'])
        raise


def _save_result_csv(anomaly_result, result_data):
    """Write result records to a CSV file and attach to AnomalyResult."""
    from reports.models import ResultFile

    records = result_data['anomaly_records']
    if not records:
        return

    buf = io.StringIO()
    fieldnames = list(records[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)
    csv_content = buf.getvalue().encode('utf-8')

    filename = f"result_{anomaly_result.report.id}_{anomaly_result.report.original_filename}.csv"
    result_file, _ = ResultFile.objects.update_or_create(
        result=anomaly_result,
        defaults={}
    )
    result_file.file.save(filename, ContentFile(csv_content), save=True)
