from django.db import models
from core.models import UploadedReport


class AnomalyResult(models.Model):
    report = models.OneToOneField(UploadedReport, on_delete=models.CASCADE, related_name='result')
    total_rows = models.IntegerField(default=0)
    anomaly_count = models.IntegerField(default=0)
    anomaly_records = models.JSONField(default=list)   # List of row dicts with scores
    summary_stats = models.JSONField(default=dict)     # Aggregated stats for dashboard
    column_names = models.JSONField(default=list)      # Column list for display
    numerical_columns = models.JSONField(default=list)
    text_columns = models.JSONField(default=list)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Anomaly Result'
        verbose_name_plural = 'Anomaly Results'

    def __str__(self):
        return f"Result for {self.report.original_filename} ({self.anomaly_count} anomalies)"

    @property
    def anomaly_rate(self):
        if self.total_rows == 0:
            return 0
        return round((self.anomaly_count / self.total_rows) * 100, 2)


class ResultFile(models.Model):
    result = models.OneToOneField(AnomalyResult, on_delete=models.CASCADE, related_name='result_file')
    file = models.FileField(upload_to='results/')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Result file for {self.result.report.original_filename}"
