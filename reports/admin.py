from django.contrib import admin
from .models import AnomalyResult, ResultFile


@admin.register(AnomalyResult)
class AnomalyResultAdmin(admin.ModelAdmin):
    list_display = ['report', 'total_rows', 'anomaly_count', 'anomaly_rate', 'created_at']
    readonly_fields = ['created_at']

    def anomaly_rate(self, obj):
        return f"{obj.anomaly_rate}%"
    anomaly_rate.short_description = 'Anomaly Rate'


@admin.register(ResultFile)
class ResultFileAdmin(admin.ModelAdmin):
    list_display = ['result', 'file', 'created_at']
    readonly_fields = ['created_at']
