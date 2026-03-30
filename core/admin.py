from django.contrib import admin
from .models import UploadedReport


@admin.register(UploadedReport)
class UploadedReportAdmin(admin.ModelAdmin):
    list_display = ['original_filename', 'file_type', 'status', 'file_size', 'uploaded_at']
    list_filter = ['status', 'file_type']
    search_fields = ['original_filename']
    readonly_fields = ['uploaded_at', 'updated_at']
