import os
import threading
import logging
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.conf import settings
from .models import UploadedReport

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls', '.pdf', '.docx'}


def home(request):
    recent_reports = UploadedReport.objects.all()[:10]
    return render(request, 'core/home.html', {'recent_reports': recent_reports})


@require_POST
def upload_file(request):
    if 'file' not in request.FILES:
        return JsonResponse({'error': 'No file uploaded.'}, status=400)

    uploaded_file = request.FILES['file']
    original_name = uploaded_file.name
    ext = os.path.splitext(original_name)[1].lower()

    if ext not in ALLOWED_EXTENSIONS:
        return JsonResponse({
            'error': f'Unsupported file type "{ext}". Allowed: {", ".join(ALLOWED_EXTENSIONS)}'
        }, status=400)

    file_type = ext.lstrip('.')

    report = UploadedReport.objects.create(
        file=uploaded_file,
        original_filename=original_name,
        file_type=file_type,
        file_size=uploaded_file.size,
        status='pending',
    )

    # Run pipeline in background thread (synchronous, no Celery)
    def _run():
        from ml_engine.pipeline import run_pipeline
        try:
            run_pipeline(report.id)
        except Exception as e:
            logger.error(f"Pipeline failed for report {report.id}: {e}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return JsonResponse({'report_id': report.id, 'redirect': f'/processing/{report.id}/'})


def processing(request, report_id):
    report = get_object_or_404(UploadedReport, id=report_id)
    return render(request, 'core/processing.html', {'report': report})


def check_status(request, report_id):
    report = get_object_or_404(UploadedReport, id=report_id)
    response = {
        'status': report.status,
        'filename': report.original_filename,
        'error': report.error_message,
    }
    if report.status == 'done':
        response['redirect'] = f'/reports/{report_id}/'
    return JsonResponse(response)
