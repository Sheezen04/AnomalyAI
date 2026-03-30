import json
import logging
from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse, FileResponse, Http404
from django.core.paginator import Paginator
from core.models import UploadedReport
from reports.models import AnomalyResult

logger = logging.getLogger(__name__)


def result_table(request, report_id):
    report = get_object_or_404(UploadedReport, id=report_id)
    if report.status != 'done':
        from django.shortcuts import redirect
        return redirect('core:processing', report_id=report_id)

    result = get_object_or_404(AnomalyResult, report=report)

    # Filter controls
    show_anomalies_only = request.GET.get('anomaly_only', 'true').lower() == 'true'
    sort_by = request.GET.get('sort', 'anomaly_score')
    sort_dir = request.GET.get('dir', 'desc')
    page_num = request.GET.get('page', 1)

    records = result.anomaly_records
    if show_anomalies_only:
        records = [r for r in records if r.get('is_anomaly') == 1]

    # Sort
    reverse = sort_dir == 'desc'
    try:
        records = sorted(records, key=lambda x: x.get(sort_by, 0) or 0, reverse=reverse)
    except Exception:
        pass

    paginator = Paginator(records, 50)
    page_obj = paginator.get_page(page_num)

    visible_cols = [c for c in result.column_names if not c.startswith('_')]

    context = {
        'report': report,
        'result': result,
        'page_obj': page_obj,
        'column_names': visible_cols,
        'show_anomalies_only': show_anomalies_only,
        'sort_by': sort_by,
        'sort_dir': sort_dir,
        'total_anomalies': result.anomaly_count,
        'total_rows': result.total_rows,
        'anomaly_rate': result.anomaly_rate,
        'normal_count': result.total_rows - result.anomaly_count,
    }
    return render(request, 'reports/table.html', context)


def dashboard(request, report_id):
    report = get_object_or_404(UploadedReport, id=report_id)
    if report.status != 'done':
        from django.shortcuts import redirect
        return redirect('core:processing', report_id=report_id)

    result = get_object_or_404(AnomalyResult, report=report)
    return render(request, 'reports/dashboard.html', {
        'report': report,
        'result': result,
        'summary': result.summary_stats,
    })


def result_data_api(request, report_id):
    """Return anomaly records as JSON for AJAX table rendering."""
    report = get_object_or_404(UploadedReport, id=report_id)
    result = get_object_or_404(AnomalyResult, report=report)
    return JsonResponse({
        'records': result.anomaly_records,
        'columns': result.column_names,
        'total_rows': result.total_rows,
        'anomaly_count': result.anomaly_count,
    })


def chart_data_api(request, report_id):
    """Return summary stats JSON for Plotly charts."""
    report = get_object_or_404(UploadedReport, id=report_id)
    result = get_object_or_404(AnomalyResult, report=report)
    return JsonResponse(result.summary_stats)


def download_result(request, report_id):
    report = get_object_or_404(UploadedReport, id=report_id)
    result = get_object_or_404(AnomalyResult, report=report)

    import os

    # Strip extension from original filename for the download name
    base_name = os.path.splitext(report.original_filename)[0]
    download_name = f"anomaly_result_{base_name}.csv"

    # Try serving the saved ResultFile first
    try:
        result_file = result.result_file
        if result_file.file and os.path.isfile(result_file.file.path):
            return FileResponse(
                open(result_file.file.path, 'rb'),
                as_attachment=True,
                filename=download_name,
            )
    except Exception:
        pass  # Fall through to on-the-fly generation

    # Fallback: generate CSV on-the-fly from the JSON anomaly_records
    import io
    import csv
    from django.http import HttpResponse

    records = result.anomaly_records
    if not records:
        raise Http404("No result data available for download.")

    buf = io.StringIO()
    fieldnames = list(records[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)

    response = HttpResponse(buf.getvalue(), content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{download_name}"'
    return response


def history(request):
    reports = UploadedReport.objects.all()
    return render(request, 'reports/history.html', {'reports': reports})
