from django.urls import path
from . import views

app_name = 'reports'

urlpatterns = [
    path('<int:report_id>/', views.result_table, name='result_table'),
    path('<int:report_id>/dashboard/', views.dashboard, name='dashboard'),
    path('<int:report_id>/download/', views.download_result, name='download'),
    path('<int:report_id>/api/data/', views.result_data_api, name='result_data'),
    path('<int:report_id>/api/chart/', views.chart_data_api, name='chart_data'),
    path('history/', views.history, name='history'),
]
