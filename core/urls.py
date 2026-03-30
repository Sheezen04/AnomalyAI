from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    path('', views.home, name='home'),
    path('upload/', views.upload_file, name='upload'),
    path('processing/<int:report_id>/', views.processing, name='processing'),
    path('status/<int:report_id>/', views.check_status, name='check_status'),
]
