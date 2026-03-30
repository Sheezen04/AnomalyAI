from django.db import models
import os


def upload_to(instance, filename):
    return f'uploads/{filename}'


class UploadedReport(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('done', 'Done'),
        ('failed', 'Failed'),
    ]

    file = models.FileField(upload_to=upload_to)
    original_filename = models.CharField(max_length=255)
    file_type = models.CharField(max_length=20)  # csv, xlsx, pdf, docx
    file_size = models.PositiveIntegerField(default=0)  # bytes
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(blank=True, null=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-uploaded_at']
        verbose_name = 'Uploaded Report'
        verbose_name_plural = 'Uploaded Reports'

    def __str__(self):
        return f"{self.original_filename} ({self.status})"

    @property
    def filename(self):
        return os.path.basename(self.file.name)
