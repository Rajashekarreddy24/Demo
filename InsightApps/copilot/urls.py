from django.urls import path
from .views import *

urlpatterns = [
    path('copilot/',GetServerTablesList.as_view(),name='Get Query Set Details'),
]