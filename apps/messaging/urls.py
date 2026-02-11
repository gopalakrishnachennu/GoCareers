from django.urls import path
from .views import InboxView, ThreadDetailView, StartThreadView

urlpatterns = [
    path('', InboxView.as_view(), name='inbox'),
    path('thread/<int:pk>/', ThreadDetailView.as_view(), name='thread-detail'),
    path('start/<int:user_id>/', StartThreadView.as_view(), name='start-thread'),
]
