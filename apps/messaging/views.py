from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, DetailView, View
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Q
from .models import Thread, Message
from .forms import MessageForm
from users.models import User

class InboxView(LoginRequiredMixin, ListView):
    model = Thread
    template_name = 'messaging/inbox.html'
    context_object_name = 'threads'

    def get_queryset(self):
        return self.request.user.threads.all().prefetch_related('participants', 'messages')

class ThreadDetailView(LoginRequiredMixin, View):
    def get(self, request, pk):
        thread = get_object_or_404(Thread, pk=pk)
        if request.user not in thread.participants.all():
            return redirect('inbox')
            
        messages = thread.messages.all()
        form = MessageForm()
        
        # Mark other's messages as read
        messages.exclude(sender=request.user).update(is_read=True)
        
        return render(request, 'messaging/thread_detail.html', {
            'thread': thread,
            'messages': messages,
            'form': form
        })

    def post(self, request, pk):
        thread = get_object_or_404(Thread, pk=pk)
        if request.user not in thread.participants.all():
            return redirect('inbox')
            
        form = MessageForm(request.POST)
        if form.is_valid():
            message = form.save(commit=False)
            message.thread = thread
            message.sender = request.user
            message.save()
            thread.save() # Update updated_at
            return redirect('thread-detail', pk=pk)
            
        return render(request, 'messaging/thread_detail.html', {
            'thread': thread,
            'messages': thread.messages.all(),
            'form': form
        })

from django.db import transaction

class StartThreadView(LoginRequiredMixin, View):
    def get(self, request, user_id):
        other_user = get_object_or_404(User, pk=user_id)
        if other_user == request.user:
            return redirect('inbox')
            
        # 1. Role Check: Prevent Consultant <-> Consultant
        if request.user.role == User.Role.CONSULTANT and other_user.role == User.Role.CONSULTANT:
            # Optionally redirect with error, or just fail silently/redirect to inbox
            return redirect('inbox')
            
        # Check if thread exists
        thread = Thread.objects.filter(participants=request.user).filter(participants=other_user).first()
        
        if not thread:
            with transaction.atomic():
                # Double check inside transaction if possible, or just create
                # (For strict uniqueness, we'd need a unique constraints on a "participants_hash" field or similar)
                thread = Thread.objects.create()
                thread.participants.add(request.user, other_user)
            
        return redirect('thread-detail', pk=thread.pk)
