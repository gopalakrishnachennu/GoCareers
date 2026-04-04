"""Unread inbound message count for nav badge."""

from messaging.models import Message


def unread_messages_count(request):
    if not request.user.is_authenticated:
        return {"unread_message_count": 0}
    n = (
        Message.objects.filter(
            thread__participants=request.user,
            is_read=False,
        )
        .exclude(sender=request.user)
        .count()
    )
    return {"unread_message_count": n}
