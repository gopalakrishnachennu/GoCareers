from .models import Job

class JobService:
    @staticmethod
    def clone_job(job_id, user):
        """
        Clones a job and returns the new job instance.
        - Sets status to DRAFT.
        - Appends '(Copy)' to title.
        - Copies M2M relations clearly.
        """
        try:
            original_job = Job.objects.get(pk=job_id)
            
            # Create shallow copy
            new_job = Job.objects.get(pk=job_id)
            new_job.pk = None
            new_job.id = None
            new_job.title = f"{original_job.title} (Copy)"
            new_job.status = Job.Status.DRAFT
            new_job.posted_by = user
            new_job.save()

            # Copy M2M relationships
            new_job.marketing_roles.set(original_job.marketing_roles.all())
            
            return new_job
        except Job.DoesNotExist:
            return None
