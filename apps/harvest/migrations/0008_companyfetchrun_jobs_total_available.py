from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('harvest', '0007_rename_harvest_rawjob_company_platform_idx_harvest_raw_company_4ed860_idx_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='companyfetchrun',
            name='jobs_total_available',
            field=models.PositiveIntegerField(
                default=0,
                help_text='Total jobs reported by the platform API (even if we only fetched a subset)',
            ),
        ),
    ]
