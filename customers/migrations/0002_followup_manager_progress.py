from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("customers", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="customercase",
            name="fu1_manager",
            field=models.CharField(blank=True, default="", max_length=50, verbose_name="후기1 담당자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu1_progress_date",
            field=models.DateField(blank=True, null=True, verbose_name="후기1 진행일자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu2_manager",
            field=models.CharField(blank=True, default="", max_length=50, verbose_name="후기2 담당자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu2_progress_date",
            field=models.DateField(blank=True, null=True, verbose_name="후기2 진행일자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu3_manager",
            field=models.CharField(blank=True, default="", max_length=50, verbose_name="후기3 담당자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu3_progress_date",
            field=models.DateField(blank=True, null=True, verbose_name="후기3 진행일자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu4_manager",
            field=models.CharField(blank=True, default="", max_length=50, verbose_name="후기4 담당자"),
        ),
        migrations.AddField(
            model_name="customercase",
            name="fu4_progress_date",
            field=models.DateField(blank=True, null=True, verbose_name="후기4 진행일자"),
        ),
    ]
