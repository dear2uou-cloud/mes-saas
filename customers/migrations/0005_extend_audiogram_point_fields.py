from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("customers", "0004_audiogram_models"),
    ]

    operations = [
        migrations.AddField(
            model_name="audiogrampoint",
            name="mask_right_db",
            field=models.IntegerField(blank=True, null=True, verbose_name="차폐(우)"),
        ),
        migrations.AddField(
            model_name="audiogrampoint",
            name="mask_left_db",
            field=models.IntegerField(blank=True, null=True, verbose_name="차폐(좌)"),
        ),
        migrations.AddField(
            model_name="audiogrampoint",
            name="bone_right_db",
            field=models.IntegerField(blank=True, null=True, verbose_name="골도(우)"),
        ),
        migrations.AddField(
            model_name="audiogrampoint",
            name="bone_left_db",
            field=models.IntegerField(blank=True, null=True, verbose_name="골도(좌)"),
        ),
    ]
