from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("customers", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="inventoryproductmodel",
            name="importer_name",
            field=models.CharField(blank=True, default="", max_length=120, verbose_name="제조수입업소명"),
        ),
    ]
