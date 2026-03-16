from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("customers", "0007_afterserviceevent_tree_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="afterservice",
            name="paid_amount",
            field=models.IntegerField(default=0, verbose_name="결제 금액"),
        ),
    ]
