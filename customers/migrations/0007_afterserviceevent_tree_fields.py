from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("customers", "0006_alter_audiogrampoint_left_db_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="afterserviceevent",
            name="amount",
            field=models.IntegerField(default=0, verbose_name="금액"),
        ),
        migrations.AddField(
            model_name="afterserviceevent",
            name="happened_on",
            field=models.DateField(blank=True, null=True, verbose_name="기준일"),
        ),
        migrations.AddField(
            model_name="afterserviceevent",
            name="memo",
            field=models.CharField(blank=True, default="", max_length=300, verbose_name="메모"),
        ),
        migrations.AddField(
            model_name="afterserviceevent",
            name="payment_method",
            field=models.CharField(blank=True, default="", max_length=20, verbose_name="결제수단"),
        ),
        migrations.AddField(
            model_name="afterserviceevent",
            name="reason",
            field=models.CharField(blank=True, default="", max_length=200, verbose_name="사유"),
        ),
        migrations.AddField(
            model_name="afterserviceevent",
            name="tax_type",
            field=models.CharField(blank=True, default="", max_length=10, verbose_name="과세구분"),
        ),
        migrations.AddField(
            model_name="afterserviceevent",
            name="parent_event",
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="children", to="customers.afterserviceevent", verbose_name="상위 이벤트"),
        ),
    ]
