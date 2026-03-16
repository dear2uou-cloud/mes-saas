from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("customers", "0008_afterservice_paid_amount"),
    ]

    operations = [
        migrations.CreateModel(
            name="UserAuthProfile",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("phone", models.CharField(max_length=20, unique=True, verbose_name="휴대폰번호")),
                ("phone_verified", models.BooleanField(default=False, verbose_name="휴대폰 인증 여부")),
                ("phone_verified_at", models.DateTimeField(blank=True, null=True, verbose_name="휴대폰 인증일시")),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("user", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="auth_profile", to=settings.AUTH_USER_MODEL, verbose_name="계정")),
            ],
            options={
                "verbose_name": "계정 인증 프로필",
                "verbose_name_plural": "계정 인증 프로필",
            },
        ),
    ]
