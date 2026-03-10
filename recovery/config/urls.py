from django.contrib import admin
from django.urls import path, include
from django.contrib.auth import views as auth_views
from django.shortcuts import redirect

from .account_setup import setup_admin
from .auth_views import logout_to_login_with_next

# ✅ admin은 운영자(superuser)만 접근 가능
admin.site.has_permission = lambda request: request.user.is_active and request.user.is_superuser

urlpatterns = [
    # 루트(/) 접속 시 고객 목록으로 이동
    path("", lambda request: redirect("/customers/")),

    path("admin/", admin.site.urls),

    # 로그인/로그아웃
    path("accounts/login/", auth_views.LoginView.as_view(), name="login"),
    path("accounts/setup/", setup_admin, name="admin_setup"),
    path("accounts/logout/", logout_to_login_with_next, name="logout"),

    # 앱
    path("customers/", include("customers.urls")),
]
