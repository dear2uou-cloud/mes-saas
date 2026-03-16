from django.contrib import admin
from django.urls import include, path
from django.contrib.auth import views as django_auth_views

from .account_setup import setup_admin
from .auth_views import (
    find_id_view,
    find_password_view,
    landing_view,
    logout_to_login_with_next,
    password_reset_set_view,
    signup_view,
)

admin.site.has_permission = lambda request: request.user.is_active and request.user.is_superuser

urlpatterns = [
    path("", landing_view, name="landing"),
    path("admin/", admin.site.urls),
    path("accounts/login/", django_auth_views.LoginView.as_view(), name="login"),
    path("accounts/signup/", signup_view, name="signup"),
    path("accounts/find-id/", find_id_view, name="find_id"),
    path("accounts/find-password/", find_password_view, name="find_password"),
    path("accounts/password-reset-set/", password_reset_set_view, name="password_reset_set"),
    path("accounts/setup/", setup_admin, name="admin_setup"),
    path("accounts/logout/", logout_to_login_with_next, name="logout"),
    path("customers/", include("customers.urls")),
]
