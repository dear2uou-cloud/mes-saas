from __future__ import annotations

from urllib.parse import urlencode

from django.contrib.auth import logout
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.conf import settings


def logout_to_login_with_next(request: HttpRequest) -> HttpResponse:
    """
    Logs out current user and redirects to /accounts/login/ with ?next=<target>.
    Target priority:
      1) request.POST['next'] or request.GET['next']
      2) HTTP_REFERER path (if same host)
      3) settings.LOGIN_REDIRECT_URL
    """
    next_target = request.POST.get("next") or request.GET.get("next") or ""
    if not next_target:
        ref = request.META.get("HTTP_REFERER", "")
        # Only keep path+query if same host; else ignore.
        try:
            if ref.startswith("http"):
                # crude parse without external libs
                # keep after domain
                parts = ref.split("//", 1)[1].split("/", 1)
                ref_path = "/" + parts[1] if len(parts) > 1 else "/"
            else:
                ref_path = ref
            next_target = ref_path or ""
        except Exception:
            next_target = ""
    if not next_target:
        next_target = getattr(settings, "LOGIN_REDIRECT_URL", "/")

    logout(request)

    login_url = getattr(settings, "LOGIN_URL", "/accounts/login/")
    qs = urlencode({"next": next_target})
    return redirect(f"{login_url}?{qs}")
