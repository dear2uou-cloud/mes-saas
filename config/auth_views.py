from __future__ import annotations

from urllib.parse import urlencode

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model, logout
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from customers.models import UserAuthProfile


User = get_user_model()
PHONE_CODE = "123456"
PHONE_SESSION_KEY = "signup_phone_verified"
PHONE_TARGET_KEY = "signup_phone_target"
RESET_TARGET_KEY = "password_reset_target_user_id"


def landing_view(request: HttpRequest) -> HttpResponse:
    login_url = getattr(settings, "LOGIN_URL", "/accounts/login/")
    return redirect(login_url)


def logout_to_login_with_next(request: HttpRequest) -> HttpResponse:
    next_target = request.POST.get("next") or request.GET.get("next") or ""
    if not next_target:
        ref = request.META.get("HTTP_REFERER", "")
        try:
            if ref.startswith("http"):
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


def signup_view(request: HttpRequest) -> HttpResponse:
    raw_phone = request.POST.get("phone", "").strip()
    normalized_phone = _normalize_phone(raw_phone)
    form_data = {
        "username": request.POST.get("username", "").strip(),
        "email": request.POST.get("email", "").strip(),
        "phone": _format_phone(raw_phone),
    }
    phone_verified = bool(
        request.session.get(PHONE_SESSION_KEY)
        and request.session.get(PHONE_TARGET_KEY) == normalized_phone
        and normalized_phone
    )

    if request.method == "POST":
        action = request.POST.get("action", "signup")
        username = form_data["username"]
        email = form_data["email"]
        phone = normalized_phone
        phone_display = _format_phone(phone)
        password1 = request.POST.get("password1", "")
        password2 = request.POST.get("password2", "")
        phone_code = request.POST.get("phone_code", "").strip()

        if action == "send_code":
            form_data["phone"] = phone_display
            if not phone:
                messages.error(request, "휴대폰번호를 입력해 주세요.")
            elif len(phone) != 11:
                messages.error(request, "휴대폰번호는 010-1234-5678 형식으로 입력해 주세요.")
            else:
                request.session[PHONE_TARGET_KEY] = phone
                request.session[PHONE_SESSION_KEY] = False
                messages.success(request, "인증번호가 발송되었습니다. 현재 구조 단계에서는 123456을 입력해 주세요.")
        elif action == "verify_code":
            form_data["phone"] = phone_display
            target = request.session.get(PHONE_TARGET_KEY, "")
            if not phone or len(phone) != 11:
                messages.error(request, "휴대폰번호는 010-1234-5678 형식으로 입력해 주세요.")
            elif target != phone:
                messages.error(request, "먼저 인증번호 받기를 진행해 주세요.")
            elif phone_code != PHONE_CODE:
                messages.error(request, "인증번호가 올바르지 않습니다.")
            else:
                request.session[PHONE_SESSION_KEY] = True
                phone_verified = True
                messages.success(request, "휴대폰 인증이 완료되었습니다.")
        else:
            form_data["phone"] = phone_display
            if not username or not email or not phone or not password1 or not password2:
                messages.error(request, "모든 항목을 입력해 주세요.")
            elif password1 != password2:
                messages.error(request, "비밀번호가 서로 일치하지 않습니다.")
            elif User.objects.filter(username=username).exists():
                messages.error(request, "이미 사용 중인 아이디입니다.")
            elif User.objects.filter(email=email).exists():
                messages.error(request, "이미 사용 중인 이메일입니다.")
            elif len(phone) != 11:
                messages.error(request, "휴대폰번호는 010-1234-5678 형식으로 입력해 주세요.")
            elif UserAuthProfile.objects.filter(phone=phone).exists():
                messages.error(request, "이미 사용 중인 휴대폰번호입니다.")
            elif not phone_verified:
                messages.error(request, "휴대폰 인증을 완료해 주세요.")
            else:
                user = User.objects.create_user(username=username, email=email, password=password1)
                UserAuthProfile.objects.create(
                    user=user,
                    phone=phone,
                    phone_verified=True,
                    phone_verified_at=timezone.now(),
                )
                request.session.pop(PHONE_SESSION_KEY, None)
                request.session.pop(PHONE_TARGET_KEY, None)
                messages.success(request, "회원가입이 완료되었습니다. 로그인해 주세요.")
                return redirect("login")

    return render(
        request,
        "registration/signup.html",
        {"form_data": form_data, "phone_verified": phone_verified},
    )


def find_id_view(request: HttpRequest) -> HttpResponse:
    method = request.POST.get("method", "email") if request.method == "POST" else "email"
    value = ""
    result_usernames: list[str] = []

    if request.method == "POST":
        if method == "phone":
            value = _normalize_phone(request.POST.get("phone", ""))
            if not value:
                messages.error(request, "휴대폰번호를 입력해 주세요.")
            else:
                result_usernames = list(
                    User.objects.filter(auth_profile__phone=value).values_list("username", flat=True)
                )
        else:
            value = request.POST.get("email", "").strip()
            if not value:
                messages.error(request, "이메일을 입력해 주세요.")
            else:
                result_usernames = list(User.objects.filter(email=value).values_list("username", flat=True))

        if not result_usernames and value:
            messages.error(request, "일치하는 계정을 찾지 못했습니다.")

    return render(
        request,
        "registration/find_id.html",
        {"method": method, "value": value, "result_usernames": result_usernames},
    )


def find_password_view(request: HttpRequest) -> HttpResponse:
    method = request.POST.get("method", "email") if request.method == "POST" else "email"
    username = request.POST.get("username", "").strip() if request.method == "POST" else ""
    value = ""

    if request.method == "POST":
        if not username:
            messages.error(request, "아이디를 입력해 주세요.")
        else:
            qs = User.objects.filter(username=username)
            if method == "phone":
                value = _normalize_phone(request.POST.get("phone", ""))
                if not value:
                    messages.error(request, "휴대폰번호를 입력해 주세요.")
                else:
                    qs = qs.filter(auth_profile__phone=value)
            else:
                value = request.POST.get("email", "").strip()
                if not value:
                    messages.error(request, "이메일을 입력해 주세요.")
                else:
                    qs = qs.filter(email=value)

            user = qs.first() if username and value else None
            if user:
                request.session[RESET_TARGET_KEY] = user.id
                messages.success(request, "본인 확인이 완료되었습니다. 새 비밀번호를 설정해 주세요.")
                return redirect("password_reset_set")
            elif username and value:
                messages.error(request, "입력한 정보와 일치하는 계정을 찾지 못했습니다.")

    return render(
        request,
        "registration/find_password.html",
        {"method": method, "value": value, "username": username},
    )


def password_reset_set_view(request: HttpRequest) -> HttpResponse:
    user_id = request.session.get(RESET_TARGET_KEY)
    if not user_id:
        messages.error(request, "먼저 본인 확인을 진행해 주세요.")
        return redirect("find_password")

    user = User.objects.filter(id=user_id).first()
    if not user:
        request.session.pop(RESET_TARGET_KEY, None)
        messages.error(request, "대상 계정을 찾지 못했습니다.")
        return redirect("find_password")

    if request.method == "POST":
        password1 = request.POST.get("password1", "")
        password2 = request.POST.get("password2", "")
        if not password1 or not password2:
            messages.error(request, "새 비밀번호를 입력해 주세요.")
        elif password1 != password2:
            messages.error(request, "비밀번호가 서로 일치하지 않습니다.")
        else:
            user.set_password(password1)
            user.save(update_fields=["password"])
            request.session.pop(RESET_TARGET_KEY, None)
            messages.success(request, "비밀번호가 변경되었습니다. 다시 로그인해 주세요.")
            return redirect("login")

    return render(request, "registration/password_reset_set.html", {"target_username": user.username})


def _normalize_phone(raw: str) -> str:
    return "".join(ch for ch in (raw or "") if ch.isdigit())


def _format_phone(raw: str) -> str:
    digits = _normalize_phone(raw)[:11]
    if not digits:
        return ""
    if len(digits) <= 3:
        return digits
    if len(digits) <= 7:
        return f"{digits[:3]}-{digits[3:]}"
    return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
