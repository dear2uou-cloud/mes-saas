from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import UserCreationForm
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods


@require_http_methods(["GET", "POST"])
def setup_admin(request):
    """초기 1회 관리자(is_staff) 계정을 생성하는 화면.
    이미 관리자 계정이 존재하면 접근을 막습니다.
    """
    User = get_user_model()

    # 이미 관리자가 있으면 더 이상 생성 불가
    if User.objects.filter(is_staff=True).exists():
        return render(
            request,
            "registration/setup_admin_blocked.html",
            status=403,
        )

    form = UserCreationForm(request.POST or None)

    if request.method == "POST":
        if form.is_valid():
            user = form.save(commit=False)
            user.is_staff = True   # 관리자 역할
            user.is_superuser = False
            user.save()
            messages.success(request, "관리자 계정 생성이 완료되었습니다. 로그인해 주세요.")
            return redirect("login")
        else:
            messages.error(request, "입력값을 다시 확인해 주세요.")

    return render(request, "registration/setup_admin.html", {"form": form})
