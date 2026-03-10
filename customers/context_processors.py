from __future__ import annotations

from django.db import connection
from django.db.utils import OperationalError, ProgrammingError


def _has_column(table: str, column: str) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"PRAGMA table_info({table});")
            cols = [row[1] for row in cursor.fetchall()]
        return column in cols
    except Exception:
        return False


def _fmt(n: int) -> str:
    # 템플릿에서 콤마 포맷을 따로 안 쓰므로 여기서 문자열로 만들어줌
    try:
        return f"{int(n):,}"
    except Exception:
        return "0"


def sidebar_counts(request):
    """
    templates/base.html, customer_list.html 이 사용하는 변수들:
    - sb_exam_count, sb_payment_count, sb_nhis_count, sb_followup_count
    - sb_customers_total
    어떤 상황에서도 500을 내지 않도록 방어합니다.
    """
    data = {
        "sb_exam_count": "0",
        "sb_payment_count": "0",
        "sb_nhis_count": "0",
        "sb_followup_count": "0",
        "sb_customers_total": "0",
    }

    try:
        from .models import Customer  # 늦은 import로 앱 로딩 안정화

        table = Customer._meta.db_table  # 보통 customers_customer

        # 테이블이 아직 없으면 그냥 0으로
        if not _has_column(table, "id"):
            return data

        # 기본 queryset
        qs = Customer.objects.all()

        # soft-delete 컬럼이 있으면 정상 필터, 없으면 전체를 활성으로 취급
        if _has_column(table, "is_deleted"):
            active = qs.filter(is_deleted=False)
        else:
            active = qs

        # 총 고객 수(고객관리 상단 뱃지)
        data["sb_customers_total"] = _fmt(active.count())

        # 단계 뱃지(사이드바)
        data["sb_exam_count"] = _fmt(active.filter(stage="검사").count())
        data["sb_payment_count"] = _fmt(active.filter(stage="제품/결제").count())
        data["sb_nhis_count"] = _fmt(active.filter(stage="공단").count())
        data["sb_followup_count"] = _fmt(active.filter(stage="후기적합").count())

        return data

    except (OperationalError, ProgrammingError):
        return data
    except Exception:
        return data


def topbar_notifications(request):
    """상단 종(🔔) 알림 표시용 공통 데이터

    - 재고 알림: DB(Notification) 기반, 읽지 않은 알림이 있으면 빨간 점 표시
    - 방문 알림: DB 저장 없이(동적 계산) 오늘/내일/3일 이내 도래만 표시
    """

    data = {
        "notif_has_unread": False,
        "notif_stock_items": [],
        "notif_visit_items": [],
    }

    try:
        if not getattr(request, "user", None) or not request.user.is_authenticated:
            return data

        from django.utils import timezone
        from datetime import timedelta

        # 1) 재고 알림
        try:
            from .models import Notification

            cutoff = timezone.now() - timedelta(days=30)
            qs = Notification.objects.filter(kind="INVENTORY", created_at__gte=cutoff).order_by("-created_at", "-id")
            data["notif_has_unread"] = qs.filter(is_read=False).exists()
            items = []
            for n in qs[:30]:
                items.append({
                    "id": n.id,
                    "title": (n.title or "").strip(),
                    "message": (n.message or "").strip(),
                    "link": (n.link or "").strip(),
                    "is_read": bool(n.is_read),
                    "created_at": timezone.localtime(n.created_at).strftime("%Y-%m-%d %H:%M"),
                })
            data["notif_stock_items"] = items
        except Exception:
            # 마이그레이션 전/테이블 없음 등 방어
            return data

        # 2) 방문 알림(동적)
        try:
            from .models import Consultation

            today = timezone.localdate()
            end = today + timedelta(days=3)
            start_dt = timezone.make_aware(timezone.datetime.combine(today, timezone.datetime.min.time()))
            end_dt = timezone.make_aware(timezone.datetime.combine(end, timezone.datetime.max.time()))
            qs = (
                Consultation.objects.select_related("customer")
                .filter(customer__is_deleted=False, visit_reservation_at__isnull=False)
                .filter(visit_reservation_at__gte=start_dt, visit_reservation_at__lte=end_dt)
                .exclude(outcome="취소")
                .order_by("visit_reservation_at", "id")
            )
            v_items = []
            for c in qs[:200]:
                dt = timezone.localtime(c.visit_reservation_at)
                d = dt.date()
                label = "3일 이내"
                if d == today:
                    label = "오늘"
                elif d == today + timedelta(days=1):
                    label = "내일"
                v_items.append({
                    "label": label,
                    "date": d.isoformat(),
                    "time": dt.strftime("%H:%M"),
                    "customer_name": c.customer.name,
                    # 캘린더로 이동 + 날짜 하이라이트
                    "link": f"/customers/calendar/?view=month&date={d.isoformat()}&hl_date={d.isoformat()}",
                })
            data["notif_visit_items"] = v_items
        except Exception:
            pass

        return data

    except Exception:
        return data
