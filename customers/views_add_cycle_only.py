from __future__ import annotations

import datetime
from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, redirect
from django.views.decorators.http import require_POST

from .models import Customer, CustomerCase, PaymentItem


def _calc_fu4_end(case: CustomerCase) -> datetime.date | None:
    """후기적합 4차 END DATE 계산
    우선순위:
    1) fu4_end_override
    2) followup_period(4) 계산값(end)
    """
    if case.fu4_end_override:
        return case.fu4_end_override
    _start, end = case.followup_period(4)
    return end


@require_POST
@login_required
def customer_add_cycle(request, pk: int):
    """회차 추가
    - 기존 케이스는 유지(삭제 없음)
    - 새 CustomerCase 생성 후 고객.current_cycle 갱신
    - 트리거: (현재 회차) 후기적합 4차 END DATE + 365일 -> 새 회차 purchase_date 자동 입력(가능한 경우)
    """
    customer = get_object_or_404(Customer, pk=pk, is_deleted=False)

    # 현재 회차 케이스를 기준으로 다음 회차를 생성
    current_case = (
        CustomerCase.objects.filter(customer=customer, cycle_no=customer.current_cycle)
        .order_by("-created_at", "-id")
        .first()
    )
    if current_case is None:
        # 안전장치: 케이스가 없으면 1회차부터 생성
        current_case = CustomerCase.objects.create(customer=customer, cycle_no=1)
        PaymentItem.objects.create(case=current_case, is_base=True)
        if customer.current_cycle != 1:
            customer.current_cycle = 1
            customer.save(update_fields=["current_cycle"])

    max_cycle = (
        CustomerCase.objects.filter(customer=customer)
        .order_by("-cycle_no")
        .values_list("cycle_no", flat=True)
        .first()
    ) or 1
    new_cycle = int(max_cycle) + 1

    next_purchase_date = None
    fu4_end = _calc_fu4_end(current_case)
    if fu4_end:
        next_purchase_date = fu4_end + datetime.timedelta(days=365)

    new_case = CustomerCase.objects.create(
        customer=customer,
        cycle_no=new_cycle,
        purchase_date=next_purchase_date,
    )
    PaymentItem.objects.create(case=new_case, is_base=True)

    customer.current_cycle = new_cycle
    customer.save(update_fields=["current_cycle"])

    # 생성 직후 바로 새 회차 '제품/결제'로 이동
    return redirect(f"/customers/{customer.id}/?tab=제품/결제&case={new_case.id}")
