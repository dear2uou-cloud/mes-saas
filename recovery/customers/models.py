from __future__ import annotations

import datetime
from django.conf import settings
from django.db import models
from django.utils import timezone


class Customer(models.Model):
    TRACK_CHOICES = [
        ("일반", "일반"),
        ("의료", "의료"),
        ("차상위", "차상위"),
        ("직접구매", "직접구매"),
    ]

    STAGE_CHOICES = [
        ("고객등록", "고객등록"),
        ("검사", "검사"),
        ("제품/결제", "제품/결제"),
        ("공단", "공단"),
        ("후기적합", "후기적합"),
        ("완료", "완료"),
    ]

    track = models.CharField("구분", max_length=10, choices=TRACK_CHOICES, default="일반")

    current_cycle = models.IntegerField("현재 회차", default=1)

    name = models.CharField("고객명", max_length=50)
    phone = models.CharField("연락처", max_length=30, blank=True, default="")
    guardian_phone = models.CharField("보호자 연락처", max_length=30, blank=True, default="")
    guardian_phone_2 = models.CharField("보호자 연락처 2", max_length=30, blank=True, default="")
    address_summary = models.CharField("주소", max_length=200, blank=True, default="")
    memo = models.TextField("메모", blank=True, default="")
    담당자 = models.CharField("담당자", max_length=30, blank=True, default="")

    rrn_full = models.CharField("주민등록번호(전체)", max_length=20, blank=True, default="")

    stage = models.CharField("단계", max_length=20, choices=STAGE_CHOICES, default="고객등록")

    # 휴지통(소프트 삭제)
    is_deleted = models.BooleanField("휴지통", default=False)
    deleted_at = models.DateTimeField("삭제일시", null=True, blank=True)
    deleted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="deleted_customers",
        verbose_name="삭제자",
    )

    # 검사 탭
    exam_hospital_name = models.CharField("병원명", max_length=100, blank=True, default="")
    exam_visit_1_date = models.DateField("병원방문 1차", null=True, blank=True)
    exam_visit_2_date = models.DateField("병원방문 2차", null=True, blank=True)
    exam_visit_3_date = models.DateField("병원방문 3차", null=True, blank=True)
    exam_submit_date = models.DateField("제출일", null=True, blank=True)
    exam_retest_date = models.DateField("재검일자", null=True, blank=True)
    exam_retest_hospital = models.CharField("재검병원", max_length=100, blank=True, default="")
    exam_disability_decision_date = models.DateField("장애결정일", null=True, blank=True)


    DISABILITY_LEVEL_CHOICES = [
        ("", "선택"),
        ("심각한 장애", "심각한 장애"),
        ("심각하지 않은 장애", "심각하지 않은 장애"),
    ]
    exam_disability_level = models.CharField(
        "장애도", max_length=30, choices=DISABILITY_LEVEL_CHOICES, blank=True, default=""
    )

    # 검사 탭 메모
    exam_memo = models.TextField(
        "검사 메모",
        blank=True,
        default=""
    )

    created_at = models.DateTimeField(default=timezone.now)

    @property
    def rrn_masked(self) -> str:
        s = (self.rrn_full or "").strip()
        if not s:
            return ""
        digits = "".join([c for c in s if c.isdigit()])
        if len(digits) < 7:
            return s
        front6 = digits[:6]
        back1 = digits[6]
        return f"{front6}-{back1}" + "*" * 6


class Consultation(models.Model):
    """상담/방문/피팅 이력 (고객 단위 타임라인)"""

    OUTCOME_CHOICES = [
        ("진행", "진행"),
        ("보류", "보류"),
        ("취소", "취소"),
    ]

    customer = models.ForeignKey(
        Customer,
        on_delete=models.CASCADE,
        related_name="consultations",
        verbose_name="고객",
    )

    outcome = models.CharField("상담 결과", max_length=10, choices=OUTCOME_CHOICES)
    note = models.TextField("상담 내용", blank=True, default="")

    # 방문 이력(실제 방문한 날짜)
    visit_date = models.DateField("방문일", null=True, blank=True)

    # 방문 예약(향후 캘린더 연동 예정)
    visit_reservation_at = models.DateTimeField("방문 예약", null=True, blank=True)

    fitting_note = models.TextField("피팅/조절", blank=True, default="")

    created_at = models.DateTimeField("작성일시", default=timezone.now)
    updated_at = models.DateTimeField("수정일시", auto_now=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="created_consultations",
        verbose_name="작성자",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="updated_consultations",
        verbose_name="수정자",
    )

    class Meta:
        verbose_name = "상담 기록"
        verbose_name_plural = "상담 기록"
        ordering = ["-created_at", "-id"]

    def save(self, *args, **kwargs):
        # 방문 예약이 있으면 방문일은 비웁니다.
        if self.visit_reservation_at:
            self.visit_date = None
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"Consultation({self.customer_id}, {self.outcome}, {self.created_at})"

def _add_years_safe(d: datetime.date, years: int) -> datetime.date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(month=2, day=28, year=d.year + years)


class ConsultationReservationChangeLog(models.Model):
    """방문예약 변경 이력(캘린더/상담 탭 공통)"""
    consultation = models.ForeignKey("Consultation", on_delete=models.CASCADE, related_name="reservation_logs")
    customer = models.ForeignKey("Customer", on_delete=models.CASCADE, related_name="reservation_logs")
    changed_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)

    old_reservation_at = models.DateTimeField(null=True, blank=True)
    new_reservation_at = models.DateTimeField(null=True, blank=True)
    reason = models.CharField(max_length=120, blank=True, default="")

    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "방문예약 변경 이력"
        verbose_name_plural = "방문예약 변경 이력"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"ReservationLog({self.customer_id}, {self.created_at})"


class AfterService(models.Model):
    """A/S 접수(고객 1:N, 동시 다건 가능)"""

    STATUS_CHOICES = [
        ("IN_PROGRESS", "진행중"),
        ("COMPLETED", "완료"),
        ("CANCELED", "취소"),
    ]

    SIDE_CHOICES = [
        ("LEFT", "좌"),
        ("RIGHT", "우"),
        ("BOTH", "양이"),
    ]

    REASON_CHOICES = [
        ("SOUND", "소리 불만(작다/크다/먹먹함)"),
        ("FEEDBACK", "피드백(삐소리)"),
        ("CUT", "끊김/간헐적 무음"),
        ("RECEIVER", "리시버/튜브 문제"),
        ("BATTERY", "배터리/충전 문제"),
        ("BT", "블루투스/연결 문제"),
        ("LOST", "분실/파손"),
        ("CLEAN", "청소/필터/돔 교체"),
        ("CHECK", "점검 요청(정기 점검)"),
        ("ETC", "기타(직접입력)"),
    ]

    PAYMENT_STATUS_CHOICES = [
        ("UNPAID", "미수"),
        ("PAID", "수납완료"),
    ]

    PAYMENT_METHOD_CHOICES = [
        ("", "선택"),
        ("카드", "카드"),
        ("계좌이체", "계좌이체"),
        ("현금", "현금"),
        ("기타", "기타"),
    ]

    TAX_TYPE_CHOICES = [
        ("", "선택"),
        ("과세", "과세"),
        ("면세", "면세"),
    ]

    customer = models.ForeignKey(
        Customer,
        on_delete=models.CASCADE,
        related_name="after_services",
        verbose_name="고객",
    )

    status = models.CharField("상태", max_length=20, choices=STATUS_CHOICES, default="IN_PROGRESS")
    is_paid = models.BooleanField("유상", default=False)
    target_side = models.CharField("대상", max_length=10, choices=SIDE_CHOICES, default="LEFT")

    owner = models.CharField("담당자", max_length=30, blank=True, default="")

    received_at = models.DateField("접수일", null=False, blank=False)
    completed_at = models.DateField("완료일", null=True, blank=True)
    canceled_at = models.DateField("취소일", null=True, blank=True)

    reason_code = models.CharField("사유", max_length=20, choices=REASON_CHOICES, default="SOUND")
    reason_text = models.CharField("사유 상세", max_length=200, blank=True, default="")
    memo = models.TextField("메모", blank=True, default="")

    amount = models.IntegerField("A/S 비용", default=0)
    payment_status = models.CharField(
        "결제상태",
        max_length=10,
        choices=PAYMENT_STATUS_CHOICES,
        blank=True,
        default="",
    )
    payment_method = models.CharField("수납수단", max_length=20, choices=PAYMENT_METHOD_CHOICES, blank=True, default="")
    tax_type = models.CharField("과세 구분", max_length=10, choices=TAX_TYPE_CHOICES, blank=True, default="")
    paid_at = models.DateField("결제일", null=True, blank=True)
    deposited_at = models.DateField("입금일", null=True, blank=True)

    refund_amount = models.IntegerField("환불금액", default=0)
    refund_at = models.DateField("환불일", null=True, blank=True)

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "A/S"
        verbose_name_plural = "A/S"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"AfterService({self.customer_id}, {self.status}, {self.received_at})"


class AfterServiceEvent(models.Model):
    """A/S 타임라인 이벤트(감사용/우측 로그)"""

    TYPE_CHOICES = [
        ("CREATED", "접수"),
        ("STATUS", "상태"),
        ("AMOUNT", "비용"),
        ("PAYMENT", "결제"),
        ("REFUND", "환불"),
        ("MEMO", "메모"),
        ("OWNER", "담당자"),
    ]

    after_service = models.ForeignKey(
        AfterService,
        on_delete=models.CASCADE,
        related_name="events",
        verbose_name="A/S",
    )
    # NOTE: DB 스키마(0019_after_service)에서는 컬럼명이 'type' 입니다.
    # 기존 DB를 깨지 않기 위해 ORM 필드명은 event_type으로 유지하되 db_column을 매핑합니다.
    event_type = models.CharField(
        "유형",
        max_length=20,
        choices=TYPE_CHOICES,
        db_column="type",
        default="CREATED",
    )
    message = models.CharField("내용", max_length=300, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "A/S 이벤트"
        verbose_name_plural = "A/S 이벤트"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"ASEvent({self.after_service_id}, {self.event_type})"



class CenterEvent(models.Model):
    """센터 공용 일정(휴가/외근/회의)"""

    TYPE_CHOICES = [
        ("휴가", "휴가"),
        ("외근", "외근"),
        ("회의", "회의"),
        ("교육", "교육"),
        ("기타", "기타"),
    ]

    # NOTE:
    # - 화면에서는 '제목' 입력을 기본적으로 숨기고,
    # - 유형이 '기타'일 때만 '기타 제목'으로 입력/노출됩니다.
    # 기존 데이터/템플릿 호환을 위해 title 필드는 유지합니다.
    title = models.CharField("제목(기타)", max_length=80, blank=True, default="")
    event_type = models.CharField("유형", max_length=10, choices=TYPE_CHOICES)

    STATUS_CHOICES = [
        ("ACTIVE", "정상"),
        ("CANCELED", "취소"),
    ]
    status = models.CharField("상태", max_length=12, choices=STATUS_CHOICES, default="ACTIVE")
    canceled_at = models.DateTimeField("취소일시", null=True, blank=True)
    canceled_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="canceled_center_events",
        verbose_name="취소자",
    )
    start_at = models.DateTimeField("시작", null=False, blank=False)
    end_at = models.DateTimeField("종료", null=False, blank=False)
    memo = models.CharField("메모", max_length=120, blank=True, default="")

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="created_center_events",
        verbose_name="등록자",
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "센터 일정"
        verbose_name_plural = "센터 일정"
        ordering = ["-start_at", "-id"]

    def __str__(self) -> str:
        who = ""
        try:
            if self.created_by:
                who = self.created_by.get_username()
        except Exception:
            who = ""
        base = f"{self.event_type}"
        if self.title:
            base = f"{base} {self.title}"
        return f"CenterEvent({base})"


class CenterEventLog(models.Model):
    """센터 일정 변경/취소 로그(삭제 금지 정책)."""

    ACTION_CHOICES = [
        ("CREATE", "생성"),
        ("UPDATE", "수정"),
        ("CANCEL", "취소"),
    ]

    event = models.ForeignKey(CenterEvent, on_delete=models.CASCADE, related_name="logs")
    action = models.CharField(max_length=10, choices=ACTION_CHOICES)
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    # 간단 JSON 스냅샷(필수는 아님) - DB 호환을 위해 TextField 사용
    before_json = models.TextField(blank=True, default="")
    after_json = models.TextField(blank=True, default="")

    class Meta:
        verbose_name = "센터 일정 로그"
        verbose_name_plural = "센터 일정 로그"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"CenterEventLog({self.event_id}, {self.action}, {self.created_at})"


class CustomerCase(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    created_at = models.DateTimeField(default=timezone.now)

    cycle_no = models.IntegerField('회차', default=1)

    has_sub = models.BooleanField(default=False)

    # 메인 제품/결제
    manufacturer = models.CharField("제조사", max_length=50, blank=True, default="")
    model_name = models.CharField("모델명", max_length=50, blank=True, default="")
    serial_number = models.CharField("제조번호", max_length=50, blank=True, default="")
    standard_code = models.CharField("표준코드", max_length=50, blank=True, default="")
    manufacture_date = models.DateField("제조일", null=True, blank=True)
    receiver = models.CharField("리시버", max_length=50, blank=True, default="")
    receiver_manufacturer = models.CharField("리시버 제조사", max_length=50, blank=True, default="")
    receiver_serial_number = models.CharField("리시버 제조번호", max_length=50, blank=True, default="")
    receiver_standard_code = models.CharField("리시버 표준코드", max_length=50, blank=True, default="")
    receiver_manufacture_date = models.DateField("리시버 제조일", null=True, blank=True)
    pre_fit_date = models.DateField("선착용일", null=True, blank=True)
    purchase_date = models.DateField("착용일/구매일", null=True, blank=True)

    SIDE_CHOICES = [("", "선택"), ("좌", "좌"), ("우", "우")]
    side = models.CharField("좌우", max_length=10, choices=SIDE_CHOICES, blank=True, default="")

    earmold_made_date = models.DateField("이어몰드제작일", null=True, blank=True)
    nhis_amount = models.IntegerField("공단_인정금액", null=True, blank=True)
    copay_amount = models.IntegerField("본인부담액", null=True, blank=True)

    # 서브(추가) 제품/결제
    manufacturer_add = models.CharField("제조사(추가)", max_length=50, blank=True, default="")
    model_name_add = models.CharField("모델명(추가)", max_length=50, blank=True, default="")
    serial_number_add = models.CharField("제조번호(추가)", max_length=50, blank=True, default="")
    standard_code_add = models.CharField("표준코드(추가)", max_length=50, blank=True, default="")
    manufacture_date_add = models.DateField("제조일(추가)", null=True, blank=True)
    receiver_add = models.CharField("리시버(추가)", max_length=50, blank=True, default="")
    receiver_manufacturer_add = models.CharField("리시버 제조사(추가)", max_length=50, blank=True, default="")
    receiver_serial_number_add = models.CharField("리시버 제조번호(추가)", max_length=50, blank=True, default="")
    receiver_standard_code_add = models.CharField("리시버 표준코드(추가)", max_length=50, blank=True, default="")
    receiver_manufacture_date_add = models.DateField("리시버 제조일(추가)", null=True, blank=True)
    pre_fit_date_add = models.DateField("선착용일(추가)", null=True, blank=True)
    purchase_date_add = models.DateField("착용일/구매일(추가)", null=True, blank=True)
    side_add = models.CharField("좌우(추가)", max_length=10, choices=SIDE_CHOICES, blank=True, default="")

    earmold_made_date_add = models.DateField("이어몰드제작일(추가)", null=True, blank=True)

    self_pay_amount_add = models.IntegerField("자부담금액(추가)", null=True, blank=True)

    # 공단 탭(케이스 단위)
    nhis_inspection_date = models.DateField("검수", null=True, blank=True)
    nhis_center_name = models.CharField("공단/주민센터명", max_length=100, blank=True, default="")
    nhis_submit_date = models.DateField("공단/주민센터 접수일", null=True, blank=True)

    SUBMIT_METHOD_CHOICES = [
        ("", "선택"),
        ("FAX", "FAX"),
        ("방문제출", "방문제출"),
    ]
    nhis_submit_method = models.CharField("제출방법", max_length=20, choices=SUBMIT_METHOD_CHOICES, blank=True, default="")

    # 공단 보완(SSOT v1.3)
    nhis_supplement_content = models.TextField("보완 내용", blank=True, default="")
    nhis_supplement_written_at = models.DateTimeField("보완 작성일시", null=True, blank=True)
    nhis_supplement_done = models.BooleanField("보완 완료", default=False)
    nhis_supplement_done_at = models.DateTimeField("보완 완료일시", null=True, blank=True)
    nhis_deposit_date = models.DateField("입금일", null=True, blank=True)
    nhis_deposit_amount = models.IntegerField("입금액", null=True, blank=True)

    # 후기적합(1~4차)
    fu1_start_override = models.DateField("후기1 시작일(수동)", null=True, blank=True)
    fu1_end_override = models.DateField("후기1 종료일(수동)", null=True, blank=True)
    fu1_manager = models.CharField("후기1 담당자", max_length=50, blank=True, default="")
    fu1_progress_date = models.DateField("후기1 진행일자", null=True, blank=True)
    fu1_deposit_date = models.DateField("후기1 입금일", null=True, blank=True)
    fu1_deposit_amount = models.IntegerField("후기1 입금액", null=True, blank=True)
    fu1_note = models.TextField("후기1 비고", blank=True, default="")

    fu1_submitted = models.BooleanField("후기1 제출완료", default=False)
    fu1_submitted_at = models.DateTimeField("후기1 제출완료일시", null=True, blank=True)

    fu2_start_override = models.DateField("후기2 시작일(수동)", null=True, blank=True)
    fu2_end_override = models.DateField("후기2 종료일(수동)", null=True, blank=True)
    fu2_manager = models.CharField("후기2 담당자", max_length=50, blank=True, default="")
    fu2_progress_date = models.DateField("후기2 진행일자", null=True, blank=True)
    fu2_deposit_date = models.DateField("후기2 입금일", null=True, blank=True)
    fu2_deposit_amount = models.IntegerField("후기2 입금액", null=True, blank=True)
    fu2_note = models.TextField("후기2 비고", blank=True, default="")

    fu2_submitted = models.BooleanField("후기2 제출완료", default=False)
    fu2_submitted_at = models.DateTimeField("후기2 제출완료일시", null=True, blank=True)

    fu3_start_override = models.DateField("후기3 시작일(수동)", null=True, blank=True)
    fu3_end_override = models.DateField("후기3 종료일(수동)", null=True, blank=True)
    fu3_manager = models.CharField("후기3 담당자", max_length=50, blank=True, default="")
    fu3_progress_date = models.DateField("후기3 진행일자", null=True, blank=True)
    fu3_deposit_date = models.DateField("후기3 입금일", null=True, blank=True)
    fu3_deposit_amount = models.IntegerField("후기3 입금액", null=True, blank=True)
    fu3_note = models.TextField("후기3 비고", blank=True, default="")

    fu3_submitted = models.BooleanField("후기3 제출완료", default=False)
    fu3_submitted_at = models.DateTimeField("후기3 제출완료일시", null=True, blank=True)

    fu4_start_override = models.DateField("후기4 시작일(수동)", null=True, blank=True)
    fu4_end_override = models.DateField("후기4 종료일(수동)", null=True, blank=True)
    fu4_manager = models.CharField("후기4 담당자", max_length=50, blank=True, default="")
    fu4_progress_date = models.DateField("후기4 진행일자", null=True, blank=True)
    fu4_deposit_date = models.DateField("후기4 입금일", null=True, blank=True)
    fu4_deposit_amount = models.IntegerField("후기4 입금액", null=True, blank=True)
    fu4_note = models.TextField("후기4 비고", blank=True, default="")

    fu4_submitted = models.BooleanField("후기4 제출완료", default=False)
    fu4_submitted_at = models.DateTimeField("후기4 제출완료일시", null=True, blank=True)

    
    @property
    def main_total(self) -> int:
        """메인 합계 = 공단 인정금액 + 본인부담액"""
        return int(self.nhis_amount or 0) + int(self.copay_amount or 0)

    @property
    def sub_total(self) -> int:
        """서브(추가) 합계 = (추가 자부담금액)"""
        return int(self.self_pay_amount_add or 0) if self.has_sub else 0

    @property
    def grand_total(self) -> int:
        """총 결제금액 = 메인 합계(공단+본인부담) + 서브(추가) 합계"""
        return self.main_total + self.sub_total

    def followup_period(self, n: int) -> tuple[datetime.date | None, datetime.date | None]:
        """후기적합 n차 기간 계산"""
        so = getattr(self, f"fu{n}_start_override")
        eo = getattr(self, f"fu{n}_end_override")
        if so or eo:
            return (so, eo)

        if not self.purchase_date:
            return (None, None)

        start = _add_years_safe(self.purchase_date, n)
        end = _add_years_safe(self.purchase_date, n + 1) - datetime.timedelta(days=1)
        return (start, end)


    class Meta:
        ordering = ['cycle_no']  # 1회차 -> N회차

class PaymentItem(models.Model):
    """
    결제정보 카드(여러 건)
    - 기본 1개는 항상 존재 (is_base=True)
    - 추가 생성한 카드만 삭제 가능 (is_base=False)
    """
    PAYMENT_METHOD_CHOICES = [
        ("", "선택"),
        ("카드", "카드"),
        ("계좌이체", "계좌이체"),
        ("현금", "현금"),
    ]

    case = models.ForeignKey(CustomerCase, on_delete=models.CASCADE, related_name="payment_items")
    created_at = models.DateTimeField(default=timezone.now)

    is_base = models.BooleanField(default=False)

    payment_method = models.CharField("결제방식", max_length=20, choices=PAYMENT_METHOD_CHOICES, blank=True, default="")
    payment_card_text = models.CharField("결제카드", max_length=50, blank=True, default="")
    memo = models.TextField("메모", blank=True, default="")
    # 미수/연체 관리(제품/결제 탭 전용)
    payment_method_selected_date = models.DateField("결제방식 선택일", null=True, blank=True)
    unpaid_due_date = models.DateField("미수 납부 예정일", null=True, blank=True)
    unpaid_note = models.TextField("미수 메모", blank=True, default="")
    repurchase_yn = models.BooleanField("재구매_여부", default=False)



class PaymentTransaction(models.Model):
    """제품/결제 탭 - 실제 수납(결제) 이력"""

    METHOD_CHOICES = [
        ("", "선택"),
        ("카드", "카드"),
        ("계좌이체", "계좌이체"),
        ("현금", "현금"),
        ("기타", "기타"),
    ]

    TAX_TYPE_CHOICES = [
        ("", "선택"),
        ("과세", "과세"),
        ("면세", "면세"),
    ]

    case = models.ForeignKey(CustomerCase, on_delete=models.CASCADE, related_name="payment_transactions")
    paid_at = models.DateField("수납일", null=False, blank=False)
    amount = models.IntegerField("수납금액", null=False, blank=False)
    method = models.CharField("수납수단", max_length=20, choices=METHOD_CHOICES, blank=True, default="")
    tax_type = models.CharField("과세 구분", max_length=10, choices=TAX_TYPE_CHOICES, blank=True, default="")
    memo = models.TextField("수납 메모", blank=True, default="")
    # 환불 처리(음수 금액)일 때 원거래/사유를 기록합니다.
    origin_tx = models.ForeignKey(
        "self",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="refund_children",
        verbose_name="원거래",
    )
    origin_seq = models.PositiveIntegerField("원거래 번호", default=0)
    refund_reason = models.CharField("환불 사유", max_length=200, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "수납 이력"
        verbose_name_plural = "수납 이력"
        ordering = ["-paid_at", "-id"]

    def __str__(self) -> str:
        return f"PaymentTransaction({self.case_id}, {self.paid_at}, {self.amount})"


class SalesDownloadLog(models.Model):
    """매출자료 다운로드 로그(화면 노출 없음)."""

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    start_date = models.DateField("시작일")
    end_date = models.DateField("종료일")
    gran = models.CharField("집계 단위", max_length=10, default="day")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "매출 다운로드 로그"
        verbose_name_plural = "매출 다운로드 로그"
        ordering = ["-created_at", "-id"]


class RRNAccessLog(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    accessed_at = models.DateTimeField(default=timezone.now)


class RRNEditedLog(models.Model):
    """주민등록번호 수정 로그 (관리자만 가능)"""

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)

    old_rrn = models.CharField("이전 주민등록번호", max_length=20, blank=True, default="")
    new_rrn = models.CharField("변경 주민등록번호", max_length=20, blank=True, default="")

    edited_at = models.DateTimeField(default=timezone.now)


class CustomerTrashLog(models.Model):
    ACTION_CHOICES = [
        ("trash", "휴지통 이동"),
        ("restore", "복구"),
        ("purge", "완전삭제"),
    ]

    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name="trash_logs")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    action = models.CharField("동작", max_length=20, choices=ACTION_CHOICES)
    created_at = models.DateTimeField(default=timezone.now)


class BusinessProfile(models.Model):
    """센터(사업자) 프로필 (로그인 계정 1:1)."""

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="business_profile",
        verbose_name="계정",
    )

    business_name = models.CharField("사업자명", max_length=100, blank=True, default="")
    representative_name = models.CharField("대표자명", max_length=50, blank=True, default="")
    business_reg_no = models.CharField("사업자등록번호", max_length=30, blank=True, default="")
    business_phone = models.CharField("사업자 연락처", max_length=30, blank=True, default="")
    business_address = models.CharField("사업자 주소", max_length=200, blank=True, default="")
    business_type = models.CharField("업태", max_length=50, blank=True, default="")
    business_item = models.CharField("종목", max_length=50, blank=True, default="")
    bank_name = models.CharField("은행", max_length=50, blank=True, default="")
    bank_account = models.CharField("계좌번호", max_length=50, blank=True, default="")

    rep_rrn_full = models.CharField("대표자 주민등록번호(전체)", max_length=20, blank=True, default="")

    # 상담 리마인드 기준(일)
    consultation_reminder_days = models.PositiveIntegerField("상담 리마인드 기준(일)", default=30)

    # 동의(목적 제한) 기록
    consent_agreed = models.BooleanField("사업자 정보 이용 동의", default=False)
    consent_agreed_at = models.DateTimeField("동의일시", null=True, blank=True)
    consent_text = models.TextField("동의 문구", blank=True, default="")

    updated_at = models.DateTimeField("수정일시", auto_now=True)

    class Meta:
        verbose_name = "사업자 프로필"
        verbose_name_plural = "사업자 프로필"

    def __str__(self) -> str:
        return f"BusinessProfile({self.user_id})"

    @property
    def rep_rrn_masked(self) -> str:
        s = (self.rep_rrn_full or "").strip()
        if not s:
            return ""
        # 900101-1234567 / 9001011234567 모두 대응
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) < 7:
            return "***"
        head = digits[:6]
        tail = digits[6:]
        masked_tail = tail[0] + ("*" * max(0, len(tail) - 1))
        return f"{head}-{masked_tail}" if len(digits) >= 7 else "***"


class BusinessProfileConsentLog(models.Model):
    """사업자 정보 이용 동의 이력"""

    profile = models.ForeignKey(BusinessProfile, on_delete=models.CASCADE, related_name="consent_logs")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    consent_text = models.TextField("동의 문구")
    ip = models.CharField("IP", max_length=64, blank=True, default="")
    user_agent = models.CharField("User-Agent", max_length=255, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "사업자 정보 동의 로그"
        verbose_name_plural = "사업자 정보 동의 로그"


class BusinessProfileAccessLog(models.Model):
    """민감정보(주민등록번호) 접근 로그"""

    ACTION_CHOICES = [
        ("RRN_REVEAL", "주민등록번호 전체 보기"),
    ]

    profile = models.ForeignKey(BusinessProfile, on_delete=models.CASCADE, related_name="access_logs")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    action = models.CharField("동작", max_length=20, choices=ACTION_CHOICES)
    ip = models.CharField("IP", max_length=64, blank=True, default="")
    user_agent = models.CharField("User-Agent", max_length=255, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "접근 로그"
        verbose_name_plural = "접근 로그"


# ==============================
# 재고관리(Inventory)
# ==============================


class InventoryManufacturer(models.Model):
    name = models.CharField("제조사", max_length=80, unique=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "재고 제조사"
        verbose_name_plural = "재고 제조사"
        ordering = ["name", "id"]

    def __str__(self) -> str:
        return self.name


class InventoryProductModel(models.Model):
    PRODUCT_KIND_CHOICES = [
        ("HEARING_AID", "보청기"),
        ("DOME", "돔"),
        ("RECEIVER", "리시버"),
    ]

    ITEM_TYPE_CHOICES = [
        ("SERIAL", "시리얼형"),
        ("QTY", "수량형"),
    ]

    manufacturer = models.ForeignKey(InventoryManufacturer, on_delete=models.CASCADE, related_name="product_models")
    importer_name = models.CharField("제조수입업소명", max_length=120, blank=True, default="")
    model_name = models.CharField("모델명", max_length=120)
    product_kind = models.CharField("품목구분", max_length=20, choices=PRODUCT_KIND_CHOICES, default="HEARING_AID")
    item_type = models.CharField("유형", max_length=10, choices=ITEM_TYPE_CHOICES, default="SERIAL")
    qty_current = models.IntegerField("현재 수량", default=0)

    # ✅ 소프트 삭제(오등록 등) - 삭제 시 목록/선택에서 제외
    is_deleted = models.BooleanField("삭제됨", default=False)
    deleted_at = models.DateTimeField("삭제일", null=True, blank=True)
    deleted_reason = models.CharField("삭제 사유", max_length=255, blank=True)

    # 안전재고(임계치) - 0이면 알림 비활성
    alert_threshold = models.IntegerField("안전재고", default=0)
    # 알림 중복 방지 플래그(상태 회복 시 초기화)
    threshold_alerted = models.BooleanField("안전재고 알림 발송됨", default=False)
    negative_alerted = models.BooleanField("음수재고 알림 발송됨", default=False)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "재고 모델"
        verbose_name_plural = "재고 모델"
        ordering = ["manufacturer__name", "model_name", "id"]
        constraints = [
            models.UniqueConstraint(fields=["manufacturer", "model_name"], name="uniq_inventory_model_by_mfr")
        ]

    def __str__(self) -> str:
        return f"{self.manufacturer.name} {self.model_name}"


class InventoryUnit(models.Model):
    """시리얼형(보청기) 개별 재고"""

    STATUS_CHOICES = [
        ("IN_STOCK", "입고"),
        ("SHIPPED", "출고"),
    ]

    product_model = models.ForeignKey(InventoryProductModel, on_delete=models.CASCADE, related_name="units")
    serial_no = models.CharField("제조번호", max_length=80, unique=True)
    standard_code = models.CharField("표준코드", max_length=80, blank=True, default="")
    mfg_date = models.DateField("제조일", null=True, blank=True)
    status = models.CharField("상태", max_length=20, choices=STATUS_CHOICES, default="IN_STOCK")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "재고(제조번호)"
        verbose_name_plural = "재고(제조번호)"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"{self.serial_no}"


class InventoryStockEvent(models.Model):
    EVENT_CHOICES = [
        ("RECEIVE", "입고"),
        ("SHIP", "출고"),
        ("ADJUST", "조정"),
    ]

    PROGRESS_CHOICES = [
        ("IN_PROGRESS", "진행중"),
        ("DONE", "완료"),
        ("CANCELED", "취소"),
    ]

    ADJUST_KIND_CHOICES = [
        ("DATA_CORRECTION", "데이터 수정"),
        ("LOST", "분실 처리"),
        ("DISCARD", "폐기 처리"),
    ]

    # SERIAL: unit 사용 / QTY: product_model + qty_delta 사용
    unit = models.ForeignKey(InventoryUnit, on_delete=models.SET_NULL, null=True, blank=True, related_name="events")
    product_model = models.ForeignKey(
        InventoryProductModel, on_delete=models.SET_NULL, null=True, blank=True, related_name="events"
    )
    qty_delta = models.IntegerField("수량 변화", default=0)

    event_type = models.CharField("유형", max_length=12, choices=EVENT_CHOICES)
    progress_status = models.CharField("진행상태", max_length=12, choices=PROGRESS_CHOICES, default="IN_PROGRESS")
    adjust_kind = models.CharField("구분", max_length=20, choices=ADJUST_KIND_CHOICES, blank=True, default="")
    reason = models.CharField("사유", max_length=200, blank=True, default="")

    # 조정 전/후 스냅샷(JSON 문자열)
    before_json = models.TextField(blank=True, default="")
    after_json = models.TextField(blank=True, default="")

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="inventory_events",
        verbose_name="담당자",
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "재고 이벤트"
        verbose_name_plural = "재고 이벤트"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"InvEvent({self.event_type}, unit={self.unit_id}, model={self.product_model_id}, {self.progress_status})"


class Notification(models.Model):
    """상단 알림(재고)

    - 방문 알림은 DB 저장하지 않고(동적 계산) UI에서만 표시합니다.
    - 재고 알림만 저장/읽음 처리/빨간 점 표시를 합니다.
    """

    KIND_CHOICES = [
        ("INVENTORY", "재고"),
    ]

    kind = models.CharField("종류", max_length=20, choices=KIND_CHOICES, default="INVENTORY")
    title = models.CharField("제목", max_length=120, blank=True, default="")
    message = models.TextField("내용", blank=True, default="")
    link = models.CharField("링크", max_length=255, blank=True, default="")
    is_read = models.BooleanField("읽음", default=False)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "알림"
        verbose_name_plural = "알림"
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"Notification({self.kind}, read={self.is_read})"



# ==========================
# 서류 다운로드 로그
# ==========================
class DocumentDownloadLog(models.Model):
    """서류 출력/다운로드 이력(일반-공단 / 일반-후기)."""

    DOCUMENT_TYPE_CHOICES = [
        ("GENERAL_PUBLIC", "일반-공단"),
        ("GENERAL_AFTERCARE", "일반-후기"),
    ]
    STATUS_CHOICES = [
        ("START", "START"),
        ("SUCCESS", "SUCCESS"),
        ("FAIL", "FAIL"),
    ]

    case = models.ForeignKey("CustomerCase", on_delete=models.CASCADE, related_name="document_download_logs")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    document_type = models.CharField(max_length=30, choices=DOCUMENT_TYPE_CHOICES)
    round_no = models.IntegerField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="START")
    error_message = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.document_type} - {self.case_id} - {self.created_at}"
