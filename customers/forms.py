from django import forms
from django.forms import ModelForm

from .models import Customer, CustomerCase, PaymentItem, PaymentTransaction, BusinessProfile
from .models import Consultation


def _digits_only(value):
    return ''.join(ch for ch in str(value or '') if ch.isdigit())


def _format_mobile_phone(value):
    digits = _digits_only(value)[:11]
    if not digits:
        return ''
    if len(digits) != 11:
        raise forms.ValidationError('연락처는 11자리 숫자로 입력해 주세요.')
    return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"


def _format_rrn(value):
    digits = _digits_only(value)[:13]
    if not digits:
        return ''
    if len(digits) != 13:
        raise forms.ValidationError('주민등록번호 13자리를 입력해 주세요.')
    return f"{digits[:6]}-{digits[6:]}"


def _format_business_reg_no(value):
    digits = _digits_only(value)[:10]
    if not digits:
        return ''
    if len(digits) != 10:
        raise forms.ValidationError('사업자등록번호 10자리를 입력해 주세요.')
    return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"


def _apply_input_class(fields):
    for f in fields.values():
        if isinstance(f.widget, (forms.TextInput, forms.DateInput, forms.NumberInput)):
            f.widget.attrs.setdefault("class", "input")
        if isinstance(f.widget, forms.Textarea):
            f.widget.attrs.setdefault("class", "input")
        if isinstance(f.widget, forms.Select):
            f.widget.attrs.setdefault("class", "input")
        if isinstance(f.widget, forms.CheckboxInput):
            f.widget.attrs.setdefault("style", "transform:scale(1.1);")


class CustomerCreateForm(ModelForm):
    class Meta:
        model = Customer
        fields = ["name", "phone", "rrn_full", "address_summary", "guardian_phone", "memo", "담당자"]
        widgets = {
            "name": forms.TextInput(attrs={"class": "input"}),
            "phone": forms.TextInput(attrs={"class": "input", "placeholder": "예: 010-1234-5678", "maxlength": "13", "inputmode": "numeric", "autocomplete": "off"}),
            "rrn_full": forms.TextInput(attrs={"class": "input", "placeholder": "예: 900101-1234567", "maxlength": "14", "inputmode": "numeric", "autocomplete": "off"}),
            "address_summary": forms.TextInput(attrs={"class": "input"}),
            "guardian_phone": forms.TextInput(attrs={"class": "input"}),
            "memo": forms.Textarea(attrs={"class": "input", "rows": 3}),
            "담당자": forms.TextInput(attrs={"class": "input"}),
            "track": forms.Select(attrs={"class": "input"}),
        }

    # ✅ 신규 등록(+고객 등록) 화면 전용 필수: 고객명/연락처/주소
    #    (브라우저 기본 툴팁(required) 사용)
    def __init__(self, *args, **kwargs):
        # views.py passes user=request.user for future permission/validation hooks
        kwargs.pop("user", None)
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # 신규 등록 시에는 3개만 필수로 강제
        required_keys = {"name", "phone", "address_summary"}
        for key, field in self.fields.items():
            if key in required_keys:
                field.required = True
                field.widget.attrs["required"] = "required"
                field.error_messages.setdefault("required", "필수 입력 항목입니다.")
            else:
                # 나머지는 필수 아님 (HTML5 required 제거)
                field.required = False
                field.widget.attrs.pop("required", None)

    def clean_phone(self):
        return _format_mobile_phone(self.cleaned_data.get("phone"))

    def clean_rrn_full(self):
        return _format_rrn(self.cleaned_data.get("rrn_full"))


class CustomerInfoInlineForm(ModelForm):
    class Meta:
        model = Customer
        fields = ["name", "phone", "guardian_phone", "guardian_phone_2", "address_summary", "memo", "담당자", "track", "stage"]
        widgets = {
            "name": forms.TextInput(attrs={"class": "input"}),
            "phone": forms.TextInput(attrs={"class": "input"}),
            "guardian_phone": forms.TextInput(attrs={"class": "input"}),
            "guardian_phone_2": forms.TextInput(attrs={"class": "input"}),
            "address_summary": forms.TextInput(attrs={"class": "input"}),
            "memo": forms.Textarea(attrs={"class": "input", "rows": 4}),
            "담당자": forms.TextInput(attrs={"class": "input"}),
        }

    # ✅ 필수 항목: 고객명/연락처/주소/담당자
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        for key in ["name", "phone", "address_summary", "담당자", "track"]:
            if key in self.fields:
                self.fields[key].required = True
                self.fields[key].error_messages.setdefault("required", "필수 입력 항목입니다.")


class RRNEditForm(ModelForm):
    class Meta:
        model = Customer
        fields = ["rrn_full"]
        widgets = {
            "rrn_full": forms.TextInput(attrs={"class": "input", "placeholder": "예: 900101-1234567"}),
        }


class CustomerExamForm(ModelForm):
    class Meta:
        model = Customer
        fields = [
            "exam_hospital_name",
            "exam_visit_1_date",
            "exam_visit_2_date",
            "exam_visit_3_date",
            "exam_submit_date",
            "exam_retest_date",
            "exam_retest_hospital",
            "exam_disability_decision_date",
            "exam_disability_level",
            "exam_memo",
        ]
        widgets = {
            "exam_hospital_name": forms.TextInput(attrs={"class": "input"}),
            "exam_visit_1_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "exam_visit_2_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "exam_visit_3_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "exam_submit_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "exam_retest_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "exam_retest_hospital": forms.TextInput(attrs={"class": "input"}),
            "exam_disability_decision_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "exam_disability_level": forms.Select(attrs={"class": "input"}),
            "exam_memo": forms.Textarea(attrs={"class": "input", "rows": 4}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # ✅ 필수: 장애도 (HTML5 required + 서버 검증)
        if "exam_disability_level" in self.fields:
            self.fields["exam_disability_level"].required = True
            self.fields["exam_disability_level"].error_messages.setdefault("required", "장애도는 필수 항목입니다.")

    # ✅ A-2: 검사 탭 - 장애도 필수
    def clean_exam_disability_level(self):
        v = self.cleaned_data.get("exam_disability_level")
        if v in (None, ""):
            raise forms.ValidationError("장애도는 필수 항목입니다.")
        return v


class CaseProductPaymentForm(ModelForm):
    # NOTE: These are explicitly declared fields (not model fields).
    # Therefore Meta.widgets does NOT apply to them. We set widget attrs here
    # so the global comma formatter can bind to .money-input.
    nhis_amount = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={"inputmode": "numeric", "class": "input money-input", "placeholder": "0"}),
    )
    copay_amount = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={"inputmode": "numeric", "class": "input money-input", "placeholder": "0"}),
    )
    self_pay_amount_add = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={"inputmode": "numeric", "class": "input money-input", "placeholder": "0"}),
    )

    class Meta:
        model = CustomerCase
        fields = [
            "manufacturer",
            "model_name",
            "serial_number",
            "standard_code",
            "manufacture_date",
            "receiver",
            "receiver_manufacturer",
            "receiver_serial_number",
            "receiver_standard_code",
            "receiver_manufacture_date",
            "pre_fit_date",
            "purchase_date",
            "side",
            "earmold_made_date",
            "nhis_amount",
            "copay_amount",
            "manufacturer_add",
            "model_name_add",
            "serial_number_add",
            "standard_code_add",
            "manufacture_date_add",
            "receiver_add",
            "receiver_manufacturer_add",
            "receiver_serial_number_add",
            "receiver_standard_code_add",
            "receiver_manufacture_date_add",
            "pre_fit_date_add",
            "purchase_date_add",
            "side_add",
            "earmold_made_date_add",
            "self_pay_amount_add",
        ]
        widgets = {
            "manufacture_date": forms.DateInput(attrs={"type": "date"}),
            "pre_fit_date": forms.DateInput(attrs={"type": "date"}),
            "purchase_date": forms.DateInput(attrs={"type": "date"}),
            "earmold_made_date": forms.DateInput(attrs={"type": "date"}),
            "manufacture_date_add": forms.DateInput(attrs={"type": "date"}),
            "pre_fit_date_add": forms.DateInput(attrs={"type": "date"}),
            "purchase_date_add": forms.DateInput(attrs={"type": "date"}),
            "earmold_made_date_add": forms.DateInput(attrs={"type": "date"}),
            "side": forms.Select(),
            "side_add": forms.Select(),
        }

    def __init__(self, *args, **kwargs):
        customer = kwargs.pop("customer", None)
        super().__init__(*args, **kwargs)
        self._customer = customer
        _apply_input_class(self.fields)

        # 라벨
        if "nhis_amount" in self.fields:
            self.fields["nhis_amount"].label = "공단 인정금액"

        # 트랙별 공단 인정금액 자동값(항상 고정)
        track = (getattr(customer, "track", "") or "").strip() if customer else ""
        fixed = None
        if track == "일반":
            fixed = 999_000
        elif track in ("의료", "차상위"):
            fixed = 1_110_000
        elif track == "직접구매":
            fixed = 0

        if fixed is not None and "nhis_amount" in self.fields:
            self.initial["nhis_amount"] = fixed
            # UI에서는 수정 불가로 고정(자동 기입)
            self.fields["nhis_amount"].widget.attrs["readonly"] = "readonly"
            self.fields["nhis_amount"].widget.attrs["inputmode"] = "numeric"

        # ✅ 필수(HTML5 required): 제품/결제(메인)
        # 리시버 상세(제조번호/표준코드/제조일)는 선택 UI 흐름상 공란일 수 있으므로
        # 서버/HTML5 required 를 강제하지 않습니다.
        required_main = [
            "manufacturer",
            "model_name",
            "serial_number",
            "standard_code",
            "manufacture_date",
            "receiver",
            "receiver_manufacturer",
            "purchase_date",
            "side",
            "copay_amount",
        ]
        for key in required_main:
            if key in self.fields:
                self.fields[key].required = True
                self.fields[key].widget.attrs["required"] = "required"
                self.fields[key].error_messages.setdefault("required", "필수 입력 항목입니다.")

        # ✅ 보청기 추가(서브) 사용 시에만 required
        case = getattr(self, "instance", None)
        has_sub = bool(getattr(case, "has_sub", False))
        if has_sub:
            # 서브도 동일 원칙: 리시버 상세(제조번호/표준코드/제조일)는 필수 강제하지 않음
            required_sub = [
                "manufacturer_add",
                "model_name_add",
                "serial_number_add",
                "standard_code_add",
                "manufacture_date_add",
                "receiver_add",
                "receiver_manufacturer_add",
                "purchase_date_add",
                "side_add",
                "self_pay_amount_add",
            ]
            for key in required_sub:
                if key in self.fields:
                    self.fields[key].required = True
                    self.fields[key].widget.attrs["required"] = "required"
                    self.fields[key].error_messages.setdefault("required", "필수 입력 항목입니다.")

    def _clean_money_int(self, field_name: str):
        v = self.cleaned_data.get(field_name)
        if v in (None, ""):
            return None
        if isinstance(v, int):
            return v
        s = str(v).strip()
        if s == "":
            return None
        s = s.replace(",", "")
        try:
            return int(s)
        except Exception:
            raise forms.ValidationError("숫자만 입력해주세요.")

    def clean_nhis_amount(self):
        # 트랙별 고정(항상 자동 기입)
        track = (getattr(getattr(self, "_customer", None), "track", "") or "").strip()
        if track == "일반":
            return 999_000
        if track in ("의료", "차상위"):
            return 1_110_000
        if track == "직접구매":
            return 0
        return self._clean_money_int("nhis_amount")

    def clean_copay_amount(self):
        return self._clean_money_int("copay_amount")

    def clean_self_pay_amount_add(self):
        return self._clean_money_int("self_pay_amount_add")

    # ==========================
        # 재고(제조번호) 검증
        # ==========================
        def _validate_inventory_serial(self, serial: str, field_name: str):
            serial = (serial or "").strip()
            if not serial:
                return serial
            try:
                from .models import InventoryUnit
            except Exception:
                return serial

            u = InventoryUnit.objects.filter(serial_no=serial).first()
            if u is None:
                self.add_error(field_name, "재고에 없는 제조번호입니다.")
                return serial
            if (u.status or "").strip() == "SHIPPED":
                self.add_error(field_name, "이미 판매 된 보청기 입니다.")
                return serial
            return serial

        # ✅ A-3: 제품/결제 탭 - 2회차 이상(정식 회차) 착용일/구매일 필수
        def clean(self):
            cleaned = super().clean()

            # ✅ 필수 항목(메인)
            required_main = [
                ("manufacturer", "제조사"),
                ("model_name", "모델명"),
                ("serial_number", "제조번호"),
                ("standard_code", "표준코드"),
                ("manufacture_date", "제조일"),
                ("receiver", "리시버"),
                ("purchase_date", "착용일/구매일"),
                ("side", "좌우"),
            ]

            for key, label in required_main:
                v = cleaned.get(key)
                if v in (None, ""):
                    self.add_error(key, "필수 입력 항목입니다.")

            # 본인부담액(=copay_amount)
            if cleaned.get("copay_amount") in (None, ""):
                self.add_error("copay_amount", "필수 입력 항목입니다.")

            # ✅ 보청기 추가(서브) 사용 시 필수
            case = getattr(self, "instance", None)
            has_sub = bool(getattr(case, "has_sub", False))
            if has_sub:
                required_sub = [
                    ("manufacturer_add", "제조사"),
                    ("model_name_add", "모델명"),
                    ("serial_number_add", "제조번호"),
                    ("standard_code_add", "표준코드"),
                    ("manufacture_date_add", "제조일"),
                    ("receiver_add", "리시버"),
                    ("purchase_date_add", "착용일/구매일"),
                    ("side_add", "좌우"),
                ]
                for key, _label in required_sub:
                    v = cleaned.get(key)
                    if v in (None, ""):
                        self.add_error(key, "필수 입력 항목입니다.")

                if cleaned.get("self_pay_amount_add") in (None, ""):
                    self.add_error("self_pay_amount_add", "필수 입력 항목입니다.")

            customer = getattr(self, "_customer", None)
            track = (getattr(customer, "track", "") or "").strip()


            # ✅ 재고 검증: 직접구매는 제외
            if track != "직접구매":
                self._validate_inventory_serial(cleaned.get("serial_number"), "serial_number")
                if has_sub:
                    self._validate_inventory_serial(cleaned.get("serial_number_add"), "serial_number_add")

            # 직접구매 이력은 "회차" 개념이 아니라서 필수 강제를 걸지 않습니다.
            if track == "직접구매":
                return cleaned

            cycle_no = getattr(case, "cycle_no", 1) or 1

            if int(cycle_no) >= 2:
                if not cleaned.get("purchase_date"):
                    self.add_error("purchase_date", "2회차 이상에서는 착용일/구매일이 필수입니다.")

                if getattr(case, "has_sub", False):
                    if not cleaned.get("purchase_date_add"):
                        self.add_error("purchase_date_add", "2회차 이상에서는 추가 보청기 착용일이 필수입니다.")

            return cleaned

class PaymentItemForm(ModelForm):
    class Meta:
        model = PaymentItem
        fields = ["payment_method", "payment_card_text", "memo", "unpaid_due_date", "unpaid_note"]
        widgets = {
            "payment_method": forms.Select(attrs={"class": "input"}),
            "payment_card_text": forms.TextInput(attrs={"class": "input"}),
            "unpaid_due_date": forms.DateInput(attrs={"type": "date"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # ✅ 필수: 결제방식
        if "payment_method" in self.fields:
            self.fields["payment_method"].required = True
            self.fields["payment_method"].widget.attrs["required"] = "required"
            self.fields["payment_method"].error_messages.setdefault("required", "필수 입력 항목입니다.")


class PaymentTransactionForm(ModelForm):
    class Meta:
        model = PaymentTransaction
        fields = ["paid_at", "amount", "method", "tax_type", "memo"]
        widgets = {
            "paid_at": forms.DateInput(attrs={"type": "date", "class": "input"}),
            "amount": forms.TextInput(attrs={"inputmode": "numeric", "class": "input money-input", "placeholder": "0"}),
            "method": forms.HiddenInput(attrs={"id": "mshPayMethodHidden"}),
            "tax_type": forms.Select(attrs={"class": "input"}),
            # 메모는 한 줄 입력으로 유지(정렬/높이 통일)
            "memo": forms.TextInput(attrs={"class": "input", "placeholder": ""}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # ✅ 과세 구분: UI 미노출 / '과세'로 고정
        if "tax_type" in self.fields:
            self.fields["tax_type"].required = False
            self.fields["tax_type"].initial = "과세"
            self.fields["tax_type"].widget = forms.HiddenInput()
            # HiddenInput에도 값이 들어가도록 보장
            self.fields["tax_type"].widget.attrs["value"] = "과세"

        for key in ["paid_at", "amount", "method"]:
            if key in self.fields:
                self.fields[key].required = True
        # 라벨(UI용)
        if "paid_at" in self.fields: self.fields["paid_at"].label = "결제일"
        if "amount" in self.fields: self.fields["amount"].label = "결제금액"
        if "method" in self.fields: self.fields["method"].label = "결제 방식"
        if "tax_type" in self.fields: self.fields["tax_type"].label = "과세 구분"
        # 필수 미입력 강조(프론트 JS에서 data-req=1을 사용)
        for k in ["paid_at", "amount", "method"]:
            if k in self.fields:
                attrs = self.fields[k].widget.attrs
                attrs["data-req"] = "1"
                attrs["required"] = "required"
                self.fields[k].error_messages.setdefault("required", "필수 입력 항목입니다.")

        # tax_type은 숨김 처리되어도 저장 시 항상 과세가 들어가도록
        if "tax_type" in self.fields:
            self.fields["tax_type"].error_messages.setdefault("required", "필수 입력 항목입니다.")

    def clean_method(self):
        v = self.cleaned_data.get("method")
        if v in (None, ""):
            raise forms.ValidationError("필수 입력 항목입니다.")
        # allow up to 2 selections joined by "+"
        parts = [p.strip() for p in str(v).split("+") if p.strip()]
        allowed = {"카드", "계좌이체", "현금"}
        parts2 = []
        for p in parts:
            if p not in allowed:
                raise forms.ValidationError("결제 방식이 올바르지 않습니다.")
            if p not in parts2:
                parts2.append(p)
        if len(parts2) == 0:
            raise forms.ValidationError("필수 입력 항목입니다.")
        if len(parts2) > 2:
            raise forms.ValidationError("결제 방식은 최대 2개까지 선택할 수 있습니다.")
        return "+".join(parts2)


    def clean_amount(self):
        v = self.cleaned_data.get("amount")
        if v in (None, ""):
            return None
        if isinstance(v, str):
            v = v.replace(",", "").strip()
            if v == "":
                return None
            try:
                v = int(v)
            except Exception:
                raise forms.ValidationError("숫자만 입력해주세요.")
        try:
            v = int(v)
        except Exception:
            raise forms.ValidationError("숫자만 입력해주세요.")
        if v <= 0:
            raise forms.ValidationError("1원 이상의 금액을 입력해주세요.")
        return v


class CaseNhisForm(ModelForm):
    class Meta:
        model = CustomerCase
        fields = [
            "nhis_inspection_date",
            "nhis_center_name",
            "nhis_submit_date",
            "nhis_submit_method",
            "nhis_supplement_content",
            "nhis_supplement_done",
            "nhis_supplement_done_at",
            "nhis_deposit_date",
            "nhis_deposit_amount",
        ]
        widgets = {
            "nhis_inspection_date": forms.DateInput(attrs={"type": "date"}),
            "nhis_submit_date": forms.DateInput(attrs={"type": "date"}),
            "nhis_supplement_done_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "nhis_deposit_date": forms.DateInput(attrs={"type": "date"}),
            "nhis_deposit_amount": forms.TextInput(attrs={"inputmode":"numeric","class":"input money-input","placeholder":"0"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # ✅ datetime-local 표시/재표시 포맷 고정 (저장 후 값이 '빈칸'처럼 보이는 문제 방지)
        if "nhis_supplement_done_at" in self.fields:
            try:
                self.fields["nhis_supplement_done_at"].input_formats = [
                    "%Y-%m-%dT%H:%M",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                ]
                self.fields["nhis_supplement_done_at"].widget.format = "%Y-%m-%dT%H:%M"
            except Exception:
                pass

        # ✅ 필수: 검수 (입금일/입금액은 선택)
        for key in ["nhis_inspection_date"]:
            if key in self.fields:
                self.fields[key].required = True
                self.fields[key].widget.attrs["required"] = "required"
                self.fields[key].error_messages.setdefault("required", "필수 입력 항목입니다.")

    def clean_nhis_deposit_amount(self):
        v = self.cleaned_data.get("nhis_deposit_amount")
        if v in (None, ""):
            return None
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        try:
            return int(v)
        except Exception:
            return None

    def clean(self):
        cleaned = super().clean()

        # ✅ 필수: 검수
        if cleaned.get("nhis_inspection_date") in (None, ""):
            self.add_error("nhis_inspection_date", "필수 입력 항목입니다.")

        return cleaned


class CaseFollowupForm(ModelForm):
    class Meta:
        model = CustomerCase
        fields = [
            "fu1_start_override", "fu1_end_override", "fu1_manager", "fu1_progress_date", "fu1_submitted", "fu1_submitted_at", "fu1_deposit_date", "fu1_deposit_amount", "fu1_note",
            "fu2_start_override", "fu2_end_override", "fu2_manager", "fu2_progress_date", "fu2_submitted", "fu2_submitted_at", "fu2_deposit_date", "fu2_deposit_amount", "fu2_note",
            "fu3_start_override", "fu3_end_override", "fu3_manager", "fu3_progress_date", "fu3_submitted", "fu3_submitted_at", "fu3_deposit_date", "fu3_deposit_amount", "fu3_note",
            "fu4_start_override", "fu4_end_override", "fu4_manager", "fu4_progress_date", "fu4_submitted", "fu4_submitted_at", "fu4_deposit_date", "fu4_deposit_amount", "fu4_note",
        ]
        widgets = {
            "fu1_start_override": forms.DateInput(attrs={"type": "date"}),
            "fu1_end_override": forms.DateInput(attrs={"type": "date"}),
            "fu1_manager": forms.TextInput(),
            "fu1_progress_date": forms.DateInput(attrs={"type": "date"}),
            "fu1_submitted": forms.CheckboxInput(),
            "fu1_submitted_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "fu2_start_override": forms.DateInput(attrs={"type": "date"}),
            "fu2_end_override": forms.DateInput(attrs={"type": "date"}),
            "fu2_manager": forms.TextInput(),
            "fu2_progress_date": forms.DateInput(attrs={"type": "date"}),
            "fu2_submitted": forms.CheckboxInput(),
            "fu2_submitted_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "fu3_start_override": forms.DateInput(attrs={"type": "date"}),
            "fu3_end_override": forms.DateInput(attrs={"type": "date"}),
            "fu3_manager": forms.TextInput(),
            "fu3_progress_date": forms.DateInput(attrs={"type": "date"}),
            "fu3_submitted": forms.CheckboxInput(),
            "fu3_submitted_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "fu4_start_override": forms.DateInput(attrs={"type": "date"}),
            "fu4_end_override": forms.DateInput(attrs={"type": "date"}),
            "fu4_manager": forms.TextInput(),
            "fu4_progress_date": forms.DateInput(attrs={"type": "date"}),
            "fu4_submitted": forms.CheckboxInput(),
            "fu4_submitted_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),

            "fu1_deposit_date": forms.DateInput(attrs={"type": "date"}),
            "fu2_deposit_date": forms.DateInput(attrs={"type": "date"}),
            "fu3_deposit_date": forms.DateInput(attrs={"type": "date"}),
            "fu4_deposit_date": forms.DateInput(attrs={"type": "date"}),

            # ✅ 콤마 표기를 위해 number input 대신 text input 사용
            # (number input은 브라우저가 천단위 콤마 표시를 하지 않습니다)
            "fu1_deposit_amount": forms.TextInput(),
            "fu2_deposit_amount": forms.TextInput(),
            "fu3_deposit_amount": forms.TextInput(),
            "fu4_deposit_amount": forms.TextInput(),

            "fu1_note": forms.Textarea(attrs={"rows": 2}),
            "fu2_note": forms.Textarea(attrs={"rows": 2}),
            "fu3_note": forms.Textarea(attrs={"rows": 2}),
            "fu4_note": forms.Textarea(attrs={"rows": 2}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # ✅ 후기적합 입금액 규칙
        # - 기본은 "빈값" (항상 표시/강제 저장 X)
        # - 입금일이 입력되면 입금액을 50,000원으로 자동 세팅(표시/저장)
        # - 입금액은 사용자가 직접 수정하지 않도록 readonly 유지
        for n in (1, 2, 3, 4):
            key = f"fu{n}_deposit_amount"
            if key not in self.fields:
                continue

            self.fields[key].required = False
            self.fields[key].disabled = False  # POST로 넘어오도록
            self.fields[key].widget.attrs.pop("readonly", None)
            self.fields[key].widget.attrs["readonly"] = "readonly"
            self.fields[key].widget.attrs.setdefault("inputmode", "numeric")

            # 기존 값이 있으면 콤마 포맷으로만 표시
            try:
                inst_val = getattr(self.instance, key, None)
            except Exception:
                inst_val = None
            if inst_val not in (None, ""):
                try:
                    self.initial[key] = f"{int(inst_val):,}"
                except Exception:
                    pass

    def clean(self):
        cleaned = super().clean()

        def _parse_money(v):
            if v in (None, ""):
                return None
            if isinstance(v, int):
                return v
            s = str(v).strip().replace(",", "")
            if not s:
                return None
            try:
                return int(s)
            except Exception:
                return None

        # ✅ 필수/순차 강제 모두 제거
        # ✅ 입금일 입력 시 입금액 자동 세팅(50,000)
        for n in (1, 2, 3, 4):
            dep_date = cleaned.get(f"fu{n}_deposit_date")
            amt_key = f"fu{n}_deposit_amount"
            amt = _parse_money(cleaned.get(amt_key))

            if dep_date not in (None, ""):
                cleaned[amt_key] = 50000 if amt in (None, "") else amt
            else:
                cleaned[amt_key] = None

        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        # clean() 결과를 그대로 저장 (강제 덮어쓰기 없음)
        if commit:
            obj.save()
        return obj


class BusinessProfileForm(ModelForm):
    class Meta:
        model = BusinessProfile
        fields = [
            "business_name",
            "representative_name",
            "business_reg_no",
            "business_phone",
            "business_address",
            "business_type",
            "business_item",
            "bank_name",
            "bank_account",
            "rep_rrn_full",
            "consultation_reminder_days",
        ]
        widgets = {
            "business_name": forms.TextInput(attrs={"class": "input"}),
            "representative_name": forms.TextInput(attrs={"class": "input"}),
            "business_reg_no": forms.TextInput(attrs={"class": "input", "placeholder": "예: 123-45-67890", "maxlength": "12", "inputmode": "numeric", "autocomplete": "off"}),
            "business_phone": forms.TextInput(attrs={"class": "input", "placeholder": "예: 010-1234-5678", "maxlength": "13", "inputmode": "numeric", "autocomplete": "off"}),
            "business_address": forms.TextInput(attrs={"class": "input"}),
            "business_type": forms.TextInput(attrs={"class": "input"}),
            "business_item": forms.TextInput(attrs={"class": "input"}),
            "bank_name": forms.TextInput(attrs={"class": "input"}),
            "bank_account": forms.TextInput(attrs={"class": "input"}),
            "rep_rrn_full": forms.TextInput(attrs={"class": "input", "placeholder": "예: 900101-1234567", "maxlength": "14"}),
            "consultation_reminder_days": forms.NumberInput(attrs={"class": "input", "min": 1}),
        }

    def __init__(self, *args, **kwargs):
        # views.py passes user=request.user (ignore unless needed)
        kwargs.pop("user", None)
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)



    def clean_business_reg_no(self):
        return _format_business_reg_no(self.cleaned_data.get("business_reg_no"))

    def clean_business_phone(self):
        return _format_mobile_phone(self.cleaned_data.get("business_phone"))

    def clean_rep_rrn_full(self):
        return _format_rrn(self.cleaned_data.get("rep_rrn_full"))

class ConsultationForm(ModelForm):
    class Meta:
        model = Consultation
        fields = [
            "outcome",
            "note",
            "visit_date",
            "visit_reservation_at",
            "fitting_note",
        ]
        widgets = {
            "outcome": forms.Select(attrs={"class": "select"}),
            "note": forms.Textarea(attrs={"class": "textarea", "rows": 4, "placeholder": "상담 내용(선택)"}),
            "visit_date": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "visit_reservation_at": forms.DateTimeInput(attrs={"class": "input", "type": "datetime-local", "step": "600"}),
            "fitting_note": forms.Textarea(attrs={"class": "textarea", "rows": 3, "placeholder": "피팅/조절(선택)"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

    def clean(self):
        cleaned = super().clean()
        visit_date = cleaned.get("visit_date")
        reservation = cleaned.get("visit_reservation_at")
        # 둘 다 입력되면 예약을 우선하고 방문일은 비웁니다.
        if reservation and visit_date:
            cleaned["visit_date"] = None
        return cleaned


class AfterServiceForm(ModelForm):
    """A/S 접수/수정 폼 (v1: 유상/무상 + 비용/결제/환불 최소 구성)"""

    # NOTE: 금액 입력은 화면에서 콤마(1,000) 입력을 허용해야 해서 CharField로 오버라이드합니다.
    # clean()에서 숫자만 추출해 int로 변환 후 모델에 저장합니다.
    amount = forms.CharField(
        required=False,
        widget=forms.TextInput(
            attrs={
                "class": "input money-input",
                "inputmode": "numeric",
                "autocomplete": "off",
                "placeholder": "예: 1,000",
            }
        ),
    )
    paid_amount = forms.CharField(
        required=False,
        widget=forms.TextInput(
            attrs={
                "class": "input money-input",
                "inputmode": "numeric",
                "autocomplete": "off",
                "placeholder": "예: 1,000",
            }
        ),
    )

    # ✅ 무상 → 유상 전환 사유(모달에서 입력, 저장 시 이벤트로 기록)
    paid_toggle_reason = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )

    # 환불은 제품/결제 탭처럼 '환불' 버튼(모달)로만 처리합니다.
    # 따라서 환불 금액/일자는 이 폼에서 직접 수정하지 않습니다.

    class Meta:
        from .models import AfterService

        model = AfterService
        fields = [
            "received_at",
            "status",
            "target_side",
            "owner",
            "reason_code",
            "reason_text",
            "memo",
            "is_paid",
            "amount",
            "paid_amount",
            "payment_method",
            "tax_type",
            "paid_at",
            "deposited_at",
        ]
        widgets = {
            "received_at": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "status": forms.Select(attrs={"class": "select"}),
            "target_side": forms.Select(attrs={"class": "select"}),
            "owner": forms.TextInput(attrs={"class": "input", "placeholder": "담당자"}),
            "reason_code": forms.Select(attrs={"class": "select"}),
            "reason_text": forms.TextInput(attrs={"class": "input", "placeholder": "기타 사유를 입력해 주세요"}),
            "memo": forms.Textarea(attrs={"class": "textarea", "rows": 3, "placeholder": "메모(선택)"}),
            "is_paid": forms.CheckboxInput(attrs={"class": "checkbox"}),
            # amount/refund_amount는 위에서 CharField로 오버라이드
            "payment_method": forms.Select(attrs={"class": "select"}),
            "tax_type": forms.Select(attrs={"class": "select"}),
            "paid_at": forms.DateInput(attrs={"class": "input", "type": "date"}),
            "deposited_at": forms.DateInput(attrs={"class": "input", "type": "date"}),
            # 환불은 별도 모달에서 처리
        }

    def __init__(self, *args, **kwargs):
        # ✅ 제품/결제(회차)에서 선택된 좌/우 기준으로 A/S '대상' 선택지를 제한합니다.
        # - 예: 메인만 좌 → [좌]
        # - 예: 메인 좌 + 서브 우 → [좌, 우, 양이]
        allowed_target_sides = kwargs.pop("allowed_target_sides", None)
        super().__init__(*args, **kwargs)
        _apply_input_class(self.fields)

        # 대상(좌/우/양이) 선택지 제한
        # 규칙(확정):
        # - 기존 A/S(as_id로 선택된 건): 그 A/S 레코드의 target_side "단 1개"만 노출
        # - 신규 A/S(as_new=1): 현재 제품/결제(메인/서브) 기준으로만 노출
        try:
            label_map = {
                "LEFT": "좌",
                "RIGHT": "우",
                "BOTH": "양이",
            }

            inst = getattr(self, "instance", None)
            cur = getattr(inst, "target_side", None)

            # 1) 기존 레코드 편집(=as_id 선택)인 경우: 현재 제품 상태와 무관하게 "그 건의 값"만
            if inst is not None and getattr(inst, "pk", None) and cur:
                self.fields["target_side"].choices = [(cur, label_map.get(cur, cur))]

            # 2) 신규(as_new=1)인 경우: allowed_target_sides만(절대 cur를 끼워넣지 않음)
            elif allowed_target_sides:
                self.fields["target_side"].choices = [(v, label_map.get(v, v)) for v in allowed_target_sides]
        except Exception:
            pass

        # ✅ 요청사항: A/S 상태 드롭다운(접수일 옆)에서만 '진행중' 대신 '진행'으로 표기
        # - 저장값은 IN_PROGRESS 그대로 유지 (뱃지 판정/로직 유지)
        try:
            st = self.fields.get("status")
            if st and getattr(st, "choices", None):
                new_choices = []
                for v, lbl in list(st.choices):
                    if v == "IN_PROGRESS":
                        new_choices.append((v, "진행"))
                    else:
                        new_choices.append((v, lbl))
                st.choices = new_choices
        except Exception:
            pass

    def clean(self):
        cleaned = super().clean()

        def _money_to_int(v) -> int:
            s = str(v or "")
            digits = "".join(ch for ch in s if ch.isdigit())
            try:
                return int(digits) if digits else 0
            except Exception:
                return 0

        status = (cleaned.get("status") or "").strip()
        is_paid = bool(cleaned.get("is_paid"))
        amount = _money_to_int(cleaned.get("amount"))
        paid_amount = _money_to_int(cleaned.get("paid_amount"))
        paid_at = cleaned.get("paid_at")
        deposited_at = cleaned.get("deposited_at")
        # 환불은 별도 처리 (refund_after_service)
        reason_code = (cleaned.get("reason_code") or "").strip()
        reason_text = (cleaned.get("reason_text") or "").strip()

        # 사유: 기타면 상세 필수
        if reason_code == "ETC" and not reason_text:
            self.add_error("reason_text", "기타 사유를 입력해 주세요.")

        # 상태별 날짜
        if status == "COMPLETED" and not cleaned.get("completed_at"):
            # completed_at은 뷰에서 자동 세팅도 하므로 폼에서는 강제하지 않음
            pass
        if status == "CANCELED" and not cleaned.get("canceled_at"):
            pass

        # 유상/무상 전환 규칙
        # 1) 유상 → 무상: '환불 후'에만 가능(전액 환불 상태에서만 변경 허용)
        # 2) 무상 → 유상: 허용 (현장 요구: 취소/재진행 포함해 같은 건에서 유상으로 전환 가능)
        if self.instance and self.instance.pk:
            try:
                prev_paid = bool(self.instance.is_paid)
                # 무상 → 유상 전환: 즉시 허용
                if prev_paid and (not is_paid):
                    prev_amount = int(getattr(self.instance, "amount", 0) or 0)
                    prev_refund = int(getattr(self.instance, "refund_amount", 0) or 0)
                    if prev_amount > 0 and prev_refund < prev_amount:
                        self.add_error("is_paid", "유상 → 무상 변경은 전액 환불 후에만 가능합니다. 먼저 환불을 처리해 주세요.")
            except Exception:
                pass

        if not is_paid:
            # 신규 무상 건은 비용/결제 입력을 비웁니다.
            # (수정 시에는 위의 변경불가 규칙으로 인해 기존 유상 데이터가 임의로 지워지지 않습니다.)
            if not (self.instance and self.instance.pk):
                cleaned["amount"] = 0
                cleaned["paid_amount"] = 0
                cleaned["payment_status"] = ""
                cleaned["payment_method"] = ""
                cleaned["tax_type"] = ""
                cleaned["paid_at"] = None
                cleaned["deposited_at"] = None
        else:
            cleaned["amount"] = amount
            cleaned["paid_amount"] = paid_amount
            if amount <= 0:
                self.add_error("amount", "유상일 때는 비용을 입력해 주세요.")
            if paid_amount < 0:
                self.add_error("paid_amount", "결제 금액이 올바르지 않습니다.")
            if paid_amount > amount > 0:
                self.add_error("paid_amount", "결제 금액은 비용을 초과할 수 없습니다.")

            method = (cleaned.get("payment_method") or "").strip()
            if paid_amount > 0:
                if not method:
                    self.add_error("payment_method", "결제 방식을 선택해 주세요.")
                if method == "카드" and not paid_at:
                    self.add_error("paid_at", "카드 결제 시 결제일을 입력해 주세요.")
                if method and method != "카드" and not deposited_at:
                    self.add_error("deposited_at", "입금일을 입력해 주세요.")

            # 결제상태는 자동 판정(저장 시 views에서 누적 결제/환불 기준 재계산)
            cleaned["payment_status"] = ""

            # 과세구분은 센터 정책상 유상 A/S는 항상 "과세"로 고정합니다.
            cleaned["tax_type"] = "과세"

            # 결제일/입금일은 선택(센터마다 다름)
            # - 카드: 결제일, 그 외: 입금일

        return cleaned