from django.contrib import admin
from .models import (
    Customer,
    CustomerCase,
    Consultation,
    RRNAccessLog,
    BusinessProfile,
    BusinessProfileConsentLog,
    BusinessProfileAccessLog,
    PaymentTransaction,
    AfterService,
    AfterServiceEvent,
)


@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ("id", "track", "name", "phone", "address_summary", "created_at")
    list_filter = ("track",)
    search_fields = ("name", "phone", "rrn_full", "address_summary")
    ordering = ("-created_at",)


@admin.register(CustomerCase)
class CustomerCaseAdmin(admin.ModelAdmin):
    list_display = ("id", "customer", "purchase_date", "created_at")
    list_filter = ("purchase_date",)
    search_fields = ("customer__name", "customer__phone")
    ordering = ("-purchase_date", "-created_at")


@admin.register(RRNAccessLog)
class RRNAccessLogAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "customer", "accessed_at")
    search_fields = ("user__username", "customer__name", "customer__phone")
    ordering = ("-accessed_at",)


@admin.register(BusinessProfile)
class BusinessProfileAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "user",
        "business_name",
        "representative_name",
        "business_reg_no",
        "consultation_reminder_days",
        "updated_at",
    )
    search_fields = ("user__username", "business_name", "representative_name", "business_reg_no")
    ordering = ("-updated_at",)


@admin.register(Consultation)
class ConsultationAdmin(admin.ModelAdmin):
    list_display = ("id", "customer", "outcome", "visit_date", "visit_reservation_at", "created_at")
    list_filter = ("outcome",)
    search_fields = ("customer__name", "customer__phone", "note", "fitting_note")
    ordering = ("-created_at", "-id")


@admin.register(BusinessProfileConsentLog)
class BusinessProfileConsentLogAdmin(admin.ModelAdmin):
    list_display = ("id", "profile", "user", "ip", "created_at")
    search_fields = ("user__username", "profile__business_name", "profile__representative_name", "ip")
    ordering = ("-created_at",)


@admin.register(BusinessProfileAccessLog)
class BusinessProfileAccessLogAdmin(admin.ModelAdmin):
    list_display = ("id", "profile", "user", "action", "ip", "created_at")
    list_filter = ("action",)
    search_fields = ("user__username", "profile__business_name", "profile__representative_name", "ip")
    ordering = ("-created_at",)


@admin.register(PaymentTransaction)
class PaymentTransactionAdmin(admin.ModelAdmin):
    list_display = ("id", "case", "paid_at", "amount", "method", "tax_type", "created_at")
    list_filter = ("paid_at", "method", "tax_type")
    search_fields = ("case__customer__name", "case__customer__phone", "memo")
    ordering = ("-paid_at", "-id")


@admin.register(AfterService)
class AfterServiceAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "customer",
        "status",
        "is_paid",
        "amount",
        "payment_status",
        "received_at",
        "created_at",
    )
    list_filter = ("status", "is_paid", "payment_status", "received_at")
    search_fields = ("customer__name", "customer__phone", "memo", "reason_text", "owner")
    ordering = ("-created_at", "-id")


@admin.register(AfterServiceEvent)
class AfterServiceEventAdmin(admin.ModelAdmin):
    # 모델 필드명은 event_type 입니다. (Python 예약어 type 사용 금지)
    list_display = ("id", "after_service", "event_type", "message", "created_at")
    list_filter = ("event_type", "created_at")
    search_fields = ("after_service__customer__name", "after_service__customer__phone", "message")
    ordering = ("-created_at", "-id")
