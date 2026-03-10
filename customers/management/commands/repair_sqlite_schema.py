from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import connection
from django.db.utils import OperationalError


def column_exists(table: str, column: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(f"PRAGMA table_info({table});")
        cols = [row[1] for row in cursor.fetchall()]
    return column in cols


class Command(BaseCommand):
    help = "Repair SQLite schema drift (add missing columns safely)."

    def handle(self, *args, **options):
        table = "customers_customer"

        # Add any columns that are referenced by code but may be missing due to migration drift.
        statements = [
            # Soft delete
            ("is_deleted", "ALTER TABLE customers_customer ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT 0;"),
            ("deleted_at", "ALTER TABLE customers_customer ADD COLUMN deleted_at DATETIME NULL;"),
            ("deleted_by_id", "ALTER TABLE customers_customer ADD COLUMN deleted_by_id INTEGER NULL;"),

            # Common historical drift: exam_visit_1_date vs exam_visit1_date
            ("exam_visit_1_date", "ALTER TABLE customers_customer ADD COLUMN exam_visit_1_date DATE NULL;"),
            ("exam_visit_2_date", "ALTER TABLE customers_customer ADD COLUMN exam_visit_2_date DATE NULL;"),
            ("exam_visit_3_date", "ALTER TABLE customers_customer ADD COLUMN exam_visit_3_date DATE NULL;"),
            ("exam_visit1_date", "ALTER TABLE customers_customer ADD COLUMN exam_visit1_date DATE NULL;"),
            ("exam_visit2_date", "ALTER TABLE customers_customer ADD COLUMN exam_visit2_date DATE NULL;"),
            ("exam_visit3_date", "ALTER TABLE customers_customer ADD COLUMN exam_visit3_date DATE NULL;"),
        ]

        changed = 0
        for col, sql in statements:
            if column_exists(table, col):
                continue
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                changed += 1
                self.stdout.write(self.style.SUCCESS(f"ADDED: {table}.{col}"))
            except OperationalError as e:
                msg = str(e).lower()
                if "duplicate column name" in msg:
                    self.stdout.write(self.style.WARNING(f"SKIP (already exists): {table}.{col}"))
                    continue
                raise

        if changed == 0:
            self.stdout.write(self.style.SUCCESS("No changes needed. Schema already OK."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Done. Added {changed} column(s)."))
