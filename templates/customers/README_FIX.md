# msh - SQLite 스키마 드리프트(마이그레이션 꼬임) 복구 절차

## 0) 절대 건드리지 말기
- venv/
- db.sqlite3 (데이터 유지하려면)

## 1) 서버 중지
CTRL + C

## 2) (추천) DB 백업
프로젝트 폴더의 db.sqlite3 를 다른 폴더로 복사

## 3) 스키마 자동 복구(1회)
python manage.py repair_sqlite_schema

## 4) 마이그레이션 동기화
python manage.py migrate

## 5) 실행
python manage.py runserver
