# Auth Service

Микросервис пользователей, ролей и JWT-аутентификации.

## Роли

- `admin` — доступ к административной панели и списку пользователей
- `user` — запуск сравнений и история собственных расчетов

## Локальный bootstrap admin

- email: `admin@example.com`
- password: `admin12345`

## Endpoint'ы

- `POST /api/v1/auth/register`
- `POST /api/v1/auth/login`
- `GET /api/v1/users/me`
- `GET /api/v1/admin/users`
- `GET /api/v1/admin/stats`
