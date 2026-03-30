# Sales Signature Hub

## One-click start (Windows)

- Double-click `launch.vbs` to start the site without a console window.
- Double-click `launch.cmd` if you want to see the startup log.
- Double-click `stop.vbs` to stop the site without a console window.
- Double-click `stop.cmd` if you prefer the console version.

## Render deploy

- Push the project to GitHub.
- Create a new Render Web Service from the repository.
- Render can detect `render.yaml` automatically.
- Set `PUBLIC_BASE_URL` to your Render URL, for example `https://your-app.onrender.com`.
- On Render free plan, local `SQLite` data and generated PDF files are temporary and can reset after redeploy or service restart.
- For persistent production data, move the database away from local `SQLite`.

Веб-приложение на Python + Flask + SQLite для управления продажами продукта, клиентской базой, PDF-документами, email-согласованиями и чатом.

## Что уже реализовано

- Обширная аналитика по продажам: выручка, pipeline, воронка, топ продуктов, топ клиентов, региональная структура.
- Дополнительные аналитические блоки: умные инсайты, прогноз, здоровье клиентов, загрузка команды, ближайшие согласования.
- База клиентов с удобным добавлением данных и фильтрацией.
- Реестр продаж с привязкой к клиентам.
- Канбан по сделкам с быстрым переводом по этапам.
- Генерация документов в PDF.
- Отправка писем клиентам с приложенным PDF и ссылкой на согласование документа.
- Страница согласования документа по уникальной ссылке.
- Клиентский чат с комнатами.
- Глобальный поиск по клиентам, продажам, документам и задачам.
- Дополнительные функции: задачи, журнал активности, email-лог, экспорт CSV, шаблоны документов, локальные заметки.

## Быстрый запуск

### Вариант 1: запуск в один клик

Просто дважды нажмите на файл:

```text
launch.cmd
```

Что делает запуск:

- сам проверяет, поднят ли уже сайт;
- при необходимости создаёт `.venv`;
- ставит зависимости только если изменился `requirements.txt`;
- запускает сервер в фоне;
- автоматически открывает страницу входа в браузере.

Для остановки сервера есть отдельный файл:

```text
stop.cmd
```

### Вариант 2: PowerShell

```powershell
.\start.ps1
```

### Вариант 3: вручную

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

После запуска откройте:

- Вход: [http://127.0.0.1:5000/login](http://127.0.0.1:5000/login)
- Главная приветственная страница: [http://127.0.0.1:5000](http://127.0.0.1:5000)
- Рабочий кабинет: [http://127.0.0.1:5000/workspace](http://127.0.0.1:5000/workspace)

## SMTP для реальной отправки писем

1. Скопируйте `.env.example` в `.env`.
2. Заполните SMTP-параметры.
3. Перезапустите приложение.

Если SMTP не настроен, система всё равно работает в demo-preview режиме:

- письмо сохраняется в лог;
- показывается готовый текст письма;
- формируется ссылка на согласование документа.

## Структура

- `app.py` - backend, API, SQLite, аналитика, PDF, email, чат
- `templates/` - HTML-шаблоны
- `static/css/styles.css` - оформление
- `static/js/app.js` - frontend-логика
- `storage/pdfs/` - сгенерированные PDF

## Идеи для следующего этапа

- Авторизация по ролям: админ, менеджер, клиент
- Онлайн-уведомления через WebSocket
- Импорт клиентов из Excel
- Интеграция с WhatsApp / Telegram / CRM
- Электронная подпись через внешний сервис
