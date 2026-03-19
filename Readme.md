# MSSQL2File

[![Build Status](https://img.shields.io/github/actions/workflow/status/Headcrab/mssql2file/ci.yml?branch=main&style=for-the-badge)](https://github.com/Headcrab/mssql2file/actions)
[![Release](https://img.shields.io/github/v/release/Headcrab/mssql2file?style=for-the-badge&color=blueviolet)](https://github.com/Headcrab/mssql2file/releases)
[![License](https://img.shields.io/github/license/Headcrab/mssql2file?style=for-the-badge)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue?style=for-the-badge)](https://github.com/Headcrab/mssql2file)

**MSSQL2File** — мощная утилита командной строки для экспорта данных из баз данных (MSSQL, MySQL, ClickHouse) в файлы различных форматов с поддержкой сжатия и гибкой настройкой периодов выгрузки.

## ✨ Ключевые возможности

| Возможность | Описание |
|-------------|----------|
| 🕒 **Гибкая выборка данных** | Экспорт за произвольный период или автоматическая выгрузка новых данных с последней точки |
| 📁 **Множество форматов** | Поддержка JSON, CSV и XML |
| 🗜️ **Сжатие на лету** | GZip и LZ4 для минимизации размера выходных файлов |
| ⚙️ **Гибкая настройка** | Конфигурация через параметры CLI или файл конфигурации |
| 🔌 **Мульти-БД** | Поддержка MSSQL, MySQL и ClickHouse |
| 📝 **Настройка CSV** | Настраиваемые разделители и заголовки |
| 🏷️ **Шаблоны имен файлов** | Динамические имена с датой, временем и периодом |

## 🚀 Быстрый старт

### Установка

```bash
# Скачайте последнюю версию
curl -L https://github.com/Headcrab/mssql2file/releases/latest/download/mssql2file.tar.gz -o mssql2file.tar.gz

# Распакуйте
tar -xzf mssql2file.tar.gz

# Переместите в PATH (опционально)
sudo mv mssql2file /usr/local/bin/
```

### Базовое использование

```bash
# Экспорт последних данных в JSON
mssql2file -start last -period 1h -format json

# Выгрузка за период с сжатием
mssql2file -start "2023-02-20 00:00:00" -period 24h -format xml -compression gz

# Использование конфигурационного файла
mssql2file -config production.cfg
```

## 📖 Примеры использования

### 1. Экспорт новых данных за последний час
```bash
mssql2file \
  -start last \
  -period 1h \
  -format json \
  -output "./data" \
  -name "last_hour_{start}_{end}.{format}"
```

### 2. Сжатая выгрузка CSV с настройками
```bash
mssql2file \
  -start last \
  -period 5m \
  -format csv \
  -csv_delimiter "|" \
  -csv_header true \
  -compression lz4 \
  -output "./exports" \
  -name "data_{start}_{end}.{format}.{compression}"
```

### 3. Периодическая выгрузка через cron
```bash
# Добавить в crontab (ежечасно)
0 * * * * /usr/local/bin/mssql2file -start last -period 1h -format json -output "/backups" -name "hourly_{start}_{end}.{format}.{compression}"
```

## ⚙️ Конфигурация

Создайте JSON-файл `mssql2file.cfg` для сохранения настроек:

```json
{
  "Connection_type": "mssql",
  "Connection_string": "server=localhost;port=1433;user id=sa;password=secret;database=production;TrustServerCertificate=true;encrypt=disable;",
  "Start": "last",
  "Period": "1h",
  "Output": "./exports",
  "Template": "data_{start}_{end}.{format}.{compression}",
  "Output_format": "json",
  "Compression": "gz",
  "Csv_delimiter": ";",
  "Csv_header": true
}
```

## 📊 Поддерживаемые форматы

| Формат | Сжатие | Особенности |
|--------|--------|-------------|
| JSON   | GZip, LZ4 | Человеко-читаемый, для API |
| CSV    | GZip, LZ4 | Настраиваемые разделители |
| XML    | GZip, LZ4 | Структурированные данные |

## 🔧 Параметры командной строки

```bash
mssql2file [OPTIONS]

Основные:
  -start              Время начала (last|2023-01-01 00:00:00)
  -period             Период выгрузки (1h, 30m)
  -format             Формат файла (json|csv|xml)
  -output             Директория выгрузки
  -name               Шаблон имени файла

База данных:
  -connection_type    Тип драйвера (mssql|mysql|clickhouse)
  -connection_string  Строка подключения
  -decoder            Декодер строковых полей (windows-1251|Windows1251|cp1251|koi8-r|KOI8R)

CSV настройки:
  -csv_delimiter      Разделитель полей
  -csv_header         Включить заголовок (true|false)

Сжатие:
  -compression        Метод сжатия (gz|lz4|none)

Конфигурация:
  -config             Путь к JSON-конфигу
  -last_period_end    Последняя обработанная точка
  -query              SQL-запрос

Переменные окружения:
  M2F_CONNECTION_TYPE
  M2F_CONNECTION_STRING
  M2F_OUTPUT
  M2F_OUTPUT_FORMAT
  M2F_COMPRESSION
```

## 📝 Формат лога

Для каждой выгрузки утилита пишет одну итоговую строку:

```text
[19.03.2026 09:15:11] Период 2026-03-19 09:14:00 -> 2026-03-19 09:15:00 | строк: 18 041 | БД: 11s | файл: 0s | всего: 11s | result_260319_091400_260319_091500_1m0s.json.gz
```

Предупреждения и ошибки печатаются в том же однострочном формате.

## 🛠️ Разработка

### Сборка из исходников

```bash
git clone https://github.com/Headcrab/mssql2file.git
cd mssql2file
go build -o mssql2file ./cmd/main.go
```

### Тестирование

```bash
go test ./...
task build
```

## 📄 Лицензия

Проект распространяется под лицензией MIT — см. файл [LICENSE](LICENSE) для подробностей.

## 🤝 Вклад в проект

Мы приветствуем вклад! Пожалуйста, ознакомьтесь с:
- [Руководством по вкладу](CONTRIBUTING.md)
- [Списком задач](https://github.com/Headcrab/mssql2file/issues)

## 📞 Контакты

- **Документация**: [Wiki](https://github.com/Headcrab/mssql2file/wiki)
- **Issues**: [GitHub Issues](https://github.com/Headcrab/mssql2file/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Headcrab/mssql2file/discussions)

---

<p align="center">
  <i>Сделано с ❤️ командой <a href="https://github.com/Headcrab">Headcrab</a></i>
</p>
