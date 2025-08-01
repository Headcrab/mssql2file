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
| 📁 **Множество форматов** | Поддержка JSON, CSV, XML, YAML и TOML |
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
  -output "data/last_hour_{{.Period}}.json"
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
  -output "exports/data_{{.Timestamp}}.csv.lz4"
```

### 3. Периодическая выгрузка через cron
```bash
# Добавить в crontab (ежечасно)
0 * * * * /usr/local/bin/mssql2file -start last -period 1h -format json -output "/backups/hourly_{{.Format}}.json.gz"
```

## ⚙️ Конфигурация

Создайте файл `mssql2file.cfg` для сохранения настроек:

```ini
[database]
server = localhost
user = admin
password = secret
database = production

[export]
format = json
compression = gz
csv_delimiter = ","
csv_header = true

[output]
path = /exports
template = "data_{{.Date}}_{{.Period}}.{{.Format}}"
```

## 📊 Поддерживаемые форматы

| Формат | Сжатие | Особенности |
|--------|--------|-------------|
| JSON   | GZip, LZ4 | Человеко-читаемый, для API |
| CSV    | GZip, LZ4 | Настраиваемые разделители |
| XML    | GZip, LZ4 | Структурированные данные |
| YAML   | GZip, LZ4 | Конфигурации и метаданные |
| TOML   | GZip, LZ4 | Минималистичный синтаксис |

## 🔧 Параметры командной строки

```bash
mssql2file [OPTIONS]

Основные:
  -start, -s     Время начала (last|2023-01-01 00:00:00)
  -period, -p    Период выгрузки (1h, 30m, 7d)
  -format, -f    Формат файла (json|csv|xml|yaml|toml)
  -output, -o    Шаблон имени файла

База данных:
  -server        Сервер БД
  -user          Пользователь БД
  -password      Пароль БД
  -database      Имя БД

CSV настройки:
  -csv_delimiter Разделитель полей
  -csv_header    Включить заголовок (true|false)

Сжатие:
  -compression   Метод сжатия (gz|lz4|none)

Конфигурация:
  -config, -c    Путь к файлу конфигурации
```

## 🛠️ Разработка

### Сборка из исходников

```bash
git clone https://github.com/Headcrab/mssql2file.git
cd mssql2file
go build -o mssql2file ./cmd
```

### Тестирование

```bash
go test ./...
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
