# Terraform-инфраструктура для MLOps-сервера в Hetzner Cloud

В этом репозитории находится Terraform-конфигурация для создания отдельного сервера в Hetzner Cloud для учебного MLOps-проекта.

## Цель

Цель работы — показать базовый жизненный цикл Infrastructure as Code:

- создание нового отдельного сервера через Terraform;
- настройка firewall;
- настройка SSH-доступа;
- создание и подключение отдельного volume для данных;
- базовая подготовка сервера для Docker Compose MLOps-стека;
- возможность проверить состояние инфраструктуры через `terraform plan`;
- возможность удалить созданные Terraform-ресурсы через `terraform destroy`.

Существующий старый Hetzner-сервер в Terraform не импортируется, не описывается в конфигурации и не управляется через Terraform.

## Архитектура

Для MLOps-стека создан новый отдельный сервер, управляемый Terraform.

Сбор первичных данных по API остаётся на старом сервере. Старый сервер может собирать raw-снапшоты, например, каждые 5 минут. Новый MLOps-сервер может позже забирать и обрабатывать эти данные реже, например каждые 15 минут.

Частота сбора raw-данных и частота обработки данных в ML-pipeline не обязаны совпадать. Такое разделение снижает нагрузку на новый сервер и защищает первичные raw-снапшоты от удаления при `terraform destroy`.

## Создаваемые ресурсы

Terraform создаёт следующие ресурсы:

- новый сервер Hetzner Cloud;
- cloud firewall;
- SSH key;
- отдельный volume;
- attachment volume к серверу.

Фактически использованный тип сервера:

- `cx33`;
- 4 vCPU;
- 8 GB RAM;
- Ubuntu 24.04 LTS.

Изначально рассматривался тип `cx32`, но в актуальном списке Hetzner Cloud доступный тип с 4 vCPU и 8 GB RAM — это `cx33`.

## Firewall

Firewall настроен минимально:

- SSH `22` открыт только с моего публичного IP;
- HTTP `80` открыт наружу;
- HTTPS `443` открыт наружу;
- ICMP открыт для проверки доступности сервера.

Порты UI-сервисов наружу не открываются:

- Airflow `8080`;
- MLflow `5000`;
- MinIO Console `9001`;
- Grafana `3000`;
- Prometheus `9090`.

Доступ к этим сервисам предполагается делать либо через SSH-туннели, либо позже через Nginx reverse proxy на портах 80/443.

## Подготовка сервера через cloud-init

При первом запуске сервера `cloud-init` выполняет базовую настройку:

- обновляет пакеты;
- устанавливает Docker;
- устанавливает Docker Compose plugin;
- создаёт пользователя `deploy`;
- настраивает SSH-доступ по ключу;
- отключает парольный SSH-вход;
- отключает root-login по SSH;
- создаёт swap-файл 4 GB;
- форматирует volume только при отсутствии файловой системы;
- монтирует volume в `/mnt/mlops-data`;
- создаёт директории под будущий MLOps-стек.

На volume созданы директории:

```text
/mnt/mlops-data/postgres
/mnt/mlops-data/minio
/mnt/mlops-data/mlflow
/mnt/mlops-data/airflow
/mnt/mlops-data/feast
/mnt/mlops-data/prometheus
/mnt/mlops-data/grafana
/mnt/mlops-data/processed-data
/mnt/mlops-data/incoming-raw
```

## После `terraform apply`

После успешного выполнения `terraform apply` Terraform создаёт сервер, firewall, SSH-ключ и volume, а `cloud-init` выполняет только базовую подготовку операционной системы и Docker-окружения.

`cloud-init` не клонирует репозиторий, не создаёт production `.env`, не запускает Docker Compose стек и не регистрирует GitHub self-hosted runner. Эти шаги выполняются отдельно, чтобы секреты и deployment-логика не хранились в Terraform-коде.

Рекомендуемый порядок действий после создания инфраструктуры:

1. Подключиться к серверу по SSH:

   ```bash
   ssh deploy@128.140.1.182
   ```


2. Проверить результат работы `cloud-init` уже на сервере:

   ```bash
   cat /home/deploy/cloud-init-checks.txt
   ```

3. Подготовить production `.env` из `.env.example` и заполнить реальные значения секретов только на сервере или через GitHub Secrets.

4. Установить или проверить GitHub self-hosted runner.

5. Запустить deploy workflow или вручную поднять стек через Docker Compose.

6. Проверить публичный health endpoint через nginx:

   ```bash
   curl http://128.140.1.182/health
   ```

Публичная prediction-точка входа: `POST http://128.140.1.182/api/predict/station`.

В финальной схеме доступа nginx является единственной публичной точкой входа на порту `80`. FastAPI напрямую доступен только локально на сервере через `127.0.0.1:8000`. Grafana доступна только через SSH-туннель на `127.0.0.1:3000`, anonymous access отключён. PostgreSQL не имеет host port mapping в Docker Compose и не публикуется наружу.

## Деинсталляция / завершение работы

Перед удалением инфраструктуры нужно учитывать, что Terraform удаляет только те ресурсы, которые описаны в этой Terraform-конфигурации: новый MLOps-сервер, firewall, SSH key, volume и attachment volume. Старый ingestion-сервер, который собирает первичные raw-данные, этой Terraform-конфигурацией не управляется и при `terraform destroy` не удаляется.

Перед удалением сервера рекомендуется сделать backup важных данных из `/mnt/mlops-data`: данных PostgreSQL, артефактов MLflow, данных MinIO, Grafana dashboards и отчётов.

Рекомендуемый порядок завершения работы:

1. Подключиться к серверу и остановить Docker Compose стек:

   ```bash
   ssh deploy@128.140.1.182
   cd /opt/bikeml
   docker compose -p bikeml down
   ```

2. Если нужно удалить также Docker volumes приложения, использовать вариант:

   ```bash
   docker compose -p bikeml down -v
   ```

   Этот шаг удаляет Docker volumes, созданные compose-проектом. Данные на отдельном Hetzner volume нужно проверять отдельно.

3. Остановить и удалить GitHub self-hosted runner с сервера, чтобы в GitHub не остался неактивный runner.

4. В локальной папке Terraform проверить план удаления:

   ```bash
   terraform plan -destroy
   ```

5. Удалить инфраструктуру:

   ```bash
   terraform destroy
   ```

После `terraform destroy` новый MLOps-сервер и управляемые Terraform ресурсы будут удалены. Старый ingestion-сервер и внешние данные, не описанные в Terraform state, останутся без изменений.
