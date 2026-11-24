# Задание 1

[Диаграмма архитектуры системы](./BionicPRO_C4_model.png)

# Задание 2

[Диаграмма архитектуры ETL](./BionicPRO_C4_model_ETL.png)

Дополнения к задачам задания 2:

[Страница Keycloak, на которую редиректит неавторизованных пользователей](./auth_protection.png)

[Страница FE с возможностью выгрузить отчет, после успешной авторизации](./client_page.png)

[Страница airflow на localhost:8081/home](./airflow.png)

[Успешный результат выгрузки отчета](./success_reports_download.png)

Запуск приложения: 

Если compose уже запускался
```bash
docker compose down
rm -rf ./postgres-keycloak-data ./crm-data ./telemetry-data ./clickhouse-data ./airflow-data
```

```bash
docker compose up -d --build
```

проверьте, что все сервисы поднялись

```bash
docker compose ps
```

Подождите примерно 1-2 минуты (чтобы Airflow, ClickHouse и Keycloak - точно запустились)


PS: при тестрировании приложения иногда наблюдались проблемы с очередью инициализации/запуска контейнеров единой командой (например airflow не дожидался запуска clickhouse и падал с ошибкой). 
Для более стабильного запуска можно запустить приложения по очереди (можно проверять статус запуска сервисов между шагами с помощью docker ps | grep {your service, например clickhouse}):

бд
```bash
docker compose up -d keycloak_db crm_db telemetry_db airflow_db
```
ClickHouse
```bash
docker compose up -d clickhouse
```
Keycloack
```bash
 docker compose up -d keycloak
```
Airflow
```bash
docker compose up -d airflow-init
docker compose up -d airflow
```
api
```bash
docker compose up -d api
```
frontend
```bash
docker compose up -d frontend
```
