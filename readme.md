# Вебинар по Clickhouse в Нетологии

## Предварительные требования

1. Установить [docker](https://docs.docker.com/desktop/)
2. Установить [git](https://git-scm.com/)
3. Склонировать этот репозиторий
```
git clone https://github.com/Volodin-DD/clickhouse-webinar.git
```
4. Войти в папку с репозиторием
```
cd clickhouse-webinar
```
5. Запустить docker compose (на некоторых системах возможно без sudo). Также на маках с м1 не запустится, тут уж ничего не поделать.
```
sudo docker compose up -d
``` 
6. Подключиться по http (jdbc: DBeaver, IntelliJ, DataGrip) можно по адресу **localhost:8111**
7. подключиться через clickhouse-client можно по адресу: **localhost:9011**
8. Логин default без пароля

## Задания

доступны в папке **scripts**