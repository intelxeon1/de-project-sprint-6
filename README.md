# Проект 6-го спринта

### Описание репозитория
src\sql\dwh_ddl.sql - скрипт создание новых таблица проект
src\sql\dwh_dml.sql - скрипт наполнение новых таблиц проекта
src\sql\query.sql - запрос для ответа на бизнес-задачу


dags - исходный код python для наполнения staging слоя


Спасибо за ревью.

У меня остались следующие вопросы.

1. Существуею ли автоматизация при моделировании data vault в идеале в графическом виде типа power designer. Прописывать все руками довольно утомительно. Плюс написание запросов тоже было бы удобне отдать некому генератору?

2. Какие подходы с учетом партицирования для обновлениях данных? Есть средства автоматизации скриптов или готовые патерны?

3. В примерах часть атрибутов, которые по сути являлись внешними ключами, остались в сателитах в виде бизнес ключа хаба.
РАзве не правильно их перевести в ключ хаба? Допустимо оставлять такие сателиты ведь по сути это уже линки.

4. Как выстраивают инкрементальную загрузку для data vault спринта/проекта?

5. У нас не было время зависимых атрибутов. Правильно понимаю, что в таком случае в сателит добавляют поля active_from и active_to.
А если атрибут-внешний ключ был временнозависимый, получается, что становится время зависимый линк?

