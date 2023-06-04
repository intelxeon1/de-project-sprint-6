--dwh
/********************************************/
--HUB
set SEARCH_path  to 'INTELXEONYANDEXRU__DWH';

drop table if exists h_users;
create table h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
; 

drop table if exists h_groups;
CREATE table h_groups
(
    hk_group_id int PRIMARY KEY,
    group_id int,
    registration_dt timestamp,
    load_dt timestamp,
    load_src varchar(20)
    
)
order by load_dt
segmented by group_id all nodes
PARTITION BY ((h_groups.load_dt)::date) GROUP BY calendar_hierarchy_day(load_dt::date,2,3)

drop table if exists h_dialogs;
CREATE TABLE h_dialogs
(
    hk_message_id int PRIMARY KEY,
    message_id int,
    message_ts timestamp,
    load_dt timestamp,
    load_src varchar(20)
)
SEGMENTED BY h_dialogs.hk_message_id all nodes
PARTITION BY ((h_dialogs.load_dt)::date) GROUP BY calendar_hierarchy_day(load_dt::date,2,3)

---
/********************************************/
--LINKS
drop table if exists l_user_message;
CREATE TABLE l_user_message
(
    hk_l_user_message int primary key,
    hk_user_id int NOT NULL CONSTRAINT fk_l_user_message_user references h_users (hk_user_id),
    hk_message_id int NOT NULL CONSTRAINT fk_l_user_message_message references h_dialogs (hk_message_id),
    load_dt timestamp,
    load_src varchar(20)
)
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY (load_dt::date) GROUP BY calendar_hierarchy_day(load_dt::date,2,3);

drop table if exists l_admins;
CREATE TABLE l_admins
(
    hk_l_admin_id int primary key,
    hk_user_id int NOT NULL CONSTRAINT fk_l_admin_user references h_users (hk_user_id),
    hk_group_id   int NOT NULL CONSTRAINT fk_l_admin_group references h_groups (hk_group_id),
    load_dt timestamp,
    load_src varchar(20)
)
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY (load_dt::date) GROUP BY calendar_hierarchy_day(load_dt::date,2,3);

drop table if exists l_groups_dialogs;
CREATE TABLE l_groups_dialogs
(
    hk_l_groups_dialogs int primary key,
    hk_message_id int NOT NULL CONSTRAINT fk_l_groups_dialogs_user references h_users (hk_user_id),
    hk_group_id   int  CONSTRAINT fk_l_groups_dialogs_group references h_groups (hk_group_id),
    load_dt timestamp,
    load_src varchar(20)
)
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY (load_dt::date) GROUP BY calendar_hierarchy_day(load_dt::date,2,3);

--Satelities

drop table if exists INTELXEONYANDEXRU__DWH.s_admins;
create table INTELXEONYANDEXRU__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES INTELXEONYANDEXRU__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

drop table if exists INTELXEONYANDEXRU__DWH.s_group_name;
create table INTELXEONYANDEXRU__DWH.s_group_name
(
    hk_group_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES INTELXEONYANDEXRU__DWH.h_groups (hk_group_id),
    group_name varchar (100),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists INTELXEONYANDEXRU__DWH.s_group_private_status;
create table INTELXEONYANDEXRU__DWH.s_group_private_status
(
    hk_group_id bigint not null CONSTRAINT fk_s_group_private_status REFERENCES INTELXEONYANDEXRU__DWH.h_groups (hk_group_id),
    is_private boolean,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists INTELXEONYANDEXRU__DWH.s_dialog_info;
create table INTELXEONYANDEXRU__DWH.s_dialog_info
(
    hk_message_id bigint not null CONSTRAINT fk_s_dialog_info REFERENCES INTELXEONYANDEXRU__DWH.h_dialogs (hk_message_id),
    message varchar(1000),
    message_from int,
    message_to int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists INTELXEONYANDEXRU__DWH.s_user_socdem;
create table INTELXEONYANDEXRU__DWH.s_user_socdem
(
    hk_user_id bigint not null CONSTRAINT s_user_socdem REFERENCES INTELXEONYANDEXRU__DWH.h_users (hk_user_id),
    country varchar(100),
    age int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists INTELXEONYANDEXRU__DWH.s_user_chatinfo;
create table INTELXEONYANDEXRU__DWH.s_user_chatinfo
(
    hk_user_id bigint not null CONSTRAINT s_user_chatinfo REFERENCES INTELXEONYANDEXRU__DWH.h_users (hk_user_id),
    chat_name varchar(1000),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);