set SEARCH_path  to 'INTELXEONYANDEXRU__DWH';

/********************************************/
--HUB
INSERT INTO h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from INTELXEONYANDEXRU__STAGING.users
where hash(id) not in (select hk_user_id from h_users);

---
INSERT INTO h_groups 
select
       hash(id) as  hk_group_id,       
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from INTELXEONYANDEXRU__STAGING.groups 
where hash(id) not in (select hk_group_id from h_groups);


INSERT INTO h_dialogs  
select
       hash(message_id) as  hk_message_id,       
       message_id,
       message_ts,       
       now() as load_dt,
       's3' as load_src
       from INTELXEONYANDEXRU__STAGING.dialogs  
where hash(message_id) not in (select hk_message_id from h_dialogs);


/********************************************/
--LINKS

INSERT INTO l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from INTELXEONYANDEXRU__STAGING.groups as g
left join h_users as hu on g.admin_id = hu.user_id
left join h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from l_admins); 


insert into l_groups_dialogs 
select 
hash(hd.hk_message_id,g.hk_group_id),
hd.hk_message_id ,
hk_group_id, 
now() as load_dt,
's3' as load_src
from INTELXEONYANDEXRU__STAGING.dialogs d
left join h_groups g on d.message_group  = g.group_id 
left join h_dialogs hd on d.message_id = hd.message_id 
where hash(hk_message_id,hk_group_id) not in (select hk_l_groups_dialogs from l_groups_dialogs)


insert into l_user_message  
select 
hash(hd.hk_message_id,hu.hk_user_id),
hu.hk_user_id,
hd.hk_message_id,
now() as load_dt,
's3' as load_src
from INTELXEONYANDEXRU__STAGING.dialogs d
left join h_users hu on d.message_from = hu.user_id 
left join h_dialogs hd on d.message_id = hd.message_id 
where hash(hd.hk_message_id,hu.hk_user_id) not in (select hk_l_user_message from l_user_message);


/********************************************/
--Satelities
INSERT INTO INTELXEONYANDEXRU__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id
,True as is_admin
,hg.registration_dt
,now() as load_dt
,'s3' as load_src
from INTELXEONYANDEXRU__DWH.l_admins as la
left join INTELXEONYANDEXRU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


INSERT INTO INTELXEONYANDEXRU__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select hk_group_id
        ,group_name

        ,now() as load_dt
       ,'s3' as load_src
from INTELXEONYANDEXRU__STAGING.groups as g
left join INTELXEONYANDEXRU__DWH.h_groups as hg on g.id = hg.group_id;


INSERT INTO INTELXEONYANDEXRU__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
select hk_group_id
        ,is_private

        ,now() as load_dt
       ,'s3' as load_src
from INTELXEONYANDEXRU__STAGING.groups as g
left join INTELXEONYANDEXRU__DWH.h_groups as hg on g.id = hg.group_id;


INSERT INTO INTELXEONYANDEXRU__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
select hk_message_id
        ,message
        ,message_from
        ,message_to
        ,now() as load_dt
       ,'s3' as load_src
from INTELXEONYANDEXRU__STAGING.dialogs as d
left join INTELXEONYANDEXRU__DWH.h_dialogs as hg on d.message_id = hg.message_id;


INSERT INTO INTELXEONYANDEXRU__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select hk_user_id
        ,country
        ,age
        ,now() as load_dt
       ,'s3' as load_src
from INTELXEONYANDEXRU__STAGING.users as u
left join INTELXEONYANDEXRU__DWH.h_users as hg on u.id = hg.user_id;


INSERT INTO INTELXEONYANDEXRU__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
select hk_user_id
        ,chat_name
        ,now() as load_dt
       ,'s3' as load_src
from INTELXEONYANDEXRU__STAGING.users as u
left join INTELXEONYANDEXRU__DWH.h_users as hg on u.id = hg.user_id;