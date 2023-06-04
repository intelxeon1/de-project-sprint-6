with user_group_messages as (
    select hg.hk_group_id,
    	   count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages    
    from INTELXEONYANDEXRU__DWH.h_groups hg     
    join INTELXEONYANDEXRU__DWH.l_groups_dialogs lgd  on hg.hk_group_id  = lgd.hk_group_id 
    join INTELXEONYANDEXRU__DWH.h_dialogs hd on lgd.hk_message_id  = hd.hk_message_id 
    join INTELXEONYANDEXRU__DWH.l_user_message lum on hd.hk_message_id  = lum.hk_message_id 
    group by hg.hk_group_id             
)
,
user_group_log as (
select hk_group_id,
       count(distinct hk_user_id ) as cnt_added_users from 
       (
		select distinct 
			luga.hk_group_id,
			luga.hk_user_id,
			from INTELXEONYANDEXRU__DWH.s_auth_history sah 
			join INTELXEONYANDEXRU__DWH.l_user_group_activity luga on sah.hk_l_user_group_activity  = luga.hk_l_user_group_activity 	
			WHERE sah.event='add' and luga.hk_group_id in ( select hk_group_id from INTELXEONYANDEXRU__DWH.h_groups order by registration_dt limit 10)
		) as c
	group by hk_group_id
)	

select l.hk_group_id, l.cnt_added_users,m.cnt_users_in_group_with_messages,m.cnt_users_in_group_with_messages / l.cnt_added_users as group_conversion
from user_group_log l join user_group_messages m on l.hk_group_id = m.hk_group_id
order by 4 desc