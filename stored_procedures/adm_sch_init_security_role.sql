/*
$Rev: 8370 $ 
$Author: randall.stanley $ 
$Date: 2010-04-01 15:43:48 -0400 (Thu, 01 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/adm_sch_init_security_role.sql $
$Id: adm_sch_init_security_role.sql 8370 2010-04-01 19:43:48Z randall.stanley $ 
*/

drop procedure if exists adm_sch_init_security_role//

create definer=`dbadmin`@`localhost` procedure adm_sch_init_security_role(p_new_client_id int)
contains sql
sql security invoker
comment '$Rev: 8370 $ $Date: 2010-04-01 15:43:48 -0400 (Thu, 01 Apr 2010) $'


proc: begin 

    declare v_school_client_exists     int(11) default '0';

    select  count(*)
    into    v_school_client_exists
    from    pmi_admin.pmi_client
    where   client_id = p_new_client_id
    and     shared_db_member_flag = 1
    ;
    

    if v_school_client_exists > 0 then

        delete from c_role_da_list
        where   client_id = p_new_client_id
        ;

        insert c_role_da_list (
            client_id
            ,role_id
            ,accessor_id
            ,last_user_id
            ,create_timestamp
        )
        select  p_new_client_id
            ,role_id
            ,p_new_client_id
            ,1234
            ,now()
            
        from   c_role
        where  role_code in ('principal', 'teacher', 'schoolAdmin')
        on duplicate key update last_user_id = values(last_user_id)
        ;

    end if;

end proc;
//
