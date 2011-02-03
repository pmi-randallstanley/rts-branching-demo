/*
$Rev: 8480 $ 
$Author: randall.stanley $ 
$Date: 2010-04-30 08:23:32 -0400 (Fri, 30 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/adm_sch_init_setup_school.sql $
$Id: adm_sch_init_setup_school.sql 8480 2010-04-30 12:23:32Z randall.stanley $ 
*/

drop procedure if exists adm_sch_init_setup_school//

create definer=`dbadmin`@`localhost` procedure adm_sch_init_setup_school( 
    p_new_client_id         int
    ,p_school_state_code    varchar(15)
)
contains sql
sql security invoker
comment '$Rev: 8480 $ $Date: 2010-04-30 08:23:32 -0400 (Fri, 30 Apr 2010) $'


proc: begin 

    declare v_school_client_exists      int(11) default '0';
    declare v_client_code               varchar(25);
    declare v_school_name               varchar(50);

    select  count(*)
    into    v_school_client_exists
    from    pmi_admin.pmi_client
    where   client_id = p_new_client_id
    and     shared_db_member_flag = 1
    ;
    

    if v_school_client_exists > 0 then

        select  client_code
            ,moniker
        
        into    v_client_code
            ,v_school_name
            
        from    pmi_admin.pmi_client
        where   client_id = p_new_client_id
        ;

        start transaction;

        insert into c_data_accessor ( accessor_id, accessor_type_code, source_code, display_text, client_id, last_user_id, create_timestamp) 
        values (p_new_client_id, 'c', v_client_code, null, p_new_client_id, 1234, now())
        on duplicate key update last_user_id = values(last_user_id)
        ;

        insert c_school (school_id, client_id, school_code, school_state_code, moniker, last_user_id, create_timestamp)
        values (p_new_client_id, p_new_client_id, p_school_state_code, p_school_state_code, v_school_name, 1234, now() )
        on duplicate key update last_user_id = values(last_user_id)
        ;

        commit;

    end if;

end proc;
//
