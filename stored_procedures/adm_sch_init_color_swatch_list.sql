/*
$Rev: 8370 $ 
$Author: randall.stanley $ 
$Date: 2010-04-01 15:43:48 -0400 (Thu, 01 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/adm_sch_init_color_swatch_list.sql $
$Id: adm_sch_init_color_swatch_list.sql 8370 2010-04-01 19:43:48Z randall.stanley $ 
*/

drop procedure if exists adm_sch_init_color_swatch_list//

create definer=`dbadmin`@`localhost` procedure adm_sch_init_color_swatch_list(p_new_client_id int)
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
    
        insert c_color_swatch_list (
            swatch_id
            ,client_id
            ,color_id
            ,sort_order
            ,last_user_id
            ,create_timestamp
        )

        select  src.swatch_id
            ,p_new_client_id
            ,src.color_id
            ,src.sort_order
            ,1234
            ,now()

        from    c_color_swatch_list as src
        where   src.client_id = 0
        on duplicate key update last_user_id = values(last_user_id)
        ;

    end if;

end proc;
//
