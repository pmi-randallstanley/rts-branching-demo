/*
$Rev: 8478 $ 
$Author: randall.stanley $ 
$Date: 2010-04-30 08:22:08 -0400 (Fri, 30 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/adm_sch_init_add_admin_user.sql $
$Id: adm_sch_init_add_admin_user.sql 8478 2010-04-30 12:22:08Z randall.stanley $ 
*/

drop procedure if exists adm_sch_init_add_admin_user//

create definer=`dbadmin`@`localhost` procedure adm_sch_init_add_admin_user (
    p_new_client_id     int
    ,p_first_name       varchar(30)
    ,p_last_name        varchar(30)
    ,p_email_address    varchar(64)
)
contains sql
sql security invoker
comment '$Rev: 8478 $ $Date: 2010-04-30 08:22:08 -0400 (Fri, 30 Apr 2010) $'


proc: begin 

    declare v_school_client_exists      int(11) default '0';
    declare v_role_id                   int(11) default '0';

    select  count(*)
    into    v_school_client_exists
    from    pmi_admin.pmi_client
    where   client_id = p_new_client_id
    and     shared_db_member_flag = 1
    ;
    

    if v_school_client_exists > 0 then

        drop table if exists `tmp_user`;
        drop table if exists `tmp_id_assign`;

        CREATE TEMPORARY TABLE if not exists `tmp_user` (
          `last_name` varchar(30) default NULL,
          `first_name` varchar(30) default NULL,
          `email_address` varchar(64) default NULL,
          `password` varchar(64) default NULL,
            unique key `uq_tmp_user` (`email_address`)
        );

        create table `tmp_id_assign` (
            new_id int(11) not null,
            base_code varchar(50) not null,
            primary key  (`new_id`),
            unique key `uq_tmp_id_assign` (`base_code`)
        )
        ;

        select  role_id
        into    v_role_id
        from    c_role
        where   role_code = 'schoolAdmin';

        insert into tmp_user (
           last_name
           ,first_name
           ,email_address
        )
        
        values (p_last_name, p_first_name, p_email_address)
        ;

        truncate table tmp_id_assign;
        insert  tmp_id_assign (new_id, base_code)
        select  pmi_admin.pmi_f_get_next_sequence('c_data_accessor', 1), src.email_address
        from    tmp_user as src
        left join   c_data_accessor as tar
                on      src.email_address = tar.source_code
                and     tar.accessor_type_code = 'u'
        where   tar.accessor_id is null
        ;

        start transaction;

        insert into c_data_accessor (
           accessor_id
           ,accessor_type_code
           ,source_code
           ,client_id
           ,last_user_id 
           ,create_timestamp
        ) 
           
        
        select   coalesce(tmpid.new_id, tar.accessor_id)
           ,'u'
           ,src.email_address
           ,p_new_client_id
           ,1234
           ,now()
        
        from     tmp_user as src
        left join   tmp_id_assign as tmpid
                on      src.email_address = tmpid.base_code
        left join   c_data_accessor as tar
                on      src.email_address = tar.source_code
                and     tar.accessor_type_code = 'u'
        on duplicate key update last_user_id = values(last_user_id)
        ;


        insert into c_user (
           user_id
           ,user_code
           ,login
           ,last_name
           ,first_name
           ,email_address
           ,force_reset_pwd_flag
           ,role_id
           ,client_id
           ,last_user_id
           ,create_timestamp
           ) 
        
        select   da.accessor_id
           ,da.accessor_id
           ,src.email_address
           ,src.last_name
           ,src.first_name
           ,src.email_address
           ,0
           ,v_role_id
           ,p_new_client_id
           ,1234
           ,now()
        
        from    tmp_user  as src
        join    c_data_accessor as da
                on      src.email_address = da.source_code
                and     da.accessor_type_code = 'u'
        on duplicate key update last_user_id = values(last_user_id)
            ,force_reset_pwd_flag = values(force_reset_pwd_flag)
        ;

        insert c_user_school_list (
            user_id
            ,school_id
            ,client_id
            ,user_code
            ,role_id
            ,last_user_id
            ,create_timestamp
        )

        select  da.accessor_id
            ,p_new_client_id
            ,p_new_client_id
            ,da.accessor_id
            ,v_role_id
            ,1234
            ,now()

        from    tmp_user  as src
        join    c_data_accessor as da
                on      src.email_address = da.source_code
                and     da.accessor_type_code = 'u'
        on duplicate key update last_user_id = values(last_user_id)
        ;

        commit;

        drop table if exists `tmp_user`;
        drop table if exists `tmp_id_assign`;

    end if;

end proc;
//
