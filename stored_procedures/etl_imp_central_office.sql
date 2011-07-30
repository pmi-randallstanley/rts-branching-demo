/*
$Rev: 9282 $ 
$Author: randall.stanley $ 
$Date: 2010-09-28 12:12:18 -0400 (Tue, 28 Sep 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_central_office.sql $
$Id: etl_imp_central_office.sql 9282 2010-09-28 16:12:18Z randall.stanley $ 
*/


DROP PROCEDURE IF EXISTS etl_imp_central_office //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_central_office()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9282 $ $Date: 2010-09-28 12:12:18 -0400 (Tue, 28 Sep 2010) $'
BEGIN

    DECLARE v_role_id int DEFAULT 0;
    
    SELECT  role_id
    INTO    v_role_id
    FROM    c_role
    WHERE   role_code = 'districtUser';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    
    drop table if exists `tmp_user`;
    drop table if exists `tmp_id_assign`;

    create temporary table `tmp_user` (
      `row_num` int(10) NOT NULL auto_increment,
      `login` varchar(64) NOT NULL,
      `email_address` varchar(64) NOT NULL,
      `password` varchar(50) default NULL,
      `user_code` varchar(25) DEFAULT NULL,
      `last_name` varchar(30) DEFAULT NULL,
      `first_name` varchar(30) DEFAULT NULL,
      `new_user_flag` tinyint(1) NOT NULL default '0',
      `active_directory_login_flag` tinyint(1) NOT NULL default '0',
      primary key (`row_num`),
      unique key `uq_tmp_user` (`login`)
    );

    create table `tmp_id_assign` (
      `new_id` int(11) not null,
      `base_code` varchar(64) not null,
      primary key  (`new_id`),
      unique key `uq_tmp_id_assign` (`base_code`)
    )
    ;

    # login is populated by active_directory_login if it exist;
    # otherwise use email address
    insert into tmp_user (
        login
        ,email_address
        ,`password`
        ,user_code
        ,last_name
        ,first_name
        ,new_user_flag
        ,active_directory_login_flag
        )

    select  dt.login
        ,dt.email_address
        ,case   when dt.active_directory_login_flag = 1 and u.user_id is null then dt.`password`
                when dt.active_directory_login_flag = 0 and u.user_id is null then 'pmi1234'
                when u.user_id is not null then u.`password`
        end
        ,coalesce(dt.employee_id, u.user_code)
        ,dt.last_name
        ,dt.first_name
        ,case when u.user_id is null then 1 else 0 end
        ,dt.active_directory_login_flag
        
    from    (
                select coalesce(replace(ods.active_directory_login, '\\\\', ''), ods.email) as login
                    ,min(ods.email) as email_address
                    ,max(case when instr(ods.active_directory_login, '\\') != 0 then sha1(rand()) else null end) as `password`
                    ,min(ods.employee_id) as employee_id
                    ,min(ods.last_name) as last_name
                    ,min(ods.first_name) as first_name
                    ,max(case when instr(ods.active_directory_login, '\\') != 0 then 1 else 0 end) as active_directory_login_flag
                    
                from    v_pmi_ods_central_office as ods
                where   coalesce(ods.active_directory_login, ods.email) is not null
                group by login
            ) as dt
    left join   c_user as u
            on      u.login = dt.login
    ;

    update  tmp_user 
    set     user_code = concat(@db_name, '_', pmi_f_get_next_client_sequence('user_code', 1)) 
    where   user_code is null
    and     new_user_flag = 1;

    # address changing login values such as a switch to Active Dir user names
    update  c_data_accessor as upd
    join    tmp_user as src
            on      upd.source_code = src.email_address
            and     upd.source_code != src.login
            and     src.new_user_flag = 1
    set     upd.source_code = src.login
    where   upd.accessor_type_code = 'u'
    ;

    # sync Active Dir update above to c_user table.
    update  c_user as upd
    join    c_data_accessor as da
            on      upd.user_id = da.accessor_id
    join    tmp_user as src
            on      da.source_code = src.login
            and     src.new_user_flag = 1
    set     upd.login = da.source_code
            ,upd.`password` = case when src.active_directory_login_flag = 1 then src.`password` else upd.`password` end
    ;
    
    insert  tmp_id_assign (new_id, base_code)
    select  pmi_admin.pmi_f_get_next_sequence('c_data_accessor', 1), src.login
    from    tmp_user as src
    left join   c_data_accessor as tar
            on      src.login = tar.source_code
            and     tar.accessor_type_code = 'u'
    where   tar.accessor_id is null
    ;

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
       ,src.login
       ,@client_id
       ,1234
       ,now()
    
    from     tmp_user as src
    left join   tmp_id_assign as tmpid
            on      src.login = tmpid.base_code
    left join   c_data_accessor as tar
            on      src.login = tar.source_code
            and     tar.accessor_type_code = 'u'
    on duplicate key update last_user_id = values(last_user_id)
    ;

    INSERT INTO c_user (
        user_id
        ,user_code
        ,login
        ,last_name
        ,first_name
        ,email_address
        ,`password`
        ,role_id
        ,client_id
        ,last_user_id
        ,create_timestamp
        ) 
        
        
    select  da.accessor_id
        ,src.user_code
        ,src.login
        ,src.last_name
        ,src.first_name
        ,src.email_address
        ,src.`password`
        ,v_role_id
        ,@client_id
        ,1234
        ,now()
    
    from    tmp_user as src
    join    c_data_accessor as da
            on      src.login = da.source_code
            and     da.accessor_type_code = 'u'
    on duplicate key update user_code = values(user_code)
        ,email_address = values(email_address)
        ,role_id = values(role_id)
        ,last_name = values(last_name)
        ,first_name = values(first_name)
        ,last_user_id = values(last_user_id)
    ;

    drop table if exists `tmp_user`;
    drop table if exists `tmp_id_assign`;

    #### Update imp_upload_log
    set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_central_office\', \'P\', \'ETL Load Successful\')');
    
    prepare sql_string from @sql_string;
    execute sql_string;
    deallocate prepare sql_string; 

END;
//
