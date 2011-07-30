
/*
$Rev: 8503 $ 
$Author: randall.stanley $ 
$Date: 2010-05-04 09:55:23 -0400 (Tue, 04 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_custom_user_role.sql $
$Id: etl_imp_custom_user_role.sql 8503 2010-05-04 13:55:23Z randall.stanley $ 
 */

#INSERT into:
#    c_data_accessor
#    c_user
#    c_user_school_list 
#    for building role level and above
###  call etl_imp_custom_user_role();


DROP PROCEDURE IF EXISTS etl_imp_custom_user_role //
# 
CREATE definer=`dbadmin`@`localhost` procedure etl_imp_custom_user_role()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8503 $ $Date: 2010-05-04 09:55:23 -0400 (Tue, 04 May 2010) $'
BEGIN

    DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET @client_id = 0; SELECT 'Not a valid PMI client db.'; END;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    IF @client_id > 0 THEN 
 
            select count(*)
            into @record_count
            from v_pmi_ods_custom_user_role;
    
            IF @record_count > 0 then
            
                drop table if exists tmp_dups;
                
                CREATE TABLE tmp_dups (
                    row_num int(10) NOT NULL auto_increment
                    ,email_address varchar(64) NOT NULL
                    ,PRIMARY KEY (row_num)
                    ,unique key uq_tmp_dups (email_address)
                );
                
                
                DROP TABLE IF EXISTS tmp_user;
            
                CREATE TABLE tmp_user (
                    row_num int(10) NOT NULL auto_increment
                    ,email_address varchar(64) NOT NULL
                    ,accessor_id int(10) null
                    ,user_code varchar(25) DEFAULT NULL
                    ,role_id int(10) not null
                    ,last_name varchar(30) DEFAULT NULL
                    ,first_name varchar(30) DEFAULT NULL
                    ,middle_initial char(1) DEFAULT NULL
                    ,new_user_flag tinyint(1) NOT NULL
                    ,school_id varchar(4) null
                    ,access_level smallint(3) not null
                    ,PRIMARY KEY (row_num)
                    ,key uq_tmp_user (email_address)
                );
                
                drop table if exists tmp_id_assign
                 ;
                create table tmp_id_assign (
                    new_accessor_id int(10) null
                    ,new_access_level smallint(6) not null
                    ,max_role_id int(10) not null
                    ,base_code varchar(50) not null
                    ,primary key (new_accessor_id)
                    ,unique key uq_tmp_id_assign (base_code)
                    );
                    
                drop table if exists tmp_user_group_by
                 ;
                 
                create table tmp_user_group_by (
                    email_address varchar(124) not null
                    ,user_code varchar(50) null
                    ,access_level varchar(50) null
                    ,primary key (email_address)
                    );
                    

                insert into tmp_dups (email_address)
                
                select a.email
                from v_pmi_ods_custom_user_role a
                join v_pmi_ods_custom_user_role b
                    on a.email = b.email
                    and a.first_name <> b.first_name
                    and a.last_name <> b.last_name
                group by a.email;
                    

                INSERT INTO tmp_user (
                    email_address,
                    accessor_id,
                    user_code,
                    role_id,
                    last_name,
                    first_name,
                    new_user_flag,
                    school_id,
                    access_level
                    )
                SELECT o.email,
                    da.accessor_id,
                    o.employee_id,
                    r.role_id,
                    o.last_name,
                    o.first_name,
                    CASE WHEN u.user_id IS NULL THEN 1 ELSE 0 END,
                    o.school_id,
                    access_level                
                 FROM v_pmi_ods_custom_user_role AS o
                 join c_role r
                     on o.role_code = r.role_code
                 join pmi_access_level pal
                     on r.access_level_id = pal.access_level_id
                     and pal.access_level between 100 and 200
                     and not (pal.access_level = 200 and school_id is null)
                 lEFT JOIN c_user AS u
                     on      u.login = o.email
                 left join c_data_accessor da
                     on o.email = da.source_code
                 left join tmp_dups dups
                     on o.email = dups.email_address
                 WHERE    o.email IS NOT NULL
                     and instr(o.email,'@') > 0
                     and dups.email_address is null
                 order by email, access_level;
                
                
                 insert into tmp_user_group_by (
                     email_address
                     ,user_code
                     ,access_level
                 )   
                 select email_address
                       ,max(user_code) user_code
                       ,min(access_level) min_access_level
                 from tmp_user
                 group by  email_address
                 ;

                 
                 insert into tmp_id_assign (
                     new_accessor_id, 
                     new_access_level, 
                     max_role_id, 
                     base_code
                 )
                 select  
                     coalesce(tmp.accessor_id, pmi_admin.pmi_f_get_next_sequence('c_data_accessor', 1))  
                     ,tmp.access_level
                     ,tmp.role_id
                     ,tmp.email_address
                 from tmp_user tmp
                 join tmp_user_group_by tmp2
                     on tmp.email_address = tmp2.email_address
                     and tmp.access_level = tmp2.access_level
                 group by tmp.email_address, tmp.accessor_id, tmp2.user_code, role_id, access_level
                 ;
                

                INSERT INTO c_data_accessor (
                    accessor_id, 
                    accessor_type_code, 
                    source_code, 
                    client_id, 
                    last_user_id 
                    ) 
                    
                   select new_accessor_id,
                       'u',
                       tmp_a.base_code,
                       @client_id,
                       1234
                   from tmp_id_assign tmp_a
                   left join c_data_accessor da
                       on tmp_a.new_accessor_id = da.accessor_id
                   where da.accessor_id is null;                                      
                                        
               
                INSERT INTO c_user (
                    user_id, 
                    user_code,
                    login,
                    last_name, 
                    first_name, 
                    email_address,
                    role_id,
                    client_id,
                    last_user_id,
                    create_timestamp
                    ) 
                                        
                    SELECT distinct tmp_a.new_accessor_id,
                            tmp.user_code,
                            tmp_a.base_code,
                            tmp.last_name,
                            tmp.first_name,
                            tmp.email_address,
                            tmp_a.max_role_id,
                            @client_id client_id,
                            1234 last_user,
                            current_timestamp curr_ts
                    FROM tmp_user AS tmp
                    join tmp_id_assign tmp_a
                        on tmp.email_address = tmp_a.base_code
                        and tmp.access_level = tmp_a.new_access_level
                    ON DUPLICATE KEY UPDATE last_user_id = 1234
                        ,last_name = tmp.last_name
                        ,first_name = tmp.first_name
                        ,user_code = tmp.user_code
                        ,role_id = tmp_a.max_role_id;

                    
                    INSERT INTO c_user_school_list (
                        user_id
                        ,school_id
                        ,client_id
                        ,user_code
                        ,role_id
                        ,last_user_id
                        ,create_timestamp
                    ) 
                    SELECT  dt.user_id
                        ,dt.school_id
                        ,@client_id
                        ,dt.employee_id
                        ,dt.role_id
                        ,1234
                        ,current_timestamp
                    FROM    (
                            SELECT tmp_a.new_accessor_id as user_id,
                                c.school_id,
                                o.user_code AS employee_id, 
                                o.role_id AS role_id
                            FROM tmp_user AS o
                            join tmp_id_assign tmp_a
                                on o.email_address = tmp_a.base_code
                            join c_school c
                                on o.school_id = c.school_code
                            where o.access_level = 200
                            ) AS dt
                    ON DUPLICATE KEY UPDATE last_user_id = 1234
                            ,user_code = dt.employee_id
                            ,role_id = dt.role_id
                            ;
           
                # Cleanup
                DROP TABLE if exists tmp_dups;
                DROP TABLE if exists tmp_user;
                DROP TABLE if exists tmp_id_assign;
                DROP TABLE if exists tmp_user_group_by;
       
                #Update imp_upload_log
                set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_custom_user_role\', \'P\', \'ETL Load Successful\')');
               
                prepare sql_string from @sql_string;
                execute sql_string;
                deallocate prepare sql_string;
            
            END IF;
            
    END IF;

END;
//
