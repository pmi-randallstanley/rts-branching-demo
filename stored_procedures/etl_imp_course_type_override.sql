/*
$Rev: 8513 $
$Author: randall.stanley $
$Date: 2010-05-05 13:03:34 -0400 (Wed, 05 May 2010) $
$HeadUR$
$Id: etl_imp_course_type_override.sql 8513 2010-05-05 17:03:34Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_imp_course_type_override //

##call etl_imp_course_type_override();

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_course_type_override()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8513 $ $Date: 2010-05-05 13:03:34 -0400 (Wed, 05 May 2010) $'
BEGIN

    DECLARE v_use_school_id                 char(1) default 'y';
    DECLARE v_use_course_type_override      char(1);
    DECLARE v_exclude_school_id_in_code     char(1);
  
    DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET @client_id = 0; SELECT 'Not a valid PMI client db.'; END;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    #add setting 20091005 to determine if loading course_type in this process.
    SET v_use_course_type_override := pmi_f_get_etl_setting('coreCourseTypeOverride');
    SET v_exclude_school_id_in_code := pmi_f_get_etl_setting('coreExcludeSchoolInCourseCode');

    IF v_exclude_school_id_in_code = 'y' THEN
        SET v_use_school_id = 'n';
    END IF;

    IF v_use_course_type_override = 'y' then

        ##Update Course Types from override Table
        ##added for recordnumbers 20091109 mt
        select  count(*)
        into    @record_count
        from    v_pmi_ods_course_type_override
        ;
                    
        IF @record_count > 0 then

            drop table if exists `tmp_id_assign`;

            create table tmp_id_assign (
            new_id int(11) not null,
            base_code varchar(50) not null,
            primary key (new_id),
            unique key uq_tmp_id_assign (base_code))
            ;
           
            #obtain a new id only for records that are not already in the target table
            insert tmp_id_assign (new_id, base_code)
           
            select pmi_f_get_next_sequence_app_db('c_course_type', 1), dt.course_type
            from    (
                        select  ods.course_type
                        from    v_pmi_ods_course_type_override as ods
                        where   ods.course_type is not null
                        group by ods.course_type
                    ) as dt
            left join   c_course_type as tar
                    on      dt.course_type = tar.moniker
            where   tar.moniker is null
            ;
         
            INSERT c_course_type (
                course_type_id
                ,client_id
                ,moniker
                ,last_user_id
                ,create_timestamp
            )
           
            select  coalesce(tmp1.new_id, tar.course_type_id)
                ,@client_id
                ,dt.course_type
                ,1234
                ,now()
            from    (
                        select  ods.course_type
                        from    v_pmi_ods_course_type_override as ods
                        where   ods.course_type is not null
                        group by ods.course_type

                    ) as dt
            left join   tmp_id_assign as tmp1
                    on  dt.course_type = tmp1.base_code
            left join   c_course_type as tar
                    on  dt.course_type = tar.moniker
            where   tar.moniker is null
            ;

            set @sql_upd_type := 'UPDATE c_course as c
            left join v_pmi_ods_course_type_override as vc
            on c.course_code = ';
            
            IF v_use_school_id = 'y' THEN
                set @sql_upd_type := concat(@sql_upd_type, 'CONCAT(vc.school_id, \'_\', vc.course_id)'); 
            ELSE 
                set @sql_upd_type := concat(@sql_upd_type, 'vc.course_id');
            END IF;
           
            set @sql_upd_type := concat(@sql_upd_type
                , ' join    c_course_type as ct 
                            on ct.moniker = vc.course_type 
                    set c.course_type_id = COALESCE(ct.course_type_id, 0) 
                    where ct.course_type_id != coalesce(c.course_type_id, 0);');
            
            PREPARE stmt FROM @sql_upd_type;
       
            EXECUTE stmt;
       
            DEALLOCATE PREPARE stmt;
       
           
            # Cleanup
            drop table if exists tmp_id_assign;
  
            #Update imp_upload_log
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_course_type_override\', \'P\', \'ETL Load Successful\')');
          
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;

        END IF;
        
    END IF;
           
END;
//

