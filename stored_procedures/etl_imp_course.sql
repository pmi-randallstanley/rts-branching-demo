/*
$Rev: 8470 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 15:58:28 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_course.sql $
$Id: etl_imp_course.sql 8470 2010-04-29 19:58:28Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_imp_course //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_course()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8470 $ $Date: 2010-04-29 15:58:28 -0400 (Thu, 29 Apr 2010) $'
BEGIN
    
    DECLARE V_USE_SCHOOL_ID CHAR(1);
    DECLARE ManualCourseType char(1);
   
    DECLARE EXIT HANDLER FOR NOT FOUND BEGIN SET @client_id = 0; SELECT 'Not a valid PMI client db.'; END;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
   
    IF @client_id > 0 THEN
        SET @setting := pmi_f_get_etl_setting('coreExcludeSchoolInCourseCode');
       
        set v_use_school_id = 'y';
       
        IF @setting = 'y' THEN
            SET v_use_school_id = 'n';
        END IF;

        ##ADDED 20091109 MT
        SET ManualCourseType := pmi_f_get_etl_setting('coreCourseTypeOverride');
       
        IF ManualCourseType is null THEN
            set ManualcourseType = 'n';
        END IF;

        ##START C_COURSE_TYPE
        select count(*)
        into @record_count
        from v_pmi_ods_course;

        IF @record_count > 0 then
       
            drop table if exists tmp_id_assign
            ;
            create table tmp_id_assign (
                new_id int(11) not null,
                base_code varchar(50) not null,
                primary key (new_id),
                unique key uq_tmp_id_assign (base_code)
                );
               
                ##obtain a new id only for records that are not already in the target table
                insert tmp_id_assign (new_id, base_code)
               
                select pmi_f_get_next_sequence_app_db('c_course_type', 1),
                vc.course_type
                FROM
                (select course_type
                FROM  v_pmi_ods_course
                WHERE course_type IS NOT NULL
                group by course_type) VC
                left join c_course_type tar
                on vc.course_type = tar.moniker
                and tar.moniker is null;
       
                INSERT c_course_type (
                course_type_id,
                client_id,
                moniker,
                last_user_id,
                create_timestamp)
               
                select coalesce(tmp.new_id, tar.course_type_id),
                @client_id,
                course_type,
                vc.last_user_id,
                create_ts
                from (
                SELECT  course_type,
                        1234 last_user_id,
                        now() create_ts
                FROM v_pmi_ods_course
                WHERE course_type IS NOT NULL
                group by course_type) vc
                join tmp_id_assign tmp
                on vc.course_type = tmp.base_code
                left join c_course_type tar
                on vc.course_type = tar.moniker
                where tar.moniker is null;

                ##Start c_course update--------------------------
                 
                drop table if exists tmp_id_assign;
               
                create table tmp_id_assign (
                new_id int(11) not null,
                base_code varchar(50) not null,
                primary key (new_id),
                unique key uq_tmp_id_assign (base_code)
                );
               
                    set @sql := 'insert tmp_id_assign (new_id, base_code)
                    select pmi_f_get_next_sequence_app_db(\'c_course\', 1),';
               
                    IF v_use_school_id = 'y' THEN
                        SET @sql := CONCAT(@sql,
                            ' CONCAT(ods.school_id, \'_\', ods.course_id)');
                    ELSE
                         SET @sql := CONCAT(@sql,
                            'ods.course_id AS course_code ');
                    END IF;         
                    set @sql := concat(@sql, 
                    'FROM  v_pmi_ods_course ods
                    left join c_course tar
                    on '
                    );
                    
                    IF v_use_school_id = 'y' THEN
                        SET @sql := CONCAT(@sql,
                            ' CONCAT(ods.school_id, \'_\', ods.course_id) = tar.course_code');
                    ELSE
                         SET @sql := CONCAT(@sql,
                            'ods.course_id = tar.course_code');
                    END IF;                      
                        set @sql := concat(@sql,                     
                        ' where tar.course_code is null and ods.course_id is not null;'
                        );
                        
                        # select @sql;
                        
                    PREPARE stmt FROM @sql;
           
                    EXECUTE stmt;
           
                    DEALLOCATE PREPARE stmt;
             
                               
                SET @sql_text_course := 'INSERT INTO c_course (
                course_id,
                course_code,
                moniker, ';

                
               IF ManualCourseType = 'n' THEN
                    Set @sql_text_course := concat(@sql_text_course, ' course_type_id,');
                END IF;
        
                SET @sql_text_course := concat(@sql_text_course, ' last_user_id,
                    client_id,
                    create_timestamp
                    )
           
                SELECT  dt.course_id
                    ,dt.course_code
                    ,dt.course_title');            

                IF ManualCourseType = 'n' THEN
                    set @sql_text_course := concat(@sql_text_course ,
                    ' ,COALESCE(dt.course_type_id,0)');
                END IF;

                   
                   
                SET @sql_text_course := concat(@sql_text_course ,
                ', 1234 ,@client_id ,current_timestamp FROM  (');
       
        IF v_use_school_id = 'y' THEN
            SET @sql_text_course := CONCAT(@sql_text_course,
                    ' SELECT coalesce(tmp.new_id, tar.course_id) as course_id,  CONCAT(o.school_id, \'_\', o.course_id) AS course_code, ');
        ELSE
            SET @sql_text_course := CONCAT(@sql_text_course,
                    ' SELECT coalesce(tmp.new_id, tar.course_id) as course_id,  o.course_id AS course_code, ');
        END IF;         
       
            SET @sql_text_course := CONCAT(@sql_text_course, '
                    MAX(o.course_title) AS course_title,
                    MAX(o.course_type) AS course_type,
                    MAX(ct.course_type_id) AS course_type_id
                   
                    FROM     v_pmi_ods_course AS o
                    LEFT JOIN c_course_type ct
                    ON o.course_type = ct.moniker
                    and    o.course_id IS NOT NULL
                    LEFT JOIN tmp_id_assign tmp
                    on ');

        IF v_use_school_id = 'y' THEN
            SET @sql_text_course := CONCAT(@sql_text_course, 'CONCAT(o.school_id, \'_\', o.course_id)');
        ELSE
            SET @sql_text_course := CONCAT(@sql_text_course, 'o.course_id');
        END IF;
       
        SET @sql_text_course :=  CONCAT(@sql_text_course, ' = tmp.base_code
                left join c_course tar
                on ');
                   
        IF v_use_school_id = 'y' THEN
            SET @sql_text_course := CONCAT(@sql_text_course, ' CONCAT(o.school_id, \'_\', o.course_id)');
        ELSE
            SET @sql_text_course := CONCAT(@sql_text_course, ' o.course_id');
        END IF;
       
        SET @sql_text_course :=  CONCAT(@sql_text_course, ' = tar.course_code
                WHERE    o.course_id IS NOT NULL
                AND      o.course_title IS NOT NULL');
       
            IF v_use_school_id = 'y' THEN
                SET @sql_text_course := CONCAT(@sql_text_course, '
                    GROUP BY o.school_id, o.course_id ');           
            ELSE
                SET @sql_text_course := CONCAT(@sql_text_course, '
                    GROUP BY o.course_id ');           
            END IF;       
   
            SET @sql_text_course := CONCAT(@sql_text_course, '
                        ) AS dt
                ORDER BY dt.course_title
            ON DUPLICATE KEY UPDATE last_user_id = 1234
                    ,moniker = dt.course_title');
           
            IF ManualCourseType = 'n' THEN
                SET @sql_text_course := concat(@sql_text_course,
                ' ,course_type_id = values(course_type_id)
                    ');
            END IF;       
   
            SET @sql_text_course := concat(@sql_text_course,  ';');
            #select @sql_text_course;
       
            PREPARE stmt FROM @sql_text_course;
           
            EXECUTE stmt;
           
            DEALLOCATE PREPARE stmt;
           
            # Cleanup
            drop table if exists tmp_id_assign;
  
            #Update imp_upload_log
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_course\', \'P\', \'ETL Load Successful\')');
          
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;
           
          

        END IF;
   
    END IF;
END;
//
