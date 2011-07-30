/*
$Rev: 8470 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 15:58:28 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_class.sql $
$Id: etl_imp_class.sql 8470 2010-04-29 19:58:28Z randall.stanley $ 
 */

#insert into:
#    c_class
#    c_class_enrollment

DROP PROCEDURE IF EXISTS etl_imp_class //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_class()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8470 $ $Date: 2010-04-29 15:58:28 -0400 (Thu, 29 Apr 2010) $'
BEGIN
    DECLARE v_use_school_id char(1) default 'y';
    
    SET @@session.max_error_count = 0;
    DROP TABLE IF EXISTS tmp_class_enrollment;
    SET @@session.max_error_count = 64;
    
    
    SET @useIncrementalSchedule := pmi_f_get_etl_setting('useIncrementalSchedule');
    
    IF @useIncrementalSchedule = 'y' THEN 
        TRUNCATE TABLE c_class_enrollment;
    END IF;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    truncate table c_ayp_year_class;
    
    CREATE TABLE tmp_class_enrollment (
        school_id int,
        course_id int,
        section_id int,
        user_id int,
    --    marking_period_event_id int,
        student_id int,
        key ind1_tmp_class_enrollment (student_id),
        key ind2_tmp_class_enrollment (school_id, course_id, section_id, user_id, student_id)
    );


    select  school_year_id
    into    @curr_year
    from    c_school_year
    where   active_flag = 1
    ;

    SET @setting := pmi_f_get_etl_setting('coreExcludeSchoolInCourseCode');
    
    IF @setting = 'y' THEN 
        SET v_use_school_id = 'n';
    END IF;

    # moved loading of c_course_section from etl_imp_course to here
    SET @sql_text_course_section := 'INSERT INTO c_course_section (
        course_id 
        ,section_id 
        ,section_code 
        ,moniker 
        ,client_id 
        ,last_user_id 
        ,create_timestamp
        ) 
    
    SELECT   crs.course_id
        ,pmi_f_get_next_sequence_app_db(\'c_course_section\', 1)
        ,dt.section_id
        ,CONCAT(crs.moniker, \' - \', dt.section_id)
        ,@client_id
        ,1234
        ,current_timestamp
        
    FROM    (  ';
    
    IF v_use_school_id = 'y' THEN
        SET @sql_text_course_section := CONCAT(@sql_text_course_section, '
                SELECT  CONCAT(os.school_id, \'_\', os.course_id) AS course_code, COALESCE(os.section_id, \'\') AS section_id ');
    ELSE
        SET @sql_text_course_section := CONCAT(@sql_text_course_section, '
                SELECT  os.course_id AS course_code, COALESCE(os.section_id, \'\') AS section_id ');
    END IF;        
    
    
    SET @sql_text_course_section := CONCAT(@sql_text_course_section, '
                FROM    v_pmi_ods_schedule AS os ');
    
    IF v_use_school_id = 'y' THEN
        SET @sql_text_course_section := CONCAT(@sql_text_course_section, '
                GROUP BY os.school_id, os.course_id, os.section_id ');
    ELSE
        SET @sql_text_course_section := CONCAT(@sql_text_course_section, '
                GROUP BY os.course_id, os.section_id ');
    END IF;        
                
                
    SET @sql_text_course_section := CONCAT(@sql_text_course_section, '
                ) AS dt
        JOIN    c_course AS crs
                ON      crs.course_code = dt.course_code
        
        ON DUPLICATE KEY UPDATE last_user_id = 1234
                ,moniker = CONCAT(crs.moniker, \' - \', dt.section_id) ');

    PREPARE stmt_course_section FROM @sql_text_course_section;
    EXECUTE stmt_course_section;
    DEALLOCATE PREPARE stmt_course_section;

    SET @sql_text := 'INSERT tmp_class_enrollment (
            school_id ,
            course_id ,
            section_id ,
            user_id ,
        --    marking_period_event_id ,
            student_id
            )
        SELECT   sch.school_id,
                c.course_id,
                cs.section_id,
                usl.user_id,
        --    mpe.marking_period_event_id,
                stu.student_id
        FROM     v_pmi_ods_schedule AS o
        JOIN     c_school AS sch
                            ON    sch.school_code = o.school_id 
        JOIN     c_course AS c
                            ON    c.course_code = ';
        
    IF v_use_school_id = 'y' THEN
        SET @sql_text := CONCAT(@sql_text, ' CONCAT(o.school_id, \'_\', o.course_id) ');
    ELSE
        SET @sql_text := CONCAT(@sql_text, ' o.course_id ');
    END IF;        

    SET @sql_text := CONCAT(@sql_text, '
        JOIN     c_course_section AS cs
                            ON    cs.course_id = c.course_id
                            AND   cs.section_code = COALESCE(o.section_id, \'\')
        JOIN     c_user_school_list AS usl
                            ON    usl.school_id = sch.school_id
                            AND   usl.user_code = o.teacher_id
        JOIN     c_student AS stu
                            ON    stu.student_code = o.student_id ');
                                
   prepare stmt from @sql_text;
   execute stmt;
   deallocate prepare stmt;
    
    INSERT c_class(
        class_id
        ,school_id
        ,course_id
        ,section_id
        ,user_id
        ,client_id
        ,last_user_id
        ,create_timestamp
    )
    SELECT   pmi_f_get_next_sequence_app_db('c_class', 1)
            ,tmp.school_id
            ,tmp.course_id
            ,tmp.section_id
            ,tmp.user_id
            ,@client_id
            ,1234
            ,current_timestamp
    FROM     tmp_class_enrollment AS tmp
    WHERE NOT EXISTS    (   SELECT  *
                            FROM    c_class AS cl2
                            WHERE   cl2.school_id = tmp.school_id
                            AND     cl2.course_id = tmp.course_id
                            AND     cl2.section_id = tmp.section_id
                            AND     cl2.user_id = tmp.user_id
                                            )
    GROUP BY tmp.school_id,
            tmp.course_id,
            tmp.section_id,
            tmp.user_id
    ;
    
    
    INSERT c_class_enrollment (
        class_id
        ,student_id
        ,client_id
        ,last_user_id
        ,create_timestamp
    )
    SELECT  cl.class_id
            ,tmp.student_id
            ,@client_id
            ,1234
            ,current_timestamp
    FROM     tmp_class_enrollment tmp
    JOIN     c_class AS cl
                        ON    cl.school_id = tmp.school_id
                        AND   cl.course_id = tmp.course_id
                        AND   cl.section_id = tmp.section_id
                        AND   cl.user_id = tmp.user_id
    WHERE NOT EXISTS  ( SELECT   *
                        FROM     c_class_enrollment AS ce
                        WHERE    ce.class_id = cl.class_id
                        AND      ce.student_id = tmp.student_id
                                        )
    GROUP BY cl.class_id, tmp.student_id
    ;
    


    #cleanup
    # deactivate c_student based on c_class_enrollment non-existence
    update  c_student as st
    set     st.active_flag = 0
    where not exists (  select  *
                        from    c_class_enrollment as cle
                        where   cle.student_id = st.student_id
                     )
    ;

    delete  tesl.*
    from    c_class as cl
    join    sam_test_event_schedule_list as tesl
            on      cl.class_id = tesl.class_id
    left join   c_class_enrollment as cle
        on      cl.class_id = cle.class_id
    where   cle.class_id is null
    ;
    
    delete  cl.*
    from    c_class as cl
    left join   c_class_enrollment as cle
        on      cl.class_id = cle.class_id
    where   cle.class_id is null
    ;    

    delete cs.*
    from c_course_section cs
    left join   c_class as cl
            on      cl.course_id = cs.course_id
            and     cl.section_id = cs.section_id
    where   cl.class_id is null
    ;    


    # remove any references to values deleted above that are saved in a User Filter.
    delete  ufl.*
    from    pmi_filter as f
    join    c_user_filter_list as ufl
            on      ufl.filter_id = f.filter_id
    join    c_user as u
            on      u.user_id = ufl.user_id
    left join   c_class as fitem
            on      fitem.class_id = cast(ufl.filter_value as signed)
    where   f.filter_code in ('cohClass','glbClass')
    and     fitem.class_id is null
    ;

    # if current student is not active, deactivate student years also
    update  c_student_year as sty
    join    c_student as st
            on      sty.student_id = st.student_id
            and     st.active_flag = 0
    set     sty.active_flag = 0
    ;
 
    update  c_student_school_list as sl
    set     active_flag = 0
    ;

    insert into c_student_school_list (
        student_id
        ,school_year_id
        ,school_id
        ,enrolled_school_flag
        ,active_flag
        ,last_user_id
        ,create_timestamp
        )
    select st.student_id
        ,@curr_year
        ,sc.school_id
        ,case when sty.school_id = sc.school_id then 1 else 0 end
        ,1
        ,1234
        ,now()
        
    from c_student st
    join    (   select  ods.student_id, ods.school_id
                from    v_pmi_ods_schedule as ods
                group by ods.student_id, ods.school_id
            ) as dt
            on      st.student_code = dt.student_id
    join    c_student_year as sty
            on      sty.student_id = st.student_id
            and     sty.school_year_id = @curr_year
    join    c_school as sc
            on      sc.school_code = dt.school_id
    on duplicate key update enrolled_school_flag = values(enrolled_school_flag)
            ,active_flag = values(active_flag)
            ,last_user_id = values(last_user_id)
    ;
    
    
    update  c_student_school_list as sl
    join    c_class_enrollment as cle
            on      sl.student_id = cle.student_id
    join    c_class as cl
            on      cle.class_id = cl.class_id
            and     sl.school_id = cl.school_id
    set     sl.active_flag = 1
    where   sl.school_year_id = @curr_year
    ;

    update  c_student_school_list as sl
    join    c_student_year as sty
            on      sl.student_id = sty.student_id
            and     sl.school_id = sty.school_id
            and     sl.school_year_id = sty.school_year_id
    set     sl.active_flag = 1
    where   sl.school_year_id = @curr_year
    ;

    insert  c_ayp_year_class (class_id, school_year_id, last_user_id, create_timestamp)
    select  cle.class_id, ss.school_year_id, 1234, now()
    from    c_ayp_subject_student as ss
    join    c_class_enrollment as cle
            on      ss.student_id = cle.student_id
    group by cle.class_id, ss.school_year_id
    on duplicate key update last_user_id = 1234
    ;

    SET @@session.max_error_count = 0;
    DROP TABLE IF EXISTS tmp_class_enrollment;
    SET @@session.max_error_count = 64;

END;
//
