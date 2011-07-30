/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_access.sql $
$Id: etl_rpt_baseball_detail_access.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_baseball_detail_access//

create definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_access()
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'

proc: begin 


    Declare v_table_exists tinyint(3);
    Declare v_access_cnt int(10);
    Declare v_school_year_id smallint(4);
    Declare v_bb_group_id int(10);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    v_table_exists
    from    information_schema.tables as t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_access';


    if v_table_exists > 0 then
    
        select school_year_id
        into v_school_year_id
        from c_school_year
        where active_flag = 1;
        
       
        select count(1)
        into v_access_cnt
        from v_pmi_ods_access;
    
        if v_access_cnt > 1 then    
        
            select bb_group_id
            into v_bb_group_id
            from pm_baseball_group
            where bb_group_code = 'access';

                     
            insert rpt_baseball_detail_access (
                bb_group_id
                ,bb_measure_id
                ,bb_measure_item_id
                ,student_id
                ,school_year_id
                ,score
                ,score_color
                ,last_user_id
                ,create_timestamp
            )
            
            select  dt.bb_group_id
                    ,dt.bb_measure_id
                    ,dt.bb_measure_list_id
                    ,dt.student_id
                    ,dt.school_year_id
                    ,dt.score
                    ,null as score_color
                    ,1234 as last_user_id
                    ,now() as create_timestamp
            from (
                        SELECT   bbm.bb_group_id
                        ,bbm.bb_measure_id
                        ,0 as bb_measure_list_id
                        ,s.student_id
                        ,v_school_year_id as school_year_id
                        ,CASE WHEN bbm.bb_measure_code = 'accessListeningSS' THEN access.listening_ss
                              WHEN bbm.bb_measure_code = 'accessListeningPL' THEN access.listening_pl
                              WHEN bbm.bb_measure_code = 'accessSpeakingSS' THEN access.speaking_ss
                              WHEN bbm.bb_measure_code = 'accessSpeakingPL' THEN access.speaking_pl
                              WHEN bbm.bb_measure_code = 'accessReadingSS' THEN access.reading_ss
                              WHEN bbm.bb_measure_code = 'accessReadingPL' THEN access.reading_pl
                              WHEN bbm.bb_measure_code = 'accessWritingSS' THEN access.writing_ss
                              WHEN bbm.bb_measure_code = 'accessWritingPL' THEN access.writing_pl
                              WHEN bbm.bb_measure_code = 'accessComprehenSS' THEN access.Comprehension_ss
                              WHEN bbm.bb_measure_code = 'accessComprehenPL' THEN access.Comprehension_pl
                              WHEN bbm.bb_measure_code = 'accessOralSS' THEN access.oral_ss
                              WHEN bbm.bb_measure_code = 'accessOralPL' THEN access.oral_pl
                              WHEN bbm.bb_measure_code = 'accessLiteracySS' THEN access.literacy_ss
                              WHEN bbm.bb_measure_code = 'accessLiteracyPL' THEN access.literacy_pl
                              WHEN bbm.bb_measure_code = 'accessCompositeSS' THEN access.Composite_ss
                              WHEN bbm.bb_measure_code = 'accessCompositePL' THEN access.composite_pl
                              WHEN bbm.bb_measure_code = 'accessListeningPLKG' THEN access.listening_pl_kg
                              WHEN bbm.bb_measure_code = 'accessSpeakingPLKG' THEN access.speaking_pl_kg
                              WHEN bbm.bb_measure_code = 'accessReadingPLKG' THEN access.reading_pl_kg
                              WHEN bbm.bb_measure_code = 'accessWritingPLKG' THEN access.writing_pl_kg
                              WHEN bbm.bb_measure_code = 'accessComprehenPLKG' THEN access.Comprehension_pl_kg
                              WHEN bbm.bb_measure_code = 'accessOralPLKG' THEN access.oral_pl_kg
                              WHEN bbm.bb_measure_code = 'accessLiteracyPLKG' THEN access.literacy_pl_kg
                              WHEN bbm.bb_measure_code = 'accessCompositePLKG' THEN access.composite_pl_kg
                              ELSE NULL END AS score
                        FROM    v_pmi_ods_access as access
                        JOIN    c_student as s
                                ON      s.student_code = access.district_student_id
                        JOIN    pm_baseball_measure as bbm
                                ON      bbm.bb_group_id = v_bb_group_id
                        ) dt
                where dt.score is not null                
                on duplicate key update  school_year_id = values(school_year_id)
                               , score = values(score)
                               , score_color = values(score_color)
                               , last_user_id = values(last_user_id);
                               
                               
                update rpt_baseball_detail_access as rpt
                join pm_baseball_measure as bbm
                            on rpt.bb_measure_id = bbm.bb_measure_id
                            and bbm.bb_measure_code in (
                                    'accessCompositePL'
                                    ,'accessCompositePLKG'
                                    ,'accessComprehenPL'
                                    ,'accessComprehenPLKG'
                                    ,'accessListeningPL'
                                    ,'accessListeningPLKG'
                                    ,'accessLiteracyPL'
                                    ,'accessLiteracyPLKG'
                                    ,'accessOralPL'
                                    ,'accessOralPLKG'
                                    ,'accessReadingPL'
                                    ,'accessReadingPLKG'
                                    ,'accessSpeakingPL'
                                    ,'accessSpeakingPLKG'
                                    ,'accessWritingPL'
                                    ,'accessWritingPLKG')
                join c_student_year as sy
                        on rpt.student_id = sy.student_id
                        and rpt.school_year_id = sy.school_year_id
                join c_grade_level as g
                        on sy.grade_level_id = g.grade_level_id
                join pm_color_access as c
                        on sy.school_year_id between c.begin_year and c.end_year
                        and g.grade_sequence between c.begin_grade_sequence and c.end_grade_sequence
                        and rpt.score between c.min_score and c.max_score
                join pmi_color as pmic
                       on c.color_id = pmic.color_id
                set score_color = pmic.moniker
                ;

        end if;

        -- Update imp_upload_log
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_access\', \'P\', \'ETL Load Successful\')');
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;
    
    end if;

end proc;
//
