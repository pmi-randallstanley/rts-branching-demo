/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_terranova.sql $
$Id: etl_rpt_baseball_detail_terranova.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_baseball_detail_terranova//

create definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_terranova()
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'


proc: begin 

    Declare v_table_exists tinyint(3);
    Declare v_terranova_cnt int(10);
    Declare v_school_year_id smallint(4);
    Declare v_bb_group_id int(10);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    v_table_exists
    from    information_schema.tables as t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_terranova';

     if v_table_exists > 0 then
    
        select school_year_id
        into v_school_year_id
        from c_school_year
        where active_flag = 1;
        
       
        select count(1)
        into v_terranova_cnt
        from v_pmi_ods_terranova;
    
        if v_terranova_cnt > 1 then    

            select bb_group_id
            into v_bb_group_id
            from pm_baseball_group
            where bb_group_code = 'terranova';

             
            insert rpt_baseball_detail_terranova (
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
    
           
            Select    dt.bb_group_id
                     ,dt.bb_measure_id
                     ,dt.bb_measure_item_id
                     ,dt.student_id
                     ,dt.school_year_id
                     ,dt.score
                     ,null as score_color
                     ,1234 as last_user_id
                     ,now() as create_timestamp
            From (
                    SELECT   bbm.bb_group_id
                        ,bbm.bb_measure_id
                        ,0 as bb_measure_item_id
                        ,s.student_id
                        ,v_school_year_id as school_year_id 
                        ,CASE WHEN bbm.bb_measure_code = 'terranovaReadingGE' THEN terranova.reading_ge
                              WHEN bbm.bb_measure_code = 'terranovaLangArtsGE' THEN terranova.language_arts_ge
                              WHEN bbm.bb_measure_code = 'terranovaMathGE' THEN terranova.math_ge
                              WHEN bbm.bb_measure_code = 'terranovaScienceGE' THEN terranova.science_ge
                              WHEN bbm.bb_measure_code = 'terranovaTotalGE' THEN terranova.total_ge
                              WHEN bbm.bb_measure_code = 'terranovaReadingNP' THEN terranova.reading_np
                              WHEN bbm.bb_measure_code = 'terranovaLangArtsNP' THEN terranova.language_arts_np
                              WHEN bbm.bb_measure_code = 'terranovaMathNP' THEN terranova.math_np
                              WHEN bbm.bb_measure_code = 'terranovaScienceNP' THEN terranova.science_np
                              WHEN bbm.bb_measure_code = 'terranovaTotalNP' THEN terranova.total_np
                              ELSE NULL END AS score
                        FROM    v_pmi_ods_terranova as terranova
                        JOIN    c_student as s
                                ON      s.student_code = terranova.student_id
                        JOIN    pm_baseball_measure as bbm
                                ON      bbm.bb_group_id = v_bb_group_id ) dt
                where dt.score is not null                 
                on duplicate key update  school_year_id = values(school_year_id)
                               , score = values(score)
                               , score_color = values(score_color)
                               , last_user_id = values(last_user_id);

        
            update rpt_baseball_detail_terranova as rpt
            join pm_baseball_measure as bbm
                        on rpt.bb_measure_id = bbm.bb_measure_id
                        and bbm.bb_measure_code in (
                                'terranovaLangArtsNP' 
                                ,'terranovaMathNP'
                                ,'terranovaReadingNP'
                                ,'terranovaScienceNP'
                                ,'terranovaTotalNP')
            join c_student_year as sy
                    on rpt.student_id = sy.student_id
                    and rpt.school_year_id = sy.school_year_id
            join c_grade_level as g
                    on sy.grade_level_id = g.grade_level_id
            join pm_color_terranova as c
                    on sy.school_year_id between c.begin_year and c.end_year
                    and g.grade_sequence between c.begin_grade_sequence and c.end_grade_sequence
                    and rpt.score between c.min_score and c.max_score
            join pmi_color as pmic
                   on c.color_id = pmic.color_id
            set score_color = pmic.moniker
            ;
        

        end if;

        -- Update imp_upload_log
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_terranova\', \'P\', \'ETL Load Successful\')');
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;
    
    end if;

end proc;
//
