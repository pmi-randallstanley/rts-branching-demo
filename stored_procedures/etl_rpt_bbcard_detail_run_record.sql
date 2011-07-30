/*
$Rev: 9764 $ 
$Author: randall.stanley $ 
$Date: 2010-12-08 09:50:45 -0500 (Wed, 08 Dec 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_run_record.sql $
$Id: etl_rpt_bbcard_detail_run_record.sql 9764 2010-12-08 14:50:45Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_run_record//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_run_record()
contains sql
sql security invoker
comment '$Rev: 9764 $ $Date: 2010-12-08 09:50:45 -0500 (Wed, 08 Dec 2010) $'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);
    declare v_bb_group_id int(11);
    declare v_curr_year_id smallint(6);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_running_record';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        select  school_year_id
        into    v_curr_year_id
        from    c_school_year
        where   active_flag = 1
        ;

        select  bb_group_id
        into    v_bb_group_id
        from    pm_bbcard_group
        where   bb_group_code = 'runRecord'
        ;

        delete from rpt_bbcard_detail_running_record
        where   bb_group_id = v_bb_group_id
        ;


        # begin level
        insert rpt_bbcard_detail_running_record (
            bb_group_id
            ,bb_measure_id
            ,bb_measure_item_id
            ,student_id
            ,school_year_id
            ,score
            ,score_type
            ,score_color
            ,last_user_id
            ,create_timestamp
        )
        select  m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
            ,v_curr_year_id
            ,max(case when mi.bb_measure_item_code = 'book' then ods.book 
                    when mi.bb_measure_item_code = 'wordRecogErrors' then ods.word_recog_errors
                    when mi.bb_measure_item_code = 'totalComprehErrors' then ods.total_compreh_errors
                    when mi.bb_measure_item_code = 'readLevel' then ods.reading_level
                    when mi.bb_measure_item_code = 'explicitErrors' then ods.explicit_errors
                    when mi.bb_measure_item_code = 'implicitErrors' then ods.implicit_errors
                    when mi.bb_measure_item_code = 'fluencyLevel' then ods.fluency_level
                    when mi.bb_measure_item_code = 'fluencyConcern' then ods.fluency_concerns
                    when mi.bb_measure_item_code = 'promptComprehQuest' then ods.prompt_compreh_questions
                    when mi.bb_measure_item_code = 'previewStrategies' then ods.preview_strategies
                    when mi.bb_measure_item_code = 'trackPrintLtoR' then ods.track_print_l_to_r
                    when mi.bb_measure_item_code = 'returnSweep' then ods.return_sweep
                    when mi.bb_measure_item_code = 'sightWords' then ods.sight_words
                    when mi.bb_measure_item_code = 'beginSounds' then ods.begin_sounds
                    when mi.bb_measure_item_code = 'medialSounds' then ods.medial_sounds
                    when mi.bb_measure_item_code = 'finalSounds' then ods.final_sounds
                    when mi.bb_measure_item_code = 'wordParts' then ods.word_parts
                    when mi.bb_measure_item_code = 'pictureClues' then ods.picture_clues
                    when mi.bb_measure_item_code = 'useContext' then ods.use_context
                    when mi.bb_measure_item_code = 'blankSkip' then ods.blank_skip
                    when mi.bb_measure_item_code = 'returnToBlankSkip' then ods.return_to_blank_skip
                    when mi.bb_measure_item_code = 'nonsenseSelfCorrect' then ods.nonsense_self_correct
                    when mi.bb_measure_item_code = 'referBack' then ods.refer_back
                    when mi.bb_measure_item_code = 'keyWords' then ods.key_words
                    when mi.bb_measure_item_code = 'rereadToClarify' then ods.reread_to_clarify
                    when mi.bb_measure_item_code = 'conceptsPrint' then ods.concepts_print
                    end) as score
            ,'a'
            ,null
            ,1234
            ,now()
        from    v_pmi_ods_running_record as ods
        join    c_student as s
                on      s.student_code = ods.student_id
        join    pm_bbcard_group as b
                on  bb_group_id = v_bb_group_id
        join    pm_bbcard_measure as m
                on      m.bb_group_id = b.bb_group_id
                and     m.bb_measure_code = 'beginLevel'
        join    pm_bbcard_measure_item as mi
                on      m.bb_group_id = mi.bb_group_id
                and     m.bb_measure_id = mi.bb_measure_id
        where   ods.time_of_year_code = 'b'
        group by m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
        having score is not null
        on duplicate key update score = values(score)
        ;

        # middle level
        insert rpt_bbcard_detail_running_record (
            bb_group_id
            ,bb_measure_id
            ,bb_measure_item_id
            ,student_id
            ,school_year_id
            ,score
            ,score_type
            ,score_color
            ,last_user_id
            ,create_timestamp
        )
        select  m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
            ,v_curr_year_id
            ,max(case when mi.bb_measure_item_code = 'book' then ods.book 
                    when mi.bb_measure_item_code = 'wordRecogErrors' then ods.word_recog_errors
                    when mi.bb_measure_item_code = 'totalComprehErrors' then ods.total_compreh_errors
                    when mi.bb_measure_item_code = 'readLevel' then ods.reading_level
                    when mi.bb_measure_item_code = 'explicitErrors' then ods.explicit_errors
                    when mi.bb_measure_item_code = 'implicitErrors' then ods.implicit_errors
                    when mi.bb_measure_item_code = 'fluencyLevel' then ods.fluency_level
                    when mi.bb_measure_item_code = 'fluencyConcern' then ods.fluency_concerns
                    when mi.bb_measure_item_code = 'promptComprehQuest' then ods.prompt_compreh_questions
                    when mi.bb_measure_item_code = 'previewStrategies' then ods.preview_strategies
                    when mi.bb_measure_item_code = 'trackPrintLtoR' then ods.track_print_l_to_r
                    when mi.bb_measure_item_code = 'returnSweep' then ods.return_sweep
                    when mi.bb_measure_item_code = 'sightWords' then ods.sight_words
                    when mi.bb_measure_item_code = 'beginSounds' then ods.begin_sounds
                    when mi.bb_measure_item_code = 'medialSounds' then ods.medial_sounds
                    when mi.bb_measure_item_code = 'finalSounds' then ods.final_sounds
                    when mi.bb_measure_item_code = 'wordParts' then ods.word_parts
                    when mi.bb_measure_item_code = 'pictureClues' then ods.picture_clues
                    when mi.bb_measure_item_code = 'useContext' then ods.use_context
                    when mi.bb_measure_item_code = 'blankSkip' then ods.blank_skip
                    when mi.bb_measure_item_code = 'returnToBlankSkip' then ods.return_to_blank_skip
                    when mi.bb_measure_item_code = 'nonsenseSelfCorrect' then ods.nonsense_self_correct
                    when mi.bb_measure_item_code = 'referBack' then ods.refer_back
                    when mi.bb_measure_item_code = 'keyWords' then ods.key_words
                    when mi.bb_measure_item_code = 'rereadToClarify' then ods.reread_to_clarify
                    when mi.bb_measure_item_code = 'conceptsPrint' then ods.concepts_print
                    end) as score
            ,'a'
            ,null
            ,1234
            ,now()
        from    v_pmi_ods_running_record as ods
        join    c_student as s
                on      s.student_code = ods.student_id
        join    pm_bbcard_group as b
                on  bb_group_id = v_bb_group_id
        join    pm_bbcard_measure as m
                on      m.bb_group_id = b.bb_group_id
                and     m.bb_measure_code = 'middleLevel'
        join    pm_bbcard_measure_item as mi
                on      m.bb_group_id = mi.bb_group_id
                and     m.bb_measure_id = mi.bb_measure_id
        where   ods.time_of_year_code = 'm'
        group by m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
        having score is not null
        on duplicate key update score = values(score)
        ;

        # end level
        insert rpt_bbcard_detail_running_record (
            bb_group_id
            ,bb_measure_id
            ,bb_measure_item_id
            ,student_id
            ,school_year_id
            ,score
            ,score_type
            ,score_color
            ,last_user_id
            ,create_timestamp
        )
        select  m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
            ,v_curr_year_id
            ,max(case when mi.bb_measure_item_code = 'book' then ods.book 
                    when mi.bb_measure_item_code = 'wordRecogErrors' then ods.word_recog_errors
                    when mi.bb_measure_item_code = 'totalComprehErrors' then ods.total_compreh_errors
                    when mi.bb_measure_item_code = 'readLevel' then ods.reading_level
                    when mi.bb_measure_item_code = 'explicitErrors' then ods.explicit_errors
                    when mi.bb_measure_item_code = 'implicitErrors' then ods.implicit_errors
                    when mi.bb_measure_item_code = 'fluencyLevel' then ods.fluency_level
                    when mi.bb_measure_item_code = 'fluencyConcern' then ods.fluency_concerns
                    when mi.bb_measure_item_code = 'promptComprehQuest' then ods.prompt_compreh_questions
                    when mi.bb_measure_item_code = 'previewStrategies' then ods.preview_strategies
                    when mi.bb_measure_item_code = 'trackPrintLtoR' then ods.track_print_l_to_r
                    when mi.bb_measure_item_code = 'returnSweep' then ods.return_sweep
                    when mi.bb_measure_item_code = 'sightWords' then ods.sight_words
                    when mi.bb_measure_item_code = 'beginSounds' then ods.begin_sounds
                    when mi.bb_measure_item_code = 'medialSounds' then ods.medial_sounds
                    when mi.bb_measure_item_code = 'finalSounds' then ods.final_sounds
                    when mi.bb_measure_item_code = 'wordParts' then ods.word_parts
                    when mi.bb_measure_item_code = 'pictureClues' then ods.picture_clues
                    when mi.bb_measure_item_code = 'useContext' then ods.use_context
                    when mi.bb_measure_item_code = 'blankSkip' then ods.blank_skip
                    when mi.bb_measure_item_code = 'returnToBlankSkip' then ods.return_to_blank_skip
                    when mi.bb_measure_item_code = 'nonsenseSelfCorrect' then ods.nonsense_self_correct
                    when mi.bb_measure_item_code = 'referBack' then ods.refer_back
                    when mi.bb_measure_item_code = 'keyWords' then ods.key_words
                    when mi.bb_measure_item_code = 'rereadToClarify' then ods.reread_to_clarify
                    when mi.bb_measure_item_code = 'conceptsPrint' then ods.concepts_print
                    end) as score
            ,'a'
            ,null
            ,1234
            ,now()
        from    v_pmi_ods_running_record as ods
        join    c_student as s
                on      s.student_code = ods.student_id
        join    pm_bbcard_group as b
                on  bb_group_id = v_bb_group_id
        join    pm_bbcard_measure as m
                on      m.bb_group_id = b.bb_group_id
                and     m.bb_measure_code = 'endLevel'
        join    pm_bbcard_measure_item as mi
                on      m.bb_group_id = mi.bb_group_id
                and     m.bb_measure_id = mi.bb_measure_id
        where   ods.time_of_year_code = 'e'
        group by m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
        having score is not null
        on duplicate key update score = values(score)
        ;

        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

end proc;
//
