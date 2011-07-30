DROP PROCEDURE IF EXISTS etl_rpt_isel_scores //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_isel_scores()
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_isel_scores.sql $
$Id: etl_rpt_isel_scores.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
*/

BEGIN

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = database()
    and     t.table_name = 'v_pmi_ods_isel';

    if @view_exists > 0 then
    
        # limit to most recent 3 years of data including current year
        select  school_year_id - 2
        into    @min_import_year
        from    c_school_year as sy
        where   sy.active_flag = 1;
        
        truncate table rpt_isel_scores;
        
        # determine if we need to backfill c_student_year based on import data
        select  count(*)
        into    @missing_stu_year_count
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        left join   c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        where   @min_import_year <= cast(ods.school_year as signed)
        and     sty.school_year_id is null;
        
        if @missing_stu_year_count > 0 then
    
            insert ignore c_student_year (student_id, school_year_id, school_id, grade_level_id, last_user_id, create_timestamp, client_id)
            select  st.student_id
                ,sy.school_year_id
                ,csty.school_id
                ,gl.grade_level_id
                ,1234
                ,now()
                ,@client_id
                
            from    v_pmi_ods_isel as ods
            join    c_student as st
                    on      ods.student_code = st.student_code
            join    c_school_year as sy
                    on      sy.school_year_id = cast(ods.school_year as signed)
            join    c_school_year as csy
                    on      csy.active_flag = 1
            join    c_student_year as csty
                    on      st.student_id = csty.student_id
                    and     csy.school_year_id = csty.school_year_id
            join    v_pmi_xref_grade_level as vgl
                    on      ods.grade_code = vgl.client_grade_code
            join    c_grade_level as gl
                    on      vgl.pmi_grade_code = gl.grade_code
            left join   c_student_year as sty
                    on      st.student_id = sty.student_id
                    and     sty.school_year_id = sy.school_year_id
        where   @min_import_year <= cast(ods.school_year as signed)
        and     sty.school_year_id is null;
        
        end if;
        
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.alpha_recog_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'alphRecog'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.alpha_recog_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.alpha_recog_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.story_listen_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'storyListen'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.story_listen_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.story_listen_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.phone_aware_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'phonAware'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.phone_aware_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.phone_aware_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.one_to_one_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'oneToOne'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.one_to_one_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.one_to_one_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.letter_sounds_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'ltrSounds'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.letter_sounds_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.letter_sounds_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.dev_spell_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'devSpell'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.dev_spell_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.dev_spell_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.word_recog_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'wordRecog'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.word_recog_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.word_recog_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.voc_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'voc'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.voc_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.voc_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.passage_read_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'psgRead'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.passage_read_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.passage_read_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.fluency_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'fluency'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.fluency_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.fluency_score
        ;
    
        insert rpt_isel_scores (
            student_id
            ,school_year_id
            ,measure_period_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        
        select  st.student_id
            ,sty.school_year_id
            ,pimp.measure_period_id
            ,ods.total_score
            ,1234
            ,now()
            
        from    v_pmi_ods_isel as ods
        join    c_student as st
                on      ods.student_code = st.student_code
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = cast(ods.school_year as signed)
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_isel_period as pip
                on      ods.period_code = pip.period_code
        join    pm_isel_measure as pim
                on      pim.measure_code = 'total'
        join    pm_isel_measure_period as pimp
                on      pim.measure_id = pimp.measure_id
                and     pip.period_id = pimp.period_id
                and     gl.grade_level_id = pimp.grade_level_id
        where   ods.total_score is not null
        and     @min_import_year <= cast(ods.school_year as signed)
        on duplicate key update last_user_id = 1234
            ,score = ods.total_score
        ;

    end if;
    
END;
//
