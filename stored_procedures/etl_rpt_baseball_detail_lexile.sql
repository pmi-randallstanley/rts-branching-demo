/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_lexile.sql $
$Id: etl_rpt_baseball_detail_lexile.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
 */

####################################################################
# Insert lexile data into rpt tables for baseball report.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_baseball_detail_lexile//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_lexile()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'

PROC: BEGIN 
    
                    
    ##############################################################
    # Insert Lexile data
    ##############################################################
                
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'pm_lexile_scores';

    if @table_exists > 0 then

        # New ID's table for Lexile measures (test names)
        drop table if exists `tmp_id_assign`;
        create table `tmp_id_assign` (
            new_id int(11) not null,
            base_code varchar(50) not null,
            moniker varchar(50) default null,
            primary key  (`new_id`),
            unique key `uq_tmp_id_assign` (`base_code`)
        );

        select  bb_group_id
        into    @bbg_lexile_id
        from    pm_baseball_group
        where   bb_group_code = 'lexile'
        ;

        # Get id's for new measures (new Lexile tests)
        insert  tmp_id_assign (new_id, base_code, moniker)
        select  pmi_f_get_next_sequence_app_db('pm_baseball_measure', 1), concat('lexile', src.test_moniker), src.test_moniker
        from    pm_lexile_scores as src
        left join   pm_baseball_measure as tar
                on      tar.bb_group_id = @bbg_lexile_id
                and     tar.bb_measure_code = concat('lexile', src.test_moniker)
        where   tar.bb_measure_id is null
        group by src.test_moniker
        ;
       
        # Add any new measures
        INSERT pm_baseball_measure ( 
            bb_group_id
            , bb_measure_id
            , bb_measure_code
            , moniker
            , active_flag
            , dynamic_creation_flag
            , last_user_id
            , create_timestamp
        )
    
        select  @bbg_lexile_id
            ,tmpid.new_id
            ,tmpid.base_code
            ,tmpid.moniker
            ,1
            ,1
            ,1234
            ,now()
        from    tmp_id_assign as tmpid
        ;

        # RStanley 20090925 - temporarily comment check of sy.active_flag below
        # The root cause is that we have imprope mgt of c_student_year.active flag
        # most notably in that when an "inactive" student is re-activated, we only
        # set active the "current" year c_student_year record.  Once a change is made
        # to properly manage c_student_year.active_flag, then uncomment line.
        insert rpt_baseball_detail_lexile (
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

        select bm.bb_group_id
            ,bm.bb_measure_id
            ,0
            ,l.student_id
            ,sy.school_year_id
            ,l.lexile_score
            ,cl.moniker
            ,1234
            ,now()
            
        from    pm_lexile_scores as l
        join    c_student_year as sy
                on      l.student_id = sy.student_id
                and     l.school_year_id = sy.school_year_id
#                and     sy.active_flag = 1
        join    c_grade_level as gl
                on      sy.grade_level_id = gl.grade_level_id
        join    pm_baseball_measure as bm
                on      bm.bb_group_id = @bbg_lexile_id
                and     bm.bb_measure_code = concat('lexile',l.test_moniker)
        left join   pm_color_lexile as lcl
                on      gl.grade_sequence between lcl.begin_grade_sequence and lcl.end_grade_sequence
                and     sy.school_year_id  between lcl.begin_year and lcl.end_year
                and     l.lexile_score between lcl.min_score and lcl.max_score
        left join   pmi_color as cl
                on      cl.color_id = lcl.color_id
        group by  bm.bb_group_id, bm.bb_measure_id, l.student_id, sy.school_year_id
        on duplicate key update score = values(score)
            ,score_color = values(score_color)
            ,last_user_id = values(last_user_id)
        ;

        drop table if exists `tmp_id_assign`;

    end if;

END PROC;
//
