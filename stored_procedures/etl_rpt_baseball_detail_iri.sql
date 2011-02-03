/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_iri.sql $
$Id: etl_rpt_baseball_detail_iri.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
 */

####################################################################
# Insert lexile data into rpt tables for baseball report.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_baseball_detail_iri//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_iri()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'

PROC: BEGIN 
                        
    ##############################################################
    # Insert IRI data
    ##############################################################
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_iri';

    if @view_exists > 0 then

        # New ID's table for adding new baseball measures for IRI
        drop table if exists `tmp_id_assign`;
        create table `tmp_id_assign` (
            new_id int(11) not null,
            base_code varchar(50) not null,
            moniker varchar(50) default null,
            primary key  (`new_id`),
            unique key `uq_tmp_id_assign` (`base_code`)
        );
    
        select  bb_group_id
        into    @bbg_iri_id
        from    pm_baseball_group
        where   bb_group_code = 'iri'
        ;

        # Get id's for new measures (new IRI tests)
        insert  tmp_id_assign (new_id, base_code, moniker)
        select  pmi_f_get_next_sequence_app_db('pm_baseball_measure', 1), src.moniker, src.moniker
        from    v_pmi_ods_iri as src
        left join   pm_baseball_measure as tar
                on      tar.bb_group_id = @bbg_iri_id
                and     tar.bb_measure_code = src.moniker
        where   tar.bb_measure_id is null
        group by src.moniker
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
    
        select  @bbg_iri_id
            ,tmpid.new_id
            ,tmpid.base_code
            ,tmpid.moniker
            ,1
            ,1
            ,1234
            ,now()
        from    tmp_id_assign as tmpid
        ;

        # Add IRI scores
        insert rpt_baseball_detail_iri (
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
        select  bm.bb_group_id
            ,bm.bb_measure_id
            ,0
            ,st.student_id
            ,sy.school_year_id
            ,l.iri_score
            ,l.color
            ,1234
            ,now()
        from    v_pmi_ods_iri as l
        join    c_student as st
                on      l.student_id = st.student_code
                and     st.active_flag = 1
        join    c_school_year as sy
                on      sy.active_flag = 1
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sy.school_year_id = sty.school_year_id
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_baseball_measure as bm
                on      bm.bb_group_id = @bbg_iri_id
                and     bm.bb_measure_code = l.moniker
        on duplicate key update score = values(score)
            ,score_color = values(score_color)
            ,last_user_id = values(last_user_id)
        ;

        drop table if exists `tmp_id_assign`;

        -- Update imp_upload_log
        set @sql_string := '';
        set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_iri', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
        
        prepare sql_string from @sql_string;
        execute sql_string;
        deallocate prepare sql_string;     

        
    end if;
    
END PROC;
//
