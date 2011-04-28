/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_backfill_stu_year.sql $
$Id: etl_hst_load_backfill_stu_year.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

drop procedure if exists etl_hst_load_backfill_stu_year//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_backfill_stu_year()
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    #########################################################################################
    ## Requires the calling proc to have created and loaded a standard table
    ## called tmp_student_year_backfill with missing student_year records and only
    ## loads required fields all others set to table default values.
    ##  
    ##      CREATE TABLE `tmp_student_year_backfill` (
    ##          `ods_row_num` int(10) NOT NULL,
    ##          `student_id` int(10) NOT NULL,
    ##          `school_year_id` smallint(4) NOT NULL,
    ##          `grade_level_id` int(10) null,
    ##          `school_id` int(10) null,
    ##      Primary KEY  (ods_row_num ),
    ##      Key `ind_school_year` (`student_id`, `school_year_id`) ;
    #########################################################################################

    # Get unassigned grade level id
    select  grade_level_id 
    into    @grade_level_id
    from    c_grade_level gl
    where   gl.grade_code = 'unassigned'
    ;

    insert  into c_student_year (
            student_id
            ,school_year_id
            ,school_id
            ,grade_level_id
            ,last_user_id
            ,create_timestamp
            ,client_id
    )
    select  student_id
            ,school_year_id
            ,school_id
            ,grade_level_id
            ,1234 as last_user_id
            ,now() as create_timestamp
            ,@client_id as client_id
    from tmp_student_year_backfill
    on duplicate key update 
            grade_level_id = case when c_student_year.grade_level_id = @grade_level_id then values(grade_level_id) else c_student_year.grade_level_id end,
            last_user_id = 1234,
            last_edit_timestamp = now()
    ;
    
end proc;
//
