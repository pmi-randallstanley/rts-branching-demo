/*
$Rev: 9932 $ 
$Author: randall.stanley $ 
$Date: 2011-01-26 11:11:35 -0500 (Wed, 26 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_student_fl_lg_bq_filter.sql $
$Id: etl_imp_student_fl_lg_bq_filter.sql 9932 2011-01-26 16:11:35Z randall.stanley $ 
*/

drop procedure if exists etl_imp_student_fl_lg_bq_filter//

create definer=`dbadmin`@`localhost` procedure etl_imp_student_fl_lg_bq_filter()
contains sql
sql security invoker
comment '$Rev: 9932 $ $Date: 2011-01-26 11:11:35 -0500 (Wed, 26 Jan 2011) $'


proc: begin 

    declare v_curr_school_year_id         int(11);
    declare v_fcat_school_year_id         int(11);
    declare v_fl_state_id                 int(11);
    declare v_filter_metadata_count       tinyint(4);
    declare v_value_text_yes              varchar(50);
    declare v_value_text_no               varchar(50);
    declare v_value_text_unknown          varchar(50);
    
        
    # Get state code for FL.  We won't process anything unless we're in FL
    select  state_id
    into    v_fl_state_id
    from    pmi_admin.pmi_state
    where   state_abbr = 'fl'
    ;
    
    # Set generic type codes that will be used for the learning gain and bottom quartile filters
    # ############################################################################################# #
    # #        THIS NEEDS TO BE SET TO Filter Metadata that will be used FOR BQ and LG            # #
    # ############################################################################################# #
    
    # Set value text values that will be loaded
    set v_value_text_yes := 'Yes';
    set v_value_text_no := 'No';
    set v_value_text_unknown := 'Unknown';
            
    select  count(*)
    into    v_filter_metadata_count
    from    c_generic_type
    where   generic_type_code in ('pmiStuFltr1','pmiStuFltr2','pmiStuFltr3','pmiStuFltr4')
    and     active_flag = 1
    ;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    # Only proceed if client is FL client, and filter metadata exists for that district
    if @state_id = v_fl_state_id and v_filter_metadata_count > 0 then
    
    
        #################################
        #create a tmp tables
        #################################
    
        drop table if exists `tmp_student_math_read_lg_bq`;
        drop table if exists `tmp_student_lg_bq`;
        
        CREATE TABLE `tmp_student_math_read_lg_bq` (
          `student_id` int(10) NOT NULL,
          `school_id` int(10) NOT NULL,
          `school_year_id` int(10) NOT NULL,
          `fcat_yr_grade_code` varchar(15) default NULL,
          `curr_yr_grade_code` varchar(15) default NULL,
          `ayp_subject_code` varchar(25) NOT NULL,
          `ayp_subject_id` int(10) NOT NULL,
          `fcat_yr_pmi_al` tinyint(1) default NULL,         
          `prior_yr_pmi_al` tinyint(1) default NULL,    
          `fcat_yr_ayp_score` decimal(9,3) default NULL,
          `prior_yr_ayp_score` decimal(9,3) default NULL,
          `fcat_yr_dev_score` decimal(9,3) default NULL,
          `prior_yr_dev_score` decimal(9,3) default NULL,
          `lg_flag` tinyint(1) default NULL,
          `rank` int(10) default NULL,
          `bq_flag` tinyint(1) default NULL,
          PRIMARY KEY (`student_id`,`school_year_id`,`ayp_subject_id`),
          KEY `ind_tmp_student_math_read_lg_bq_sch_grd` (`school_id`,`fcat_yr_grade_code`, `ayp_subject_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_student_lg_bq` (
          `student_id` int(10) NOT NULL,
          `student_code` varchar(15) NOT NULL,
          `math_lg` varchar(25) default NULL,
          `reading_lg` varchar(25) default NULL,
          `math_bq` varchar(25) default NULL,
          `reading_bq` varchar(25) default NULL,
          PRIMARY KEY (`student_id`),
          UNIQUE KEY `uq_tmp_student_lg_bq` (`student_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        # Get current school year ID
        select  y.school_year_id
        into    v_curr_school_year_id
        from    c_school_year y
        where   y.active_flag = 1
        ;
    
        # Get FCAT School Year ID - based on AYP Reporting Flag
        select    max(atty.school_year_id)
        into      v_fcat_school_year_id
        from      c_ayp_test_type_year atty
        join      c_ayp_test_type att
                  on    atty.ayp_test_type_id = att.ayp_test_type_id
                  and   att.moniker = 'fcat'
        where     atty.ayp_reporting_flag = 1
        ;
      
        ##################################################################################################
        #get student from core tables and their ayp achievement data. Only for fcatmath and fcatReading
        ###################################################################################################
        insert tmp_student_math_read_lg_bq (
            student_id
            ,school_id
            ,school_year_id
            ,fcat_yr_grade_code
            ,curr_yr_grade_code
            ,ayp_subject_code
            ,ayp_subject_id
            ,fcat_yr_pmi_al
            ,prior_yr_pmi_al
            ,fcat_yr_ayp_score
            ,prior_yr_ayp_score
            ,fcat_yr_dev_score
            ,prior_yr_dev_score 
            ,lg_flag
            ,bq_flag
        )
        select  currsty.student_id                as 'student_id' 
                ,currsty.school_id                as 'school_id'
                ,cass.school_year_id              as 'school_year_id'
                ,gl.grade_code                    as 'fcat_yr_grade_code'
                ,glcurr.grade_code                as 'curr_yr_grade_code'
                ,cas.ayp_subject_code             as 'ayp_subject_code' 
                ,cass.ayp_subject_id              as 'ayp_subject_id'
                ,al.pmi_al                        as 'fcat_yr_pmi_al'           
                ,NULL                             as 'prior_yr_pmi_al'  
                ,cass.ayp_score                   as 'fcat_yr_ayp_score'
                ,NULL                             as 'prior_yr_ayp_score'        
                ,cass.alt_ayp_score               as 'fcat_yr_dev_score'
                ,NULL                             as 'prior_yr_dev_score'
                ,NULL                             as 'lg_flag'
                ,NULL                             as 'bq_flag' 
        from    c_student_year as teststy  
        join    c_ayp_subject_student as cass
                on  teststy.student_id          = cass.student_id 
                and teststy.school_year_id      = cass.school_year_id
                and cass.school_year_id         = v_fcat_school_year_id  
                and cass.score_record_flag      = 1
        join    c_ayp_subject as cas
                on  cas.ayp_subject_id          = cass.ayp_subject_id 
                and cas.ayp_subject_code        in ('fcatMath','fcatReading')
        join    c_grade_level as gl
                on  gl.grade_level_id           = teststy.grade_level_id
        join    c_ayp_achievement_level as al
                on cass.al_id                   = al.al_id
        join    c_student_year currsty
                on  teststy.student_id          = currsty.student_id
                and currsty.active_flag         = 1
                and currsty.school_year_id      = v_curr_school_year_id
        join    c_grade_level glcurr
                on  glcurr.grade_level_id       = currsty.grade_level_id
        #where    teststy.active_flag            = 1
        #order by teststy.student_id, ayp_subject_code
        ;
    
        #######################################################################
        # Get prior yr ayp achievement levels per subject area
        ########################################################################
        update tmp_student_math_read_lg_bq t
        join  c_ayp_subject_student cass
              on t.student_id            = cass.student_id 
              and t.ayp_subject_id       = cass.ayp_subject_id
              and cass.school_year_id    = (v_fcat_school_year_id - 1)  
              and cass.score_record_flag = 1
        join  c_ayp_achievement_level al
              on cass.al_id = al.al_id
        set   prior_yr_pmi_al     = al.pmi_al
              ,prior_yr_ayp_score  = cass.ayp_score
              ,prior_yr_dev_score  = cass.alt_ayp_score
        ;
    
        #################################################################
        # Flag Learning Gain Students per Subject (Reading/Math only)
        #################################################################
        update  tmp_student_math_read_lg_bq t
        -- Determine if student's scores are considered a learning gain (lg)
        set t.lg_flag = 
            case
                when t.ayp_subject_code = 'fcatReading' then 
                    case 
                        -- Only count students who have curr and prior yr achievement levels as qualifying for learning gains
                        when (t.fcat_yr_pmi_al IS NULL OR t.prior_yr_pmi_al IS NULL) then NULL
                        -- A decrease in AL translates into no LG 
                        when t.fcat_yr_pmi_al < t.prior_yr_pmi_al  then 0 
                        -- Achievement level (al) of 3 or higher maintained from last year is a lg
                        when (t.fcat_yr_pmi_al >= 3 and t.prior_yr_pmi_al >= 3) and (t.fcat_yr_pmi_al >= t.prior_yr_pmi_al) then 1 
                        -- 1 or more al gain from prior year score is a lg 
                        when (t.fcat_yr_pmi_al - t.prior_yr_pmi_al >= 1) then 1 
                        -- 1 yr or more growth in dev score exceeded is a lg even though al = 1 or 2
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 4  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 230 then 1 
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 5  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 166 then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 6  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 133 then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 7  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 110 then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 8  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 92  then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 9  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 77  then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 10 and t.fcat_yr_dev_score - t.prior_yr_dev_score > 77  then 1
                        else 0
                    end
                when t.ayp_subject_code = 'fcatMath' then 
                    case 
                        -- Only count students who have curr and prior yr achievement levels as qualifying for learning gains
                        when (t.fcat_yr_pmi_al IS NULL OR t.prior_yr_pmi_al IS NULL) then NULL
                        -- A decrease in AL translates into no LG 
                        when t.fcat_yr_pmi_al < t.prior_yr_pmi_al  then 0 
                        -- Achievement level (al) of 3 or higher maintained from last year is a lg
                        when (t.fcat_yr_pmi_al >= 3 and t.prior_yr_pmi_al >= 3) and (t.fcat_yr_pmi_al >= t.prior_yr_pmi_al) then 1 
                        -- 1 or more al gain from prior year score is a lg 
                        when (t.fcat_yr_pmi_al - t.prior_yr_pmi_al >= 1) then 1 
                        -- 1 yr or more growth in dev score exceeded is a lg even though al = 1 or 2
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 4  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 162 then 1 
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 5  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 119 then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 6  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 95  then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 7  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 78  then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 8  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 64  then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 9  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 54  then 1
                        when t.fcat_yr_pmi_al in(1,2) and t.fcat_yr_grade_code = 10 and t.fcat_yr_dev_score - t.prior_yr_dev_score > 48  then 1
                        else 0
                    end
                else 0
            end     
        where t.ayp_subject_code in('fcatReading','fcatMath');
        
    
        ######################################################################################
        # Flag Lower Quartile Students per School, Grade, Subject (Reading/Math only)
        ######################################################################################
    
        #fcat reading
        update tmp_student_math_read_lg_bq t
          join (
              select mr1.school_id,
                    mr1.curr_yr_grade_code, 
                    mr1.ayp_subject_code,
                    mr1.student_id,              
                    cast(mr1.fcat_yr_dev_score as signed), 
                    sum(case 
                            when cast(mr1.fcat_yr_dev_score AS SIGNED) < cast(mr2.fcat_yr_dev_score AS SIGNED) then 1 
                                else 0 
                            end) + 1 AS 'rank',
                    count(*) AS 'total_count',  
                    round(((sum(case 
                            when cast(mr1.fcat_yr_dev_score AS SIGNED) < cast(mr2.fcat_yr_dev_score AS SIGNED) then 1 
                                else 0 
                            end) + 1)/count(*))*100) AS 'percentile'
                from   tmp_student_math_read_lg_bq mr1
                join   tmp_student_math_read_lg_bq mr2
                    on  mr1.school_id        = mr2.school_id   
                    and mr1.curr_yr_grade_code      = mr2.curr_yr_grade_code 
                    and mr1.ayp_subject_id   = mr2.ayp_subject_id  
                where 
                      mr1.ayp_subject_code = 'fcatReading'
                      and mr1.fcat_yr_dev_score is not null 
                      and mr2.fcat_yr_dev_score is not null              
                group by mr1.school_id,
                        mr1.curr_yr_grade_code,
                        mr1.ayp_subject_code,
                        mr1.student_id
                order by mr1.school_id,
                        mr1.curr_yr_grade_code,
                        rank ) t2
              on  t.student_id         = t2.student_id 
              and   t.ayp_subject_code = t2.ayp_subject_code 
            SET t.rank        = t2.percentile, 
                t.bq_flag = case  when t2.percentile >= 75 then 1  
                                          else 0
                                      end; 
        
        #fcat math
        update tmp_student_math_read_lg_bq t
        join (
              select mr1.school_id, 
                      mr1.curr_yr_grade_code, 
                      mr1.ayp_subject_code,
                      mr1.student_id,              
                    cast(mr1.fcat_yr_dev_score as signed), 
                    sum(case 
                            when cast(mr1.fcat_yr_dev_score AS SIGNED) < cast(mr2.fcat_yr_dev_score AS SIGNED) then 1 
                                else 0 
                            end) + 1 AS 'rank',
                    count(*) AS 'total_count',  
                    round(((sum(case 
                                    when cast(mr1.fcat_yr_dev_score AS SIGNED) < cast(mr2.fcat_yr_dev_score AS SIGNED) then 1 
                                        else 0 
                                    end) + 1)/count(*))*100) AS 'percentile'
                from   tmp_student_math_read_lg_bq mr1
                join   tmp_student_math_read_lg_bq mr2
                    on   mr1.school_id        = mr2.school_id   
                    and  mr1.curr_yr_grade_code      = mr2.curr_yr_grade_code 
                    and  mr1.ayp_subject_id   = mr2.ayp_subject_id   
                where 
                      mr1.ayp_subject_code = 'fcatMath' 
                      and mr1.fcat_yr_dev_score is not null 
                      and mr2.fcat_yr_dev_score is not null             
                group by mr1.school_id,
                        mr1.curr_yr_grade_code,
                        mr1.ayp_subject_code,
                        mr1.student_id
                order by  mr1.school_id,
                          mr1.curr_yr_grade_code,
                          rank ) t2
            on  t.student_id        = t2.student_id
            and  t.ayp_subject_code = t2.ayp_subject_code 
        SET t.rank        = t2.percentile, 
            t.bq_flag = case  when t2.percentile >= 75 then 1  
                                      else 0        
                                  end; 
    
        ###############################################################################################################
        # Populate denormalized table tmp_student_lg_bq
        #    Originally this was intended to be an export file that we could upload.  It may be that this table is
        #    not necessary.  But logic above and below would need to change.  
        #    Rules:
        #      LG:  Only grades 4-11 count
        #           If student is in 4-11 grade, and does not have consecutive scores, then should get 'Unknown' for LG
        #           If made learning gain, then 'Yes', else 'No'
        #      BQ:  If BQ, then 'Yes' else 'No'
        ###############################################################################################################   
     
        insert into tmp_student_lg_bq (student_id, student_code, math_lg, reading_lg, math_bq, reading_bq)
        select  sty.student_id
                ,min(st.student_code)
                , min(case
                    when tmpT.ayp_subject_code is null then null
                    when tmpT.ayp_subject_code = 'fcatMath' and tmpT.fcat_yr_grade_code in ('PK','KG','1','2','3','11','12','unassigned') then null
                    when tmpT.ayp_subject_code = 'fcatMath' and tmpT.lg_flag = 1 then v_value_text_yes
                    when tmpT.ayp_subject_code = 'fcatMath' and tmpT.lg_flag = 0 then v_value_text_no
                    when tmpT.ayp_subject_code = 'fcatMath' then v_value_text_unknown
                    when tmpT.ayp_subject_code is null then v_value_text_unknown
                  end) as math_lg
                
                , min(case
                    when tmpT.ayp_subject_code is null then null
                    when tmpT.ayp_subject_code = 'fcatReading' and tmpT.fcat_yr_grade_code in ('PK','KG','1','2','3','11','12','unassigned') then null
                    when tmpT.ayp_subject_code = 'fcatReading' and tmpT.lg_flag = 1 then v_value_text_yes
                    when tmpT.ayp_subject_code = 'fcatReading' and tmpT.lg_flag = 0 then v_value_text_no
                    when tmpT.ayp_subject_code = 'fcatReading' then v_value_text_unknown
                    when tmpT.ayp_subject_code is null then v_value_text_unknown
                  end) as reading_lg
                  
                , min(case 
                    when tmpT.ayp_subject_code = 'fcatMath' and tmpT.bq_flag = 1 then v_value_text_yes
                    when tmpT.ayp_subject_code = 'fcatMath' and tmpT.bq_flag = 0 then v_value_text_no
                  end) as math_bq
                
                , min(case 
                    when tmpT.ayp_subject_code = 'fcatReading' and tmpT.bq_flag = 1 then v_value_text_yes
                    when tmpT.ayp_subject_code = 'fcatReading' and tmpT.bq_flag = 0 then v_value_text_no
                  end) as reading_bq
        from    c_student_year as sty
        join    c_student as st
                on      sty.student_id = st.student_id
        join    c_grade_level as gl
                on    sty.grade_level_id = gl.grade_level_id
        left join    tmp_student_math_read_lg_bq as tmpT
                on    sty.student_id = tmpT.student_id
        where   sty.active_flag = 1
          and   sty.school_year_id = v_curr_school_year_id
        group by sty.student_id
        ; 
    
    
        
        # Clean up
        drop table if exists `tmp_student_math_read_lg_bq`;
    
    end if; #if state = fl and filter metadata exists

end proc;
//
