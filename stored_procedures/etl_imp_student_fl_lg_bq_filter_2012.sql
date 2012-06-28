drop procedure if exists etl_imp_student_fl_lg_bq_filter_2012//

create definer=`dbadmin`@`localhost` procedure etl_imp_student_fl_lg_bq_filter_2012()
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
    declare v_school_id_cur               int(10);
    declare v_curr_yr_grade_code_cur      varchar(15);
    declare v_ayp_subject_id_cur          int(10);
    declare v_count_holder                smallint;
    declare v_dev_score_cutoff            decimal(9,3);
    declare no_more_rows                  boolean;           
    
        
    declare cur_1 cursor for 
    select  tmp.school_id, tmp.curr_yr_grade_code, tmp.ayp_subject_id
    from    tmp_student_math_read_lg_bq tmp
    join    c_ayp_subject sub on tmp.ayp_subject_id = sub.ayp_subject_id
    where   sub.ayp_subject_code in ('fcatMath','fcatReading') 
      and   tmp.fcat_yr_dev_score is not null
    group by tmp.school_id, tmp.curr_yr_grade_code, tmp.ayp_subject_code
    order by tmp.school_id, tmp.curr_yr_grade_code, tmp.ayp_subject_code;
    
    declare continue handler for not found 
    set no_more_rows = true;
        
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
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    # Only proceed if client is FL client, and filter metadata exists for that district
    #  This logic has been amended to make sure the fcat year_id year is >= 2012 b/c it is a new calculation
    if @state_id = v_fl_state_id and v_filter_metadata_count > 0 and v_fcat_school_year_id >= 2012 then
    
        Select 'Processing LG/BQ for school year: ', v_fcat_school_year_id;
    
        #################################
        #create a tmp tables
        #################################
    
        drop table if exists `tmp_student_math_read_lg_bq`;
        drop table if exists `tmp_student_lg_bq`;
        drop table if exists `tmp_school_subject_grade_ranking`;
        
        CREATE TABLE `tmp_student_math_read_lg_bq` (
          `student_id` int(10) NOT NULL,
          `school_id` int(10) NOT NULL,
          `school_year_id` int(10) NOT NULL,
          `fcat_yr_grade_code` varchar(15) default NULL,
          `fcat_prior_yr_grade_code` varchar(15) default NULL,
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
  
  
        CREATE TABLE `tmp_school_subject_grade_ranking` (
          `school_id` int(10) NOT NULL,
          `curr_yr_grade_code` varchar(15) NOT NULL,
          `ayp_subject_id` int(10) NOT NULL,
          `fcat_yr_dev_score` decimal(9,3) default NULL,
          `ranking` int(10) default NULL,
          PRIMARY KEY (`school_id`,`curr_yr_grade_code`,`ayp_subject_id`,`fcat_yr_dev_score`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        
      
        ##################################################################################################
        #get student from core tables and their ayp achievement data. Only for fcatmath and fcatReading
        ###################################################################################################
        insert tmp_student_math_read_lg_bq (
            student_id
            ,school_id
            ,school_year_id
            ,fcat_yr_grade_code
            ,fcat_prior_yr_grade_code
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
                ,NULL                             as 'fcat_prior_year_grade_code'
                ,glcurr.grade_code                as 'curr_yr_grade_code'
                ,cas.ayp_subject_code             as 'ayp_subject_code' 
                ,cass.ayp_subject_id              as 'ayp_subject_id'
                ,al.pmi_al                        as 'fcat_yr_pmi_al'           
                ,NULL                             as 'prior_yr_pmi_al'  
                ,max(cass.ayp_score)              as 'fcat_yr_ayp_score'
                ,NULL                             as 'prior_yr_ayp_score'        
                ,max(cass.alt_ayp_score)          as 'fcat_yr_dev_score'
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
        #  New Logic to make sure we're only dealing with FCAT 2.0 scores (as identified by having NG Strand data)
        left join c_ayp_strand_student cstr 
                    on    cstr.student_id = cass.student_id 
                    AND   cstr.ayp_subject_id = cass.ayp_subject_id 
                    AND   cstr.school_year_id = cass.school_year_id 
                    AND   cstr.month_id = cass.month_id
        left join c_ayp_strand str 
                  on    cstr.ayp_subject_id = str.ayp_subject_id 
                  AND   cstr.ayp_strand_id = str.ayp_strand_id
        where str.moniker like 'ng%' 
        group by currsty.student_id, currsty.school_id, cass.school_year_id, gl.grade_code, glcurr.grade_code, cass.ayp_subject_id, al.pmi_al
        ;
    
        #######################################################################
        # Get prior yr ayp achievement levels per subject area
        ########################################################################
        update tmp_student_math_read_lg_bq t
        join  c_ayp_subject_student cass
              on    t.student_id           = cass.student_id 
              and   t.ayp_subject_id       = cass.ayp_subject_id
              and   cass.school_year_id    = (v_fcat_school_year_id - 1)  
              and   cass.score_record_flag = 1
        join  c_student_year sty
              on    cass.student_id = sty.student_id
              and   sty.school_year_id   = (v_fcat_school_year_id - 1)
        join  c_grade_level as gl
              on  gl.grade_level_id      = sty.grade_level_id
        join  c_ayp_achievement_level al
              on    cass.al_id = al.al_id
        set   prior_yr_pmi_al     = al.pmi_al
              ,prior_yr_ayp_score  = cass.ayp_score
              ,prior_yr_dev_score  = cass.alt_ayp_score
              ,fcat_prior_yr_grade_code = gl.grade_code
        ## Concerned about using where exists, but we need to make sure that the prev year score is NG
        where exists (
                      select 'x'
                      from    c_ayp_strand_student cstr
                      join    c_ayp_strand str
                              on      cstr.ayp_subject_id = str.ayp_subject_id 
                              AND     cstr.ayp_strand_id = str.ayp_strand_id
                              AND     str.moniker like 'ng%'
                      where   cass.student_id = cstr.student_id
                        and   cass.ayp_subject_id = cstr.ayp_subject_id
                        and   cass.school_year_id = cstr.school_year_id
                        and   cass.month_id = cstr.month_id
                     )
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
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 4  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 12 then 1 
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 5  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 10 then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 6  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 9 then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 7  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 8 then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 8  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 7  then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 9  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 6  then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 10 and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 8  then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 4  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 11 then 1 
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 5  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 9 then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 6  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 8 then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 7  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 7 then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 8  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 6  then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 9  and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 5  then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 10 and t.fcat_yr_dev_score - t.prior_yr_dev_score >= 7  then 1
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
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 4  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 16 then 1 
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 5  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 10 then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 6  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 10  then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 7  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 9  then 1
                        when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 8  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 11  then 1
                        #when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 9  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 54  then 1
                        #when t.fcat_yr_pmi_al = 1 and t.prior_yr_pmi_al = 1 and t.fcat_yr_grade_code = 10 and t.fcat_yr_dev_score - t.prior_yr_dev_score > 48  then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 4  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 15 then 1 
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 5  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 9 then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 6  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 9  then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 7  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 8  then 1
                        when t.fcat_yr_pmi_al = 2 and t.prior_yr_pmi_al = 2 and t.fcat_yr_grade_code = 8  and t.fcat_yr_dev_score - t.prior_yr_dev_score > 10  then 1
                        else 0
                    end
                else 0
            end     
        where t.ayp_subject_code in('fcatReading','fcatMath');
        
    
        ######################################################################################
        # Flag Lower Quartile Students per School, Grade, Subject (Reading/Math only)
        ######################################################################################


    
        # New logic to calculate bottom quartile.  Basically, we are to count only the unique scores by school, subject and grade
        #   Then get the score that is at the 25 percentile (round up - getting more is better than getting less)
        #   Then include everyone in the school, subject and grade that are <= that score
        #   BQ only applies to Math and Reading.
        open cur_1;
        loop_cur_1: loop
        
            fetch  cur_1 
            into   v_school_id_cur, v_curr_yr_grade_code_cur, v_ayp_subject_id_cur;

            if no_more_rows then
                close cur_1;
                leave loop_cur_1;
            end if;
            
            delete from tmp_school_subject_grade_ranking;
            
            SET @row_num := 0;
            insert into tmp_school_subject_grade_ranking (school_id, curr_yr_grade_code, ayp_subject_id, fcat_yr_dev_score, ranking)
            select school_id, curr_yr_grade_code, ayp_subject_id, fcat_yr_dev_score, (@row_num := @row_num + 1) AS row_num
            from tmp_student_math_read_lg_bq
            where school_id = v_school_id_cur
              and curr_yr_grade_code = v_curr_yr_grade_code_cur
              and ayp_subject_id = v_ayp_subject_id_cur
            group by school_id, curr_yr_grade_code, ayp_subject_id, fcat_yr_dev_score
            order by 4;
          
            #Get cutoff for the bottom 25%.  Round up - it's better to have more than less
            select ceiling(count(*) *.25) into v_count_holder  
            from   tmp_school_subject_grade_ranking;
           
            #Get cutoff score
            select  fcat_yr_dev_score into v_dev_score_cutoff
            from    tmp_school_subject_grade_ranking
            where   ranking = v_count_holder
            ;
            
            update tmp_student_math_read_lg_bq tmp
            set     tmp.bq_flag =
                      case 
                        when fcat_yr_dev_score <= v_dev_score_cutoff then 1
                        else 0
                      end
            where   tmp.ayp_subject_id = v_ayp_subject_id_cur
              and   tmp.school_id = v_school_id_cur
              and   tmp.curr_yr_grade_code = v_curr_yr_grade_code_cur
            ;
            
            
            ## New for 2012 school year.  Lev 3 students are no longer included in bq
            update tmp_student_math_read_lg_bq tmp
            set     tmp.bq_flag = 0 
            where  tmp.fcat_yr_pmi_al >= 3
            ;
            
            ## New for 2012 school year.  If student is retained, and is at lev 1 or 2, they are added to the bottom quartile
            update  tmp_student_math_read_lg_bq tmp
            set     tmp.bq_flag = 1
            where   tmp.fcat_yr_grade_code = tmp.fcat_prior_yr_grade_code  ## this is retained student indicator - grade levels are the same
              and   tmp.fcat_yr_pmi_al in (1,2)
            ;
            
            
            
            
        end loop loop_cur_1;
    
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
                    #when tmpT.ayp_subject_code = 'fcatMath' and tmpT.fcat_yr_grade_code in ('PK','KG','1','2','3','11','12','unassigned') then null
                    #change for 2012 - 9/10th grade students no longer apply to calculation
                    when tmpT.ayp_subject_code = 'fcatMath' and tmpT.fcat_yr_grade_code in ('PK','KG','1','2','3','9','10','11','12','unassigned') then null
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
        drop table if exists `tmp_school_subject_grade_ranking`;
    
    else
        Select 'Not FL or wrong year - no processing.';
    end if; #if state = fl and filter metadata exists

end proc;
//
