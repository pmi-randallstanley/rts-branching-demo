DROP PROCEDURE IF EXISTS etl_hst_post_load_fcat20_conversion_fl //

CREATE definer=`dbadmin`@`localhost` procedure etl_hst_post_load_fcat20_conversion_fl()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$FCAT Conversion 6/13/2012 $'
BEGIN

    declare v_table_exists smallint(4);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*)
    into    v_table_exists
    from    information_schema.`tables` as t
    where   t.table_schema = 'fl_ib'
    and     t.table_name = 'z_tmp_fcat_conversion';
    

    if v_table_exists > 0 then
    
          drop table if exists `tmp_fcat_scores_to_convert`;
          
          CREATE TABLE `tmp_fcat_scores_to_convert` (
            `student_id` int(10) NOT NULL,
            `ayp_subject_id` int(10) NOT NULL,
            `school_year_id` int(10) NOT NULL,
            `month_id` tinyint(2) NOT NULL default '0',
            PRIMARY KEY  (`student_id`,`ayp_subject_id`,`school_year_id`,`month_id`)
          ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
      
          ### This is how we determine which scores to convert.  Basically, we're looking at folks who took fcatMath and fcatReading in 2011/12
          ###  That have NG / FCAT 2.0 scores.  For 2012, makes sure score is above 500 as well.
          insert into tmp_fcat_scores_to_convert(student_id, ayp_subject_id, school_year_id, month_id)
          select cass.student_id
                  , cass.ayp_subject_id
                  , cass.school_year_id
                  , cass.month_id
          from  c_ayp_subject_student cass
          join  c_ayp_subject sub 
                on    cass.ayp_subject_id = sub.ayp_subject_id 
                and   sub.ayp_subject_code in ('fcatMath','fcatReading')
          left join c_ayp_strand_student cstr 
                    on    cstr.student_id = cass.student_id 
                    AND   cstr.ayp_subject_id = cass.ayp_subject_id 
                    AND   cstr.school_year_id = cass.school_year_id 
                    AND   cstr.month_id = cass.month_id
          left join c_ayp_strand str 
                    on    cstr.ayp_subject_id = str.ayp_subject_id 
                    AND   cstr.ayp_strand_id = str.ayp_strand_id
          where (str.moniker like 'ng%' or str.moniker is null) 
            and ((cass.school_year_id = 2012 and cass.alt_ayp_score > 500) or cass.school_year_id = 2011)
          group by cass.student_id, cass.ayp_subject_id, cass.school_year_id, cass.month_id
          ;
          
          
          ### Set alt ayp score = ayp score for those fcat 2.0 scores we are converting
          update  c_ayp_subject_student cass
          join    tmp_fcat_scores_to_convert tmp
                  on    cass.student_id = tmp.student_id
                  and   cass.ayp_subject_id = tmp.ayp_subject_id
                  and   cass.school_year_id = tmp.school_year_id
                  and   cass.month_id = tmp.month_id
          set cass.ayp_score = cass.alt_ayp_score
          ;
          
          
          ### Set alt ayp score = converted score based on lookup tables
          update  c_ayp_subject_student cass
          join    tmp_fcat_scores_to_convert tmp
                  on    cass.student_id = tmp.student_id
                  and   cass.ayp_subject_id = tmp.ayp_subject_id
                  and   cass.school_year_id = tmp.school_year_id
                  and   cass.month_id = tmp.month_id
          join    c_ayp_subject sub 
                  on    cass.ayp_subject_id = sub.ayp_subject_id
          join    c_student_year sty 
                  on    cass.student_id = sty.student_id 
                  AND   cass.school_year_id = sty.school_year_id
          join    c_grade_level gl 
                  on    sty.grade_level_id = gl.grade_level_id
          join    fl_ib.z_tmp_fcat_conversion tmpC 
                  on    sub.ayp_subject_code = tmpC.ayp_subject_code
                  and   cass.alt_ayp_score between tmpC.dss_lower and tmpC.dss_upper
                  and   gl.grade_code = tmpC.grade_code
          set cass.alt_ayp_score = tmpC.fcat20_ss
          ;

          drop table if exists `tmp_fcat_scores_to_convert`;
    else
    
      select 'Error: FCAT Conversion table does not exist in FL_IB.';
          
    end if;

END;
//
