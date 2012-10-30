drop procedure if exists etl_hst//

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_hst`()
    contains sql
    SQL SECURITY INVOKER
    COMMENT ''
proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    SELECT  count(case when st.state_abbr = 'al' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ca' then st.state_abbr end)
            ,count(case when st.state_abbr = 'co' then st.state_abbr end)
            ,count(case when st.state_abbr = 'fl' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ga' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ky' then st.state_abbr end)
            ,count(case when st.state_abbr = 'md' then st.state_abbr end)
            ,count(case when st.state_abbr = 'mn' then st.state_abbr end)
            ,count(case when st.state_abbr = 'nc' then st.state_abbr end)
            ,count(case when st.state_abbr = 'nj' then st.state_abbr end)
            ,count(case when st.state_abbr = 'oh' then st.state_abbr end)
            ,count(case when st.state_abbr = 'wy' then st.state_abbr end)
    INTO    @is_al_client
            ,@is_ca_client
            ,@is_co_client
            ,@is_fl_client
            ,@is_ga_client
            ,@is_ky_client
            ,@is_md_client
            ,@is_mn_client
            ,@is_nc_client
            ,@is_nj_client
            ,@is_oh_client
            ,@is_wy_client
    FROM    pmi_admin.pmi_state AS st
    WHERE   st.state_id = @state_id
    AND     st.state_abbr in ('al','ca','co','ga','ky','md','mn','nc','nj','oh','wy');

    IF @is_al_client > 0 then
        select 'Calling etl_hst_al()' as proc_name;
        call etl_hst_al();
    ELSEIF @is_ca_client > 0 THEN 
        select 'Calling etl_hst_ca()' as proc_name;
        call etl_hst_ca();
    ELSEIF @is_co_client > 0 THEN 
        select 'Calling etl_hst_co()' as proc_name;
        call etl_hst_co(); 
    ELSEIF @is_fl_client > 0 THEN 
        select 'Calling etl_hst_fl()' as proc_name;
        call etl_hst_fl();     
    ELSEIF @is_ga_client > 0 THEN 
        select 'Calling etl_hst_ga()' as proc_name;
        call etl_hst_ga();
    ELSEIF @is_ky_client > 0 THEN 
        select 'Calling etl_hst_ky()' as proc_name;
        call etl_hst_ky();
    ELSEIF @is_md_client > 0 THEN 
        select 'Calling etl_hst_md()' as proc_name;
        call etl_hst_md();
    ELSEIF @is_mn_client > 0 THEN 
        select 'Calling etl_hst_mn()' as proc_name;
        call etl_hst_mn();
    ELSEIF @is_nc_client > 0 THEN 
        select 'Calling etl_hst_nc()' as proc_name;
        call etl_hst_nc();
    ELSEIF @is_nj_client > 0 THEN 
        select 'Calling etl_hst_nj()' as proc_name;
        call etl_hst_nj();
    ELSEIF @is_oh_client > 0 THEN 
        select 'Calling etl_hst_oh()' as proc_name;
        call etl_hst_oh();
    ELSEIF @is_wy_client > 0 THEN
        select 'Calling etl_hst_wy()' as proc_name;
        call etl_hst_wy();
    END IF;



end proc ;
//
