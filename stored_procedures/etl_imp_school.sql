/*
$Rev: 8474 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:05:05 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_school.sql $
$Id: etl_imp_school.sql 8474 2010-04-29 20:05:05Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_imp_school //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_school()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8474 $ $Date: 2010-04-29 16:05:05 -0400 (Thu, 29 Apr 2010) $'
BEGIN

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    SET @@session.max_error_count = 0;

    DROP TABLE IF EXISTS tmp_school;
    SET @@session.max_error_count = 64;
    
    CREATE TEMPORARY TABLE tmp_school (
        `school_code` varchar(20) NOT NULL,
        `school_state_code` varchar(20) NULL,
        `school_type_code` varchar(20) NULL,
        `moniker` varchar(100) DEFAULT NULL
    );
    
    INSERT INTO tmp_school (
        school_code,
        school_state_code,
        school_type_code,
        moniker
        )
    SELECT s.school_id
        ,s.school_state_code
        ,CASE upper(s.school_type)
            WHEN 'E'  THEN 'es'
            WHEN 'ES' THEN 'es'
            WHEN 'EM' THEN 'es'
            WHEN 'EL' THEN 'es'
            WHEN 'M'  THEN 'ms'
            WHEN 'MS' THEN 'ms'
            WHEN 'MD' THEN 'ms'
            WHEN 'H'  THEN 'hs'
            WHEN 'HS' THEN 'hs'
            WHEN 'HI' THEN 'hs'
            WHEN 'P'  THEN 'ps'
            WHEN 'MH' THEN 'mh'
            ELSE 'oth'
        END AS school_type_code
        ,CASE 
            WHEN s.school_name IS NOT NULL THEN s.school_name
            WHEN s.school_name IS NULL AND s.school_abbr IS NOT NULL THEN s.school_abbr
            WHEN s.school_abbr IS NULL THEN s.school_id
        END
    FROM v_pmi_ods_school AS s
    WHERE s.school_id IS NOT NULL
    ORDER BY school_name
    ;

        
    DROP TABLE IF EXISTS tmp_id_data_accessor;
    CREATE TABLE tmp_id_data_accessor (
        new_id int(11) not null,
        base_code varchar(20) not null,
        PRIMARY KEY  (`new_id`),
        UNIQUE KEY `uq_tmp_id_data_accessor` (`base_code`)
    );
    

    ### obtain a new id only for records not already in the target table.
    INSERT tmp_id_data_accessor (new_id, base_code)
    SELECT  pmi_admin.pmi_f_get_next_sequence('c_data_accessor', 1), ods.school_code
    FROM    tmp_school AS ods
    LEFT JOIN   c_data_accessor as tar
            ON      ods.school_code = tar.source_code
            AND     tar.accessor_type_code = 's'
    WHERE   tar.accessor_id IS NULL
    GROUP BY ods.school_code
    ;      
            
            
    insert into c_data_accessor (
        accessor_id, 
        accessor_type_code, 
        source_code, 
        client_id, 
        last_user_id 
    ) 
    select   coalesce(tmpid.new_id, da.accessor_id),
        's',
        tmp.school_code,
        @client_id,
        1234
    from    tmp_school AS tmp
    left join   tmp_id_data_accessor as tmpid
            on      tmp.school_code = tmpid.base_code
    left join   c_data_accessor as da
            on      da.source_code = tmp.school_code
            and     da.accessor_type_code = 's'
    on duplicate key update last_user_id = 1234
    ;

    
    
    INSERT INTO c_school (
        school_id,
        client_id,
        school_code,
        school_state_code, 
        moniker, 
        school_type_id,
        last_user_id, 
        create_timestamp
    ) 
    SELECT   da.accessor_id,
        @client_id,
        tmp.school_code,
        tmp.school_state_code,
        tmp.moniker,
        CASE 
            WHEN tmp.school_type_code IS NOT NULL
                THEN st.school_type_id
            ELSE ( CASE
                    WHEN tmp.moniker REGEXP 'middl.*high|high.*middl/gi' > 0 THEN ( SELECT school_type_id FROM c_school_type WHERE school_type_code = 'mh' )                            
                    WHEN instr(tmp.moniker, 'elem')  > 0 THEN ( SELECT school_type_id FROM c_school_type WHERE school_type_code = 'es' )
                    WHEN instr(tmp.moniker, 'middl') > 0 THEN ( SELECT school_type_id FROM c_school_type WHERE school_type_code = 'ms' )
                    WHEN instr(tmp.moniker, 'high')  > 0 THEN ( SELECT school_type_id FROM c_school_type WHERE school_type_code = 'hs' )
                    WHEN instr(tmp.moniker, 'primary') > 0 THEN ( SELECT school_type_id FROM c_school_type WHERE school_type_code = 'ps' )
                    ELSE ( SELECT school_type_id FROM c_school_type WHERE school_type_code = 'oth' ) END )
            END AS school_type_id,
        1234,
        current_timestamp
        FROM    tmp_school AS tmp
        JOIN    c_data_accessor AS da
                        ON      da.accessor_type_code = 's'
                        AND     da.source_code = tmp.school_code
        LEFT JOIN c_school_type as st
                        ON   st.school_type_code = tmp.school_type_code
        ON DUPLICATE KEY UPDATE last_user_id = 1234
            ,school_state_code = values(school_state_code)
            ,moniker = values(moniker)
            ,school_type_id = values(school_type_id)
    ;

    
    DROP TABLE IF EXISTS tmp_school;
    

END;
//
