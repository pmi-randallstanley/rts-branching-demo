DROP VIEW IF EXISTS v_c_generic_type_value_filter_list //
CREATE definer=`dbadmin`@`localhost`
SQL SECURITY INVOKER
VIEW v_c_generic_type_value_filter_list 
AS

/*
$Rev: 9146 $ 
$Author $ 
$Date: 2010-09-13 12:16:30 -0400 (Mon, 13 Sep 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/views/v_c_generic_type_value_filter_list.sql $
$Id: v_c_generic_type_value_filter_list.sql 9146 2010-09-13 16:16:30Z randall.stanley $ 
*/

    select  gt.generic_type_id
        ,gt.generic_type_code
        ,gt.moniker
        ,gt.classification_code
        ,gt.active_flag as type_active_flag
        ,gtvl.generic_type_value_id
        ,gtvl.value_text
        ,gtvl.value_sort_order
        ,gtvl.active_flag as type_value_active_flag
        ,gtfl.filter_id
    
    from    c_generic_type as gt
    join    c_generic_type_filter_list as gtfl
            on      gt.generic_type_id = gtfl.generic_type_id
    join    c_generic_type_value_list as gtvl
            on      gt.generic_type_id = gtvl.generic_type_id
    ;

//
