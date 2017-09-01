CREATE SCHEMA TPCDS_1T;


CREATE TABLE TPCDS_1T.dbgen_version
(
    dv_version varchar(16),
    dv_create_date date,
    dv_create_time time,
    dv_cmdline_args varchar(200)
);


CREATE TABLE TPCDS_1T.customer_address
(
    ca_address_sk int NOT NULL,
    ca_address_id char(16) NOT NULL,
    ca_street_number char(10),
    ca_street_name varchar(60),
    ca_street_type char(15),
    ca_suite_number char(10),
    ca_city varchar(60),
    ca_county varchar(30),
    ca_state char(2),
    ca_zip char(10),
    ca_country varchar(20),
    ca_gmt_offset numeric(5,2),
    ca_location_type char(20)
);


CREATE TABLE TPCDS_1T.customer_demographics
(
    cd_demo_sk int NOT NULL,
    cd_gender char(1),
    cd_marital_status char(1),
    cd_education_status char(20),
    cd_purchase_estimate int,
    cd_credit_rating char(10),
    cd_dep_count int,
    cd_dep_employed_count int,
    cd_dep_college_count int
);


CREATE TABLE TPCDS_1T.date_dim
(
    d_date_sk int NOT NULL,
    d_date_id char(16) NOT NULL,
    d_date date,
    d_month_seq int,
    d_week_seq int,
    d_quarter_seq int,
    d_year int,
    d_dow int,
    d_moy int,
    d_dom int,
    d_qoy int,
    d_fy_year int,
    d_fy_quarter_seq int,
    d_fy_week_seq int,
    d_day_name char(9),
    d_quarter_name char(6),
    d_holiday char(1),
    d_weekend char(1),
    d_following_holiday char(1),
    d_first_dom int,
    d_last_dom int,
    d_same_day_ly int,
    d_same_day_lq int,
    d_current_day char(1),
    d_current_week char(1),
    d_current_month char(1),
    d_current_quarter char(1),
    d_current_year char(1)
);


CREATE TABLE TPCDS_1T.warehouse
(
    w_warehouse_sk int NOT NULL,
    w_warehouse_id char(16) NOT NULL,
    w_warehouse_name varchar(20),
    w_warehouse_sq_ft int,
    w_street_number char(10),
    w_street_name varchar(60),
    w_street_type char(15),
    w_suite_number char(10),
    w_city varchar(60),
    w_county varchar(30),
    w_state char(2),
    w_zip char(10),
    w_country varchar(20),
    w_gmt_offset numeric(5,2)
);


CREATE TABLE TPCDS_1T.ship_mode
(
    sm_ship_mode_sk int NOT NULL,
    sm_ship_mode_id char(16) NOT NULL,
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20)
);


CREATE TABLE TPCDS_1T.time_dim
(
    t_time_sk int NOT NULL,
    t_time_id char(16) NOT NULL,
    t_time int,
    t_hour int,
    t_minute int,
    t_second int,
    t_am_pm char(2),
    t_shift char(20),
    t_sub_shift char(20),
    t_meal_time char(20)
);


CREATE TABLE TPCDS_1T.reason
(
    r_reason_sk int NOT NULL,
    r_reason_id char(16) NOT NULL,
    r_reason_desc char(100)
);


CREATE TABLE TPCDS_1T.income_band
(
    ib_income_band_sk int NOT NULL,
    ib_lower_bound int,
    ib_upper_bound int
);


CREATE TABLE TPCDS_1T.item
(
    i_item_sk int NOT NULL,
    i_item_id char(16) NOT NULL,
    i_rec_start_date date,
    i_rec_end_date date,
    i_item_desc varchar(200),
    i_current_price numeric(7,2),
    i_wholesale_cost numeric(7,2),
    i_brand_id int,
    i_brand char(50),
    i_class_id int,
    i_class char(50),
    i_category_id int,
    i_category char(50),
    i_manufact_id int,
    i_manufact char(50),
    i_size char(20),
    i_formulation char(20),
    i_color char(20),
    i_units char(10),
    i_container char(10),
    i_manager_id int,
    i_product_name char(50)
);


CREATE TABLE TPCDS_1T.store
(
    s_store_sk int NOT NULL,
    s_store_id char(16) NOT NULL,
    s_rec_start_date date,
    s_rec_end_date date,
    s_closed_date_sk int,
    s_store_name varchar(50),
    s_number_employees int,
    s_floor_space int,
    s_hours char(20),
    s_manager varchar(40),
    s_market_id int,
    s_geography_class varchar(100),
    s_market_desc varchar(100),
    s_market_manager varchar(40),
    s_division_id int,
    s_division_name varchar(50),
    s_company_id int,
    s_company_name varchar(50),
    s_street_number varchar(10),
    s_street_name varchar(60),
    s_street_type char(15),
    s_suite_number char(10),
    s_city varchar(60),
    s_county varchar(30),
    s_state char(2),
    s_zip char(10),
    s_country varchar(20),
    s_gmt_offset numeric(5,2),
    s_tax_precentage numeric(5,2)
);


CREATE TABLE TPCDS_1T.call_center
(
    cc_call_center_sk int NOT NULL,
    cc_call_center_id char(16) NOT NULL,
    cc_rec_start_date date,
    cc_rec_end_date date,
    cc_closed_date_sk int,
    cc_open_date_sk int,
    cc_name varchar(50),
    cc_class varchar(50),
    cc_employees int,
    cc_sq_ft int,
    cc_hours char(20),
    cc_manager varchar(40),
    cc_mkt_id int,
    cc_mkt_class char(50),
    cc_mkt_desc varchar(100),
    cc_market_manager varchar(40),
    cc_division int,
    cc_division_name varchar(50),
    cc_company int,
    cc_company_name char(50),
    cc_street_number char(10),
    cc_street_name varchar(60),
    cc_street_type char(15),
    cc_suite_number char(10),
    cc_city varchar(60),
    cc_county varchar(30),
    cc_state char(2),
    cc_zip char(10),
    cc_country varchar(20),
    cc_gmt_offset numeric(5,2),
    cc_tax_percentage numeric(5,2)
);


CREATE TABLE TPCDS_1T.customer
(
    c_customer_sk int NOT NULL,
    c_customer_id char(16) NOT NULL,
    c_current_cdemo_sk int,
    c_current_hdemo_sk int,
    c_current_addr_sk int,
    c_first_shipto_date_sk int,
    c_first_sales_date_sk int,
    c_salutation char(10),
    c_first_name char(20),
    c_last_name char(30),
    c_preferred_cust_flag char(1),
    c_birth_day int,
    c_birth_month int,
    c_birth_year int,
    c_birth_country varchar(20),
    c_login char(13),
    c_email_address char(50),
    c_last_review_date char(10)
);


CREATE TABLE TPCDS_1T.web_site
(
    web_site_sk int NOT NULL,
    web_site_id char(16) NOT NULL,
    web_rec_start_date date,
    web_rec_end_date date,
    web_name varchar(50),
    web_open_date_sk int,
    web_close_date_sk int,
    web_class varchar(50),
    web_manager varchar(40),
    web_mkt_id int,
    web_mkt_class varchar(50),
    web_mkt_desc varchar(100),
    web_market_manager varchar(40),
    web_company_id int,
    web_company_name char(50),
    web_street_number char(10),
    web_street_name varchar(60),
    web_street_type char(15),
    web_suite_number char(10),
    web_city varchar(60),
    web_county varchar(30),
    web_state char(2),
    web_zip char(10),
    web_country varchar(20),
    web_gmt_offset numeric(5,2),
    web_tax_percentage numeric(5,2)
);


CREATE TABLE TPCDS_1T.store_returns
(
    sr_returned_date_sk int,
    sr_return_time_sk int,
    sr_item_sk int NOT NULL,
    sr_customer_sk int,
    sr_cdemo_sk int,
    sr_hdemo_sk int,
    sr_addr_sk int,
    sr_store_sk int,
    sr_reason_sk int,
    sr_ticket_number int NOT NULL,
    sr_return_quantity int,
    sr_return_amt numeric(7,2),
    sr_return_tax numeric(7,2),
    sr_return_amt_inc_tax numeric(7,2),
    sr_fee numeric(7,2),
    sr_return_ship_cost numeric(7,2),
    sr_refunded_cash numeric(7,2),
    sr_reversed_charge numeric(7,2),
    sr_store_credit numeric(7,2),
    sr_net_loss numeric(7,2)
);


CREATE TABLE TPCDS_1T.household_demographics
(
    hd_demo_sk int NOT NULL,
    hd_income_band_sk int,
    hd_buy_potential char(15),
    hd_dep_count int,
    hd_vehicle_count int
);


CREATE TABLE TPCDS_1T.web_page
(
    wp_web_page_sk int NOT NULL,
    wp_web_page_id char(16) NOT NULL,
    wp_rec_start_date date,
    wp_rec_end_date date,
    wp_creation_date_sk int,
    wp_access_date_sk int,
    wp_autogen_flag char(1),
    wp_customer_sk int,
    wp_url varchar(100),
    wp_type char(50),
    wp_char_count int,
    wp_link_count int,
    wp_image_count int,
    wp_max_ad_count int
);


CREATE TABLE TPCDS_1T.promotion
(
    p_promo_sk int NOT NULL,
    p_promo_id char(16) NOT NULL,
    p_start_date_sk int,
    p_end_date_sk int,
    p_item_sk int,
    p_cost numeric(15,2),
    p_response_target int,
    p_promo_name char(50),
    p_channel_dmail char(1),
    p_channel_email char(1),
    p_channel_catalog char(1),
    p_channel_tv char(1),
    p_channel_radio char(1),
    p_channel_press char(1),
    p_channel_event char(1),
    p_channel_demo char(1),
    p_channel_details varchar(100),
    p_purpose char(15),
    p_discount_active char(1)
);


CREATE TABLE TPCDS_1T.catalog_page
(
    cp_catalog_page_sk int NOT NULL,
    cp_catalog_page_id char(16) NOT NULL,
    cp_start_date_sk int,
    cp_end_date_sk int,
    cp_department varchar(50),
    cp_catalog_number int,
    cp_catalog_page_number int,
    cp_description varchar(100),
    cp_type varchar(100)
);


CREATE TABLE TPCDS_1T.inventory
(
    inv_date_sk int NOT NULL,
    inv_item_sk int NOT NULL,
    inv_warehouse_sk int NOT NULL,
    inv_quantity_on_hand int
);


CREATE TABLE TPCDS_1T.catalog_returns
(
    cr_returned_date_sk int,
    cr_returned_time_sk int,
    cr_item_sk int NOT NULL,
    cr_refunded_customer_sk int,
    cr_refunded_cdemo_sk int,
    cr_refunded_hdemo_sk int,
    cr_refunded_addr_sk int,
    cr_returning_customer_sk int,
    cr_returning_cdemo_sk int,
    cr_returning_hdemo_sk int,
    cr_returning_addr_sk int,
    cr_call_center_sk int,
    cr_catalog_page_sk int,
    cr_ship_mode_sk int,
    cr_warehouse_sk int,
    cr_reason_sk int,
    cr_order_number int NOT NULL,
    cr_return_quantity int,
    cr_return_amount numeric(7,2),
    cr_return_tax numeric(7,2),
    cr_return_amt_inc_tax numeric(7,2),
    cr_fee numeric(7,2),
    cr_return_ship_cost numeric(7,2),
    cr_refunded_cash numeric(7,2),
    cr_reversed_charge numeric(7,2),
    cr_store_credit numeric(7,2),
    cr_net_loss numeric(7,2)
);


CREATE TABLE TPCDS_1T.web_returns
(
    wr_returned_date_sk int,
    wr_returned_time_sk int,
    wr_item_sk int NOT NULL,
    wr_refunded_customer_sk int,
    wr_refunded_cdemo_sk int,
    wr_refunded_hdemo_sk int,
    wr_refunded_addr_sk int,
    wr_returning_customer_sk int,
    wr_returning_cdemo_sk int,
    wr_returning_hdemo_sk int,
    wr_returning_addr_sk int,
    wr_web_page_sk int,
    wr_reason_sk int,
    wr_order_number int NOT NULL,
    wr_return_quantity int,
    wr_return_amt numeric(7,2),
    wr_return_tax numeric(7,2),
    wr_return_amt_inc_tax numeric(7,2),
    wr_fee numeric(7,2),
    wr_return_ship_cost numeric(7,2),
    wr_refunded_cash numeric(7,2),
    wr_reversed_charge numeric(7,2),
    wr_account_credit numeric(7,2),
    wr_net_loss numeric(7,2)
);


CREATE TABLE TPCDS_1T.web_sales
(
    ws_sold_date_sk int,
    ws_sold_time_sk int,
    ws_ship_date_sk int,
    ws_item_sk int NOT NULL,
    ws_bill_customer_sk int,
    ws_bill_cdemo_sk int,
    ws_bill_hdemo_sk int,
    ws_bill_addr_sk int,
    ws_ship_customer_sk int,
    ws_ship_cdemo_sk int,
    ws_ship_hdemo_sk int,
    ws_ship_addr_sk int,
    ws_web_page_sk int,
    ws_web_site_sk int,
    ws_ship_mode_sk int,
    ws_warehouse_sk int,
    ws_promo_sk int,
    ws_order_number int NOT NULL,
    ws_quantity int,
    ws_wholesale_cost numeric(7,2),
    ws_list_price numeric(7,2),
    ws_sales_price numeric(7,2),
    ws_ext_discount_amt numeric(7,2),
    ws_ext_sales_price numeric(7,2),
    ws_ext_wholesale_cost numeric(7,2),
    ws_ext_list_price numeric(7,2),
    ws_ext_tax numeric(7,2),
    ws_coupon_amt numeric(7,2),
    ws_ext_ship_cost numeric(7,2),
    ws_net_paid numeric(7,2),
    ws_net_paid_inc_tax numeric(7,2),
    ws_net_paid_inc_ship numeric(7,2),
    ws_net_paid_inc_ship_tax numeric(7,2),
    ws_net_profit numeric(7,2)
);


CREATE TABLE TPCDS_1T.catalog_sales
(
    cs_sold_date_sk int,
    cs_sold_time_sk int,
    cs_ship_date_sk int,
    cs_bill_customer_sk int,
    cs_bill_cdemo_sk int,
    cs_bill_hdemo_sk int,
    cs_bill_addr_sk int,
    cs_ship_customer_sk int,
    cs_ship_cdemo_sk int,
    cs_ship_hdemo_sk int,
    cs_ship_addr_sk int,
    cs_call_center_sk int,
    cs_catalog_page_sk int,
    cs_ship_mode_sk int,
    cs_warehouse_sk int,
    cs_item_sk int NOT NULL,
    cs_promo_sk int,
    cs_order_number int NOT NULL,
    cs_quantity int,
    cs_wholesale_cost numeric(7,2),
    cs_list_price numeric(7,2),
    cs_sales_price numeric(7,2),
    cs_ext_discount_amt numeric(7,2),
    cs_ext_sales_price numeric(7,2),
    cs_ext_wholesale_cost numeric(7,2),
    cs_ext_list_price numeric(7,2),
    cs_ext_tax numeric(7,2),
    cs_coupon_amt numeric(7,2),
    cs_ext_ship_cost numeric(7,2),
    cs_net_paid numeric(7,2),
    cs_net_paid_inc_tax numeric(7,2),
    cs_net_paid_inc_ship numeric(7,2),
    cs_net_paid_inc_ship_tax numeric(7,2),
    cs_net_profit numeric(7,2)
);


CREATE TABLE TPCDS_1T.store_sales
(
    ss_sold_date_sk int,
    ss_sold_time_sk int,
    ss_item_sk int NOT NULL,
    ss_customer_sk int,
    ss_cdemo_sk int,
    ss_hdemo_sk int,
    ss_addr_sk int,
    ss_store_sk int,
    ss_promo_sk int,
    ss_ticket_number int NOT NULL,
    ss_quantity int,
    ss_wholesale_cost numeric(7,2),
    ss_list_price numeric(7,2),
    ss_sales_price numeric(7,2),
    ss_ext_discount_amt numeric(7,2),
    ss_ext_sales_price numeric(7,2),
    ss_ext_wholesale_cost numeric(7,2),
    ss_ext_list_price numeric(7,2),
    ss_ext_tax numeric(7,2),
    ss_coupon_amt numeric(7,2),
    ss_net_paid numeric(7,2),
    ss_net_paid_inc_tax numeric(7,2),
    ss_net_profit numeric(7,2)
);

select mark_design_ksafe(1);

CREATE PROJECTION TPCDS_1T.dbgen_version_DBD_1_seg_test132_b0 /*+basename(dbgen_version_DBD_1_seg_test132),createtype(D)*/
(
 dv_version ENCODING AUTO, 
 dv_create_date ENCODING AUTO, 
 dv_create_time ENCODING AUTO, 
 dv_cmdline_args ENCODING AUTO
)
AS
 SELECT dv_version, 
        dv_create_date, 
        dv_create_time, 
        dv_cmdline_args
 FROM TPCDS_1T.dbgen_version 
 ORDER BY dv_version
SEGMENTED BY MODULARHASH (dv_version) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.dbgen_version_DBD_1_seg_test132_b1 /*+basename(dbgen_version_DBD_1_seg_test132),createtype(D)*/
(
 dv_version ENCODING AUTO, 
 dv_create_date ENCODING AUTO, 
 dv_create_time ENCODING AUTO, 
 dv_cmdline_args ENCODING AUTO
)
AS
 SELECT dv_version, 
        dv_create_date, 
        dv_create_time, 
        dv_cmdline_args
 FROM TPCDS_1T.dbgen_version 
 ORDER BY dv_version
SEGMENTED BY MODULARHASH (dv_version) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.customer_address_DBD_2_rep_test132 /*+createtype(D)*/
(
 ca_address_sk ENCODING DELTARANGE_COMP, 
 ca_address_id ENCODING AUTO, 
 ca_street_number ENCODING AUTO, 
 ca_street_name ENCODING AUTO, 
 ca_street_type ENCODING RLE, 
 ca_suite_number ENCODING AUTO, 
 ca_city ENCODING AUTO, 
 ca_county ENCODING AUTO, 
 ca_state ENCODING RLE, 
 ca_zip ENCODING AUTO, 
 ca_country ENCODING RLE, 
 ca_gmt_offset ENCODING RLE, 
 ca_location_type ENCODING RLE
)
AS
 SELECT ca_address_sk, 
        ca_address_id, 
        ca_street_number, 
        ca_street_name, 
        ca_street_type, 
        ca_suite_number, 
        ca_city, 
        ca_county, 
        ca_state, 
        ca_zip, 
        ca_country, 
        ca_gmt_offset, 
        ca_location_type
 FROM TPCDS_1T.customer_address 
 ORDER BY ca_state,
          ca_gmt_offset,
          ca_country,
          ca_location_type,
          ca_street_type,
          ca_address_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.customer_demographics_DBD_41_seg_test132_b0 /*+basename(customer_demographics_DBD_41_seg_test132),createtype(D)*/
(
 cd_demo_sk ENCODING COMMONDELTA_COMP, 
 cd_gender ENCODING AUTO, 
 cd_marital_status ENCODING AUTO, 
 cd_education_status ENCODING AUTO, 
 cd_purchase_estimate ENCODING COMMONDELTA_COMP, 
 cd_credit_rating ENCODING AUTO, 
 cd_dep_count ENCODING COMMONDELTA_COMP, 
 cd_dep_employed_count ENCODING RLE, 
 cd_dep_college_count ENCODING RLE
)
AS
 SELECT cd_demo_sk, 
        cd_gender, 
        cd_marital_status, 
        cd_education_status, 
        cd_purchase_estimate, 
        cd_credit_rating, 
        cd_dep_count, 
        cd_dep_employed_count, 
        cd_dep_college_count
 FROM TPCDS_1T.customer_demographics 
 ORDER BY cd_demo_sk
SEGMENTED BY HASH (cd_demo_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.customer_demographics_DBD_41_seg_test132_b1 /*+basename(customer_demographics_DBD_41_seg_test132),createtype(D)*/
(
 cd_demo_sk ENCODING COMMONDELTA_COMP, 
 cd_gender ENCODING AUTO, 
 cd_marital_status ENCODING AUTO, 
 cd_education_status ENCODING AUTO, 
 cd_purchase_estimate ENCODING COMMONDELTA_COMP, 
 cd_credit_rating ENCODING AUTO, 
 cd_dep_count ENCODING COMMONDELTA_COMP, 
 cd_dep_employed_count ENCODING RLE, 
 cd_dep_college_count ENCODING RLE
)
AS
 SELECT cd_demo_sk, 
        cd_gender, 
        cd_marital_status, 
        cd_education_status, 
        cd_purchase_estimate, 
        cd_credit_rating, 
        cd_dep_count, 
        cd_dep_employed_count, 
        cd_dep_college_count
 FROM TPCDS_1T.customer_demographics 
 ORDER BY cd_demo_sk
SEGMENTED BY HASH (cd_demo_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.date_dim_DBD_3_rep_test132 /*+createtype(D)*/
(
 d_date_sk ENCODING COMMONDELTA_COMP, 
 d_date_id ENCODING AUTO, 
 d_date ENCODING COMMONDELTA_COMP, 
 d_month_seq ENCODING COMMONDELTA_COMP, 
 d_week_seq ENCODING COMMONDELTA_COMP, 
 d_quarter_seq ENCODING COMMONDELTA_COMP, 
 d_year ENCODING RLE, 
 d_dow ENCODING COMMONDELTA_COMP, 
 d_moy ENCODING RLE, 
 d_dom ENCODING COMMONDELTA_COMP, 
 d_qoy ENCODING RLE, 
 d_fy_year ENCODING COMMONDELTA_COMP, 
 d_fy_quarter_seq ENCODING COMMONDELTA_COMP, 
 d_fy_week_seq ENCODING COMMONDELTA_COMP, 
 d_day_name ENCODING AUTO, 
 d_quarter_name ENCODING AUTO, 
 d_holiday ENCODING AUTO, 
 d_weekend ENCODING AUTO, 
 d_following_holiday ENCODING AUTO, 
 d_first_dom ENCODING COMMONDELTA_COMP, 
 d_last_dom ENCODING COMMONDELTA_COMP, 
 d_same_day_ly ENCODING COMMONDELTA_COMP, 
 d_same_day_lq ENCODING COMMONDELTA_COMP, 
 d_current_day ENCODING AUTO, 
 d_current_week ENCODING AUTO, 
 d_current_month ENCODING AUTO, 
 d_current_quarter ENCODING AUTO, 
 d_current_year ENCODING AUTO
)
AS
 SELECT d_date_sk, 
        d_date_id, 
        d_date, 
        d_month_seq, 
        d_week_seq, 
        d_quarter_seq, 
        d_year, 
        d_dow, 
        d_moy, 
        d_dom, 
        d_qoy, 
        d_fy_year, 
        d_fy_quarter_seq, 
        d_fy_week_seq, 
        d_day_name, 
        d_quarter_name, 
        d_holiday, 
        d_weekend, 
        d_following_holiday, 
        d_first_dom, 
        d_last_dom, 
        d_same_day_ly, 
        d_same_day_lq, 
        d_current_day, 
        d_current_week, 
        d_current_month, 
        d_current_quarter, 
        d_current_year
 FROM TPCDS_1T.date_dim 
 ORDER BY d_year,
          d_date_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.date_dim_DBD_42_seg_test132_b0 /*+basename(date_dim_DBD_42_seg_test132),createtype(D)*/
(
 d_date_sk ENCODING COMMONDELTA_COMP, 
 d_date_id ENCODING AUTO, 
 d_date ENCODING COMMONDELTA_COMP, 
 d_month_seq ENCODING COMMONDELTA_COMP, 
 d_week_seq ENCODING COMMONDELTA_COMP, 
 d_quarter_seq ENCODING COMMONDELTA_COMP, 
 d_year ENCODING COMMONDELTA_COMP, 
 d_dow ENCODING BLOCKDICT_COMP, 
 d_moy ENCODING COMMONDELTA_COMP, 
 d_dom ENCODING COMMONDELTA_COMP, 
 d_qoy ENCODING COMMONDELTA_COMP, 
 d_fy_year ENCODING COMMONDELTA_COMP, 
 d_fy_quarter_seq ENCODING COMMONDELTA_COMP, 
 d_fy_week_seq ENCODING COMMONDELTA_COMP, 
 d_day_name ENCODING AUTO, 
 d_quarter_name ENCODING AUTO, 
 d_holiday ENCODING AUTO, 
 d_weekend ENCODING AUTO, 
 d_following_holiday ENCODING AUTO, 
 d_first_dom ENCODING COMMONDELTA_COMP, 
 d_last_dom ENCODING COMMONDELTA_COMP, 
 d_same_day_ly ENCODING COMMONDELTA_COMP, 
 d_same_day_lq ENCODING COMMONDELTA_COMP, 
 d_current_day ENCODING AUTO, 
 d_current_week ENCODING AUTO, 
 d_current_month ENCODING AUTO, 
 d_current_quarter ENCODING AUTO, 
 d_current_year ENCODING AUTO
)
AS
 SELECT d_date_sk, 
        d_date_id, 
        d_date, 
        d_month_seq, 
        d_week_seq, 
        d_quarter_seq, 
        d_year, 
        d_dow, 
        d_moy, 
        d_dom, 
        d_qoy, 
        d_fy_year, 
        d_fy_quarter_seq, 
        d_fy_week_seq, 
        d_day_name, 
        d_quarter_name, 
        d_holiday, 
        d_weekend, 
        d_following_holiday, 
        d_first_dom, 
        d_last_dom, 
        d_same_day_ly, 
        d_same_day_lq, 
        d_current_day, 
        d_current_week, 
        d_current_month, 
        d_current_quarter, 
        d_current_year
 FROM TPCDS_1T.date_dim 
 ORDER BY d_date_sk
SEGMENTED BY HASH (d_date_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.date_dim_DBD_42_seg_test132_b1 /*+basename(date_dim_DBD_42_seg_test132),createtype(D)*/
(
 d_date_sk ENCODING COMMONDELTA_COMP, 
 d_date_id ENCODING AUTO, 
 d_date ENCODING COMMONDELTA_COMP, 
 d_month_seq ENCODING COMMONDELTA_COMP, 
 d_week_seq ENCODING COMMONDELTA_COMP, 
 d_quarter_seq ENCODING COMMONDELTA_COMP, 
 d_year ENCODING COMMONDELTA_COMP, 
 d_dow ENCODING BLOCKDICT_COMP, 
 d_moy ENCODING COMMONDELTA_COMP, 
 d_dom ENCODING COMMONDELTA_COMP, 
 d_qoy ENCODING COMMONDELTA_COMP, 
 d_fy_year ENCODING COMMONDELTA_COMP, 
 d_fy_quarter_seq ENCODING COMMONDELTA_COMP, 
 d_fy_week_seq ENCODING COMMONDELTA_COMP, 
 d_day_name ENCODING AUTO, 
 d_quarter_name ENCODING AUTO, 
 d_holiday ENCODING AUTO, 
 d_weekend ENCODING AUTO, 
 d_following_holiday ENCODING AUTO, 
 d_first_dom ENCODING COMMONDELTA_COMP, 
 d_last_dom ENCODING COMMONDELTA_COMP, 
 d_same_day_ly ENCODING COMMONDELTA_COMP, 
 d_same_day_lq ENCODING COMMONDELTA_COMP, 
 d_current_day ENCODING AUTO, 
 d_current_week ENCODING AUTO, 
 d_current_month ENCODING AUTO, 
 d_current_quarter ENCODING AUTO, 
 d_current_year ENCODING AUTO
)
AS
 SELECT d_date_sk, 
        d_date_id, 
        d_date, 
        d_month_seq, 
        d_week_seq, 
        d_quarter_seq, 
        d_year, 
        d_dow, 
        d_moy, 
        d_dom, 
        d_qoy, 
        d_fy_year, 
        d_fy_quarter_seq, 
        d_fy_week_seq, 
        d_day_name, 
        d_quarter_name, 
        d_holiday, 
        d_weekend, 
        d_following_holiday, 
        d_first_dom, 
        d_last_dom, 
        d_same_day_ly, 
        d_same_day_lq, 
        d_current_day, 
        d_current_week, 
        d_current_month, 
        d_current_quarter, 
        d_current_year
 FROM TPCDS_1T.date_dim 
 ORDER BY d_date_sk
SEGMENTED BY HASH (d_date_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.ship_mode_DBD_4_rep_test132 /*+createtype(D)*/
(
 sm_ship_mode_sk ENCODING COMMONDELTA_COMP, 
 sm_ship_mode_id ENCODING AUTO, 
 sm_type ENCODING AUTO, 
 sm_code ENCODING AUTO, 
 sm_carrier ENCODING AUTO, 
 sm_contract ENCODING AUTO
)
AS
 SELECT sm_ship_mode_sk, 
        sm_ship_mode_id, 
        sm_type, 
        sm_code, 
        sm_carrier, 
        sm_contract
 FROM TPCDS_1T.ship_mode 
 ORDER BY sm_ship_mode_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.time_dim_DBD_5_rep_test132 /*+createtype(D)*/
(
 t_time_sk ENCODING COMMONDELTA_COMP, 
 t_time_id ENCODING AUTO, 
 t_time ENCODING COMMONDELTA_COMP, 
 t_hour ENCODING RLE, 
 t_minute ENCODING RLE, 
 t_second ENCODING COMMONDELTA_COMP, 
 t_am_pm ENCODING RLE, 
 t_shift ENCODING RLE, 
 t_sub_shift ENCODING RLE, 
 t_meal_time ENCODING RLE
)
AS
 SELECT t_time_sk, 
        t_time_id, 
        t_time, 
        t_hour, 
        t_minute, 
        t_second, 
        t_am_pm, 
        t_shift, 
        t_sub_shift, 
        t_meal_time
 FROM TPCDS_1T.time_dim 
 ORDER BY t_am_pm,
          t_shift,
          t_sub_shift,
          t_meal_time,
          t_hour,
          t_time_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.reason_DBD_6_rep_test132 /*+createtype(D)*/
(
 r_reason_sk ENCODING COMMONDELTA_COMP, 
 r_reason_id ENCODING AUTO, 
 r_reason_desc ENCODING AUTO
)
AS
 SELECT r_reason_sk, 
        r_reason_id, 
        r_reason_desc
 FROM TPCDS_1T.reason 
 ORDER BY r_reason_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.income_band_DBD_7_rep_test132 /*+createtype(D)*/
(
 ib_income_band_sk ENCODING COMMONDELTA_COMP, 
 ib_lower_bound ENCODING COMMONDELTA_COMP, 
 ib_upper_bound ENCODING COMMONDELTA_COMP
)
AS
 SELECT ib_income_band_sk, 
        ib_lower_bound, 
        ib_upper_bound
 FROM TPCDS_1T.income_band 
 ORDER BY ib_income_band_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.item_DBD_8_rep_test132 /*+createtype(D)*/
(
 i_item_sk ENCODING COMMONDELTA_COMP, 
 i_item_id ENCODING AUTO, 
 i_rec_start_date ENCODING BLOCKDICT_COMP, 
 i_rec_end_date ENCODING BLOCKDICT_COMP, 
 i_item_desc ENCODING AUTO, 
 i_current_price ENCODING DELTARANGE_COMP, 
 i_wholesale_cost ENCODING DELTARANGE_COMP, 
 i_brand_id ENCODING BLOCKDICT_COMP, 
 i_brand ENCODING AUTO, 
 i_class_id ENCODING BLOCKDICT_COMP, 
 i_class ENCODING AUTO, 
 i_category_id ENCODING RLE, 
 i_category ENCODING RLE, 
 i_manufact_id ENCODING DELTAVAL, 
 i_manufact ENCODING AUTO, 
 i_size ENCODING AUTO, 
 i_formulation ENCODING AUTO, 
 i_color ENCODING AUTO, 
 i_units ENCODING AUTO, 
 i_container ENCODING AUTO, 
 i_manager_id ENCODING DELTAVAL, 
 i_product_name ENCODING AUTO
)
AS
 SELECT i_item_sk, 
        i_item_id, 
        i_rec_start_date, 
        i_rec_end_date, 
        i_item_desc, 
        i_current_price, 
        i_wholesale_cost, 
        i_brand_id, 
        i_brand, 
        i_class_id, 
        i_class, 
        i_category_id, 
        i_category, 
        i_manufact_id, 
        i_manufact, 
        i_size, 
        i_formulation, 
        i_color, 
        i_units, 
        i_container, 
        i_manager_id, 
        i_product_name
 FROM TPCDS_1T.item 
 ORDER BY i_category,
          i_item_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.item_DBD_9_rep_test132 /*+createtype(D)*/
(
 i_item_sk ENCODING DELTAVAL, 
 i_item_id ENCODING AUTO, 
 i_current_price ENCODING RLE
)
AS
 SELECT i_item_sk, 
        i_item_id, 
        i_current_price
 FROM TPCDS_1T.item 
 ORDER BY i_current_price,
          i_item_id
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.item_DBD_44_seg_test132_b0 /*+basename(item_DBD_44_seg_test132),createtype(D)*/
(
 i_item_sk ENCODING COMMONDELTA_COMP, 
 i_item_id ENCODING AUTO, 
 i_rec_start_date ENCODING BLOCKDICT_COMP, 
 i_rec_end_date ENCODING BLOCKDICT_COMP, 
 i_item_desc ENCODING AUTO, 
 i_current_price ENCODING DELTARANGE_COMP, 
 i_wholesale_cost ENCODING DELTARANGE_COMP, 
 i_brand_id ENCODING BLOCKDICT_COMP, 
 i_brand ENCODING AUTO, 
 i_class_id ENCODING BLOCKDICT_COMP, 
 i_class ENCODING AUTO, 
 i_category_id ENCODING BLOCKDICT_COMP, 
 i_category ENCODING AUTO, 
 i_manufact_id ENCODING DELTAVAL, 
 i_manufact ENCODING AUTO, 
 i_size ENCODING AUTO, 
 i_formulation ENCODING AUTO, 
 i_color ENCODING AUTO, 
 i_units ENCODING AUTO, 
 i_container ENCODING AUTO, 
 i_manager_id ENCODING DELTAVAL, 
 i_product_name ENCODING AUTO
)
AS
 SELECT i_item_sk, 
        i_item_id, 
        i_rec_start_date, 
        i_rec_end_date, 
        i_item_desc, 
        i_current_price, 
        i_wholesale_cost, 
        i_brand_id, 
        i_brand, 
        i_class_id, 
        i_class, 
        i_category_id, 
        i_category, 
        i_manufact_id, 
        i_manufact, 
        i_size, 
        i_formulation, 
        i_color, 
        i_units, 
        i_container, 
        i_manager_id, 
        i_product_name
 FROM TPCDS_1T.item 
 ORDER BY i_item_sk
SEGMENTED BY HASH (i_item_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.item_DBD_44_seg_test132_b1 /*+basename(item_DBD_44_seg_test132),createtype(D)*/
(
 i_item_sk ENCODING COMMONDELTA_COMP, 
 i_item_id ENCODING AUTO, 
 i_rec_start_date ENCODING BLOCKDICT_COMP, 
 i_rec_end_date ENCODING BLOCKDICT_COMP, 
 i_item_desc ENCODING AUTO, 
 i_current_price ENCODING DELTARANGE_COMP, 
 i_wholesale_cost ENCODING DELTARANGE_COMP, 
 i_brand_id ENCODING BLOCKDICT_COMP, 
 i_brand ENCODING AUTO, 
 i_class_id ENCODING BLOCKDICT_COMP, 
 i_class ENCODING AUTO, 
 i_category_id ENCODING BLOCKDICT_COMP, 
 i_category ENCODING AUTO, 
 i_manufact_id ENCODING DELTAVAL, 
 i_manufact ENCODING AUTO, 
 i_size ENCODING AUTO, 
 i_formulation ENCODING AUTO, 
 i_color ENCODING AUTO, 
 i_units ENCODING AUTO, 
 i_container ENCODING AUTO, 
 i_manager_id ENCODING DELTAVAL, 
 i_product_name ENCODING AUTO
)
AS
 SELECT i_item_sk, 
        i_item_id, 
        i_rec_start_date, 
        i_rec_end_date, 
        i_item_desc, 
        i_current_price, 
        i_wholesale_cost, 
        i_brand_id, 
        i_brand, 
        i_class_id, 
        i_class, 
        i_category_id, 
        i_category, 
        i_manufact_id, 
        i_manufact, 
        i_size, 
        i_formulation, 
        i_color, 
        i_units, 
        i_container, 
        i_manager_id, 
        i_product_name
 FROM TPCDS_1T.item 
 ORDER BY i_item_sk
SEGMENTED BY HASH (i_item_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.store_DBD_45_seg_test132_b0 /*+basename(store_DBD_45_seg_test132),createtype(D)*/
(
 s_store_sk ENCODING AUTO, 
 s_store_id ENCODING AUTO, 
 s_rec_start_date ENCODING AUTO, 
 s_rec_end_date ENCODING BLOCKDICT_COMP, 
 s_closed_date_sk ENCODING BLOCKDICT_COMP, 
 s_store_name ENCODING AUTO, 
 s_number_employees ENCODING AUTO, 
 s_floor_space ENCODING AUTO, 
 s_hours ENCODING AUTO, 
 s_manager ENCODING AUTO, 
 s_market_id ENCODING AUTO, 
 s_geography_class ENCODING AUTO, 
 s_market_desc ENCODING AUTO, 
 s_market_manager ENCODING AUTO, 
 s_division_id ENCODING COMMONDELTA_COMP, 
 s_division_name ENCODING AUTO, 
 s_company_id ENCODING COMMONDELTA_COMP, 
 s_company_name ENCODING AUTO, 
 s_street_number ENCODING AUTO, 
 s_street_name ENCODING AUTO, 
 s_street_type ENCODING AUTO, 
 s_suite_number ENCODING AUTO, 
 s_city ENCODING AUTO, 
 s_county ENCODING AUTO, 
 s_state ENCODING AUTO, 
 s_zip ENCODING AUTO, 
 s_country ENCODING AUTO, 
 s_gmt_offset ENCODING COMMONDELTA_COMP, 
 s_tax_precentage ENCODING AUTO
)
AS
 SELECT s_store_sk, 
        s_store_id, 
        s_rec_start_date, 
        s_rec_end_date, 
        s_closed_date_sk, 
        s_store_name, 
        s_number_employees, 
        s_floor_space, 
        s_hours, 
        s_manager, 
        s_market_id, 
        s_geography_class, 
        s_market_desc, 
        s_market_manager, 
        s_division_id, 
        s_division_name, 
        s_company_id, 
        s_company_name, 
        s_street_number, 
        s_street_name, 
        s_street_type, 
        s_suite_number, 
        s_city, 
        s_county, 
        s_state, 
        s_zip, 
        s_country, 
        s_gmt_offset, 
        s_tax_precentage
 FROM TPCDS_1T.store 
 ORDER BY s_store_sk
SEGMENTED BY HASH (s_store_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.store_DBD_45_seg_test132_b1 /*+basename(store_DBD_45_seg_test132),createtype(D)*/
(
 s_store_sk ENCODING AUTO, 
 s_store_id ENCODING AUTO, 
 s_rec_start_date ENCODING AUTO, 
 s_rec_end_date ENCODING BLOCKDICT_COMP, 
 s_closed_date_sk ENCODING BLOCKDICT_COMP, 
 s_store_name ENCODING AUTO, 
 s_number_employees ENCODING AUTO, 
 s_floor_space ENCODING AUTO, 
 s_hours ENCODING AUTO, 
 s_manager ENCODING AUTO, 
 s_market_id ENCODING AUTO, 
 s_geography_class ENCODING AUTO, 
 s_market_desc ENCODING AUTO, 
 s_market_manager ENCODING AUTO, 
 s_division_id ENCODING COMMONDELTA_COMP, 
 s_division_name ENCODING AUTO, 
 s_company_id ENCODING COMMONDELTA_COMP, 
 s_company_name ENCODING AUTO, 
 s_street_number ENCODING AUTO, 
 s_street_name ENCODING AUTO, 
 s_street_type ENCODING AUTO, 
 s_suite_number ENCODING AUTO, 
 s_city ENCODING AUTO, 
 s_county ENCODING AUTO, 
 s_state ENCODING AUTO, 
 s_zip ENCODING AUTO, 
 s_country ENCODING AUTO, 
 s_gmt_offset ENCODING COMMONDELTA_COMP, 
 s_tax_precentage ENCODING AUTO
)
AS
 SELECT s_store_sk, 
        s_store_id, 
        s_rec_start_date, 
        s_rec_end_date, 
        s_closed_date_sk, 
        s_store_name, 
        s_number_employees, 
        s_floor_space, 
        s_hours, 
        s_manager, 
        s_market_id, 
        s_geography_class, 
        s_market_desc, 
        s_market_manager, 
        s_division_id, 
        s_division_name, 
        s_company_id, 
        s_company_name, 
        s_street_number, 
        s_street_name, 
        s_street_type, 
        s_suite_number, 
        s_city, 
        s_county, 
        s_state, 
        s_zip, 
        s_country, 
        s_gmt_offset, 
        s_tax_precentage
 FROM TPCDS_1T.store 
 ORDER BY s_store_sk
SEGMENTED BY HASH (s_store_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.customer_DBD_10_rep_test132 /*+createtype(D)*/
(
 c_customer_sk ENCODING COMMONDELTA_COMP, 
 c_customer_id ENCODING AUTO, 
 c_current_cdemo_sk ENCODING DELTAVAL, 
 c_current_hdemo_sk ENCODING DELTAVAL, 
 c_current_addr_sk ENCODING DELTAVAL, 
 c_first_shipto_date_sk ENCODING DELTAVAL, 
 c_first_sales_date_sk ENCODING DELTAVAL, 
 c_salutation ENCODING AUTO, 
 c_first_name ENCODING AUTO, 
 c_last_name ENCODING AUTO, 
 c_preferred_cust_flag ENCODING AUTO, 
 c_birth_day ENCODING DELTAVAL, 
 c_birth_month ENCODING BLOCKDICT_COMP, 
 c_birth_year ENCODING BLOCKDICT_COMP, 
 c_birth_country ENCODING AUTO, 
 c_login ENCODING AUTO, 
 c_email_address ENCODING AUTO, 
 c_last_review_date ENCODING AUTO
)
AS
 SELECT c_customer_sk, 
        c_customer_id, 
        c_current_cdemo_sk, 
        c_current_hdemo_sk, 
        c_current_addr_sk, 
        c_first_shipto_date_sk, 
        c_first_sales_date_sk, 
        c_salutation, 
        c_first_name, 
        c_last_name, 
        c_preferred_cust_flag, 
        c_birth_day, 
        c_birth_month, 
        c_birth_year, 
        c_birth_country, 
        c_login, 
        c_email_address, 
        c_last_review_date
 FROM TPCDS_1T.customer 
 ORDER BY c_customer_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.customer_DBD_47_seg_test132_b0 /*+basename(customer_DBD_47_seg_test132),createtype(D)*/
(
 c_customer_sk ENCODING COMMONDELTA_COMP, 
 c_customer_id ENCODING AUTO, 
 c_current_cdemo_sk ENCODING DELTAVAL, 
 c_current_hdemo_sk ENCODING DELTAVAL, 
 c_current_addr_sk ENCODING DELTAVAL, 
 c_first_shipto_date_sk ENCODING DELTAVAL, 
 c_first_sales_date_sk ENCODING DELTAVAL, 
 c_salutation ENCODING AUTO, 
 c_first_name ENCODING AUTO, 
 c_last_name ENCODING AUTO, 
 c_preferred_cust_flag ENCODING AUTO, 
 c_birth_day ENCODING DELTAVAL, 
 c_birth_month ENCODING BLOCKDICT_COMP, 
 c_birth_year ENCODING BLOCKDICT_COMP, 
 c_birth_country ENCODING AUTO, 
 c_login ENCODING AUTO, 
 c_email_address ENCODING AUTO, 
 c_last_review_date ENCODING AUTO
)
AS
 SELECT c_customer_sk, 
        c_customer_id, 
        c_current_cdemo_sk, 
        c_current_hdemo_sk, 
        c_current_addr_sk, 
        c_first_shipto_date_sk, 
        c_first_sales_date_sk, 
        c_salutation, 
        c_first_name, 
        c_last_name, 
        c_preferred_cust_flag, 
        c_birth_day, 
        c_birth_month, 
        c_birth_year, 
        c_birth_country, 
        c_login, 
        c_email_address, 
        c_last_review_date
 FROM TPCDS_1T.customer 
 ORDER BY c_customer_sk
SEGMENTED BY HASH (c_customer_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.customer_DBD_47_seg_test132_b1 /*+basename(customer_DBD_47_seg_test132),createtype(D)*/
(
 c_customer_sk ENCODING COMMONDELTA_COMP, 
 c_customer_id ENCODING AUTO, 
 c_current_cdemo_sk ENCODING DELTAVAL, 
 c_current_hdemo_sk ENCODING DELTAVAL, 
 c_current_addr_sk ENCODING DELTAVAL, 
 c_first_shipto_date_sk ENCODING DELTAVAL, 
 c_first_sales_date_sk ENCODING DELTAVAL, 
 c_salutation ENCODING AUTO, 
 c_first_name ENCODING AUTO, 
 c_last_name ENCODING AUTO, 
 c_preferred_cust_flag ENCODING AUTO, 
 c_birth_day ENCODING DELTAVAL, 
 c_birth_month ENCODING BLOCKDICT_COMP, 
 c_birth_year ENCODING BLOCKDICT_COMP, 
 c_birth_country ENCODING AUTO, 
 c_login ENCODING AUTO, 
 c_email_address ENCODING AUTO, 
 c_last_review_date ENCODING AUTO
)
AS
 SELECT c_customer_sk, 
        c_customer_id, 
        c_current_cdemo_sk, 
        c_current_hdemo_sk, 
        c_current_addr_sk, 
        c_first_shipto_date_sk, 
        c_first_sales_date_sk, 
        c_salutation, 
        c_first_name, 
        c_last_name, 
        c_preferred_cust_flag, 
        c_birth_day, 
        c_birth_month, 
        c_birth_year, 
        c_birth_country, 
        c_login, 
        c_email_address, 
        c_last_review_date
 FROM TPCDS_1T.customer 
 ORDER BY c_customer_sk
SEGMENTED BY HASH (c_customer_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.web_site_DBD_11_rep_test132 /*+createtype(D)*/
(
 web_site_sk ENCODING COMMONDELTA_COMP, 
 web_site_id ENCODING AUTO, 
 web_rec_start_date ENCODING RLE, 
 web_rec_end_date ENCODING BLOCKDICT_COMP, 
 web_name ENCODING AUTO, 
 web_open_date_sk ENCODING COMMONDELTA_COMP, 
 web_close_date_sk ENCODING DELTAVAL, 
 web_class ENCODING RLE, 
 web_manager ENCODING AUTO, 
 web_mkt_id ENCODING COMMONDELTA_COMP, 
 web_mkt_class ENCODING AUTO, 
 web_mkt_desc ENCODING AUTO, 
 web_market_manager ENCODING AUTO, 
 web_company_id ENCODING AUTO, 
 web_company_name ENCODING AUTO, 
 web_street_number ENCODING AUTO, 
 web_street_name ENCODING AUTO, 
 web_street_type ENCODING AUTO, 
 web_suite_number ENCODING AUTO, 
 web_city ENCODING AUTO, 
 web_county ENCODING AUTO, 
 web_state ENCODING AUTO, 
 web_zip ENCODING AUTO, 
 web_country ENCODING RLE, 
 web_gmt_offset ENCODING COMMONDELTA_COMP, 
 web_tax_percentage ENCODING AUTO
)
AS
 SELECT web_site_sk, 
        web_site_id, 
        web_rec_start_date, 
        web_rec_end_date, 
        web_name, 
        web_open_date_sk, 
        web_close_date_sk, 
        web_class, 
        web_manager, 
        web_mkt_id, 
        web_mkt_class, 
        web_mkt_desc, 
        web_market_manager, 
        web_company_id, 
        web_company_name, 
        web_street_number, 
        web_street_name, 
        web_street_type, 
        web_suite_number, 
        web_city, 
        web_county, 
        web_state, 
        web_zip, 
        web_country, 
        web_gmt_offset, 
        web_tax_percentage
 FROM TPCDS_1T.web_site 
 ORDER BY web_class,
          web_country,
          web_rec_start_date,
          web_site_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.store_returns_DBD_12_seg_test132_b0 /*+basename(store_returns_DBD_12_seg_test132),createtype(D)*/
(
 sr_returned_date_sk ENCODING DELTAVAL, 
 sr_return_time_sk ENCODING DELTAVAL, 
 sr_item_sk ENCODING RLE, 
 sr_customer_sk ENCODING DELTAVAL, 
 sr_cdemo_sk ENCODING DELTAVAL, 
 sr_hdemo_sk ENCODING DELTAVAL, 
 sr_addr_sk ENCODING DELTAVAL, 
 sr_store_sk ENCODING BLOCKDICT_COMP, 
 sr_reason_sk ENCODING BLOCKDICT_COMP, 
 sr_ticket_number ENCODING DELTARANGE_COMP, 
 sr_return_quantity ENCODING BLOCKDICT_COMP, 
 sr_return_amt ENCODING DELTAVAL, 
 sr_return_tax ENCODING DELTARANGE_COMP, 
 sr_return_amt_inc_tax ENCODING DELTAVAL, 
 sr_fee ENCODING DELTAVAL, 
 sr_return_ship_cost ENCODING DELTAVAL, 
 sr_refunded_cash ENCODING DELTARANGE_COMP, 
 sr_reversed_charge ENCODING DELTARANGE_COMP, 
 sr_store_credit ENCODING DELTARANGE_COMP, 
 sr_net_loss ENCODING DELTAVAL
)
AS
 SELECT sr_returned_date_sk, 
        sr_return_time_sk, 
        sr_item_sk, 
        sr_customer_sk, 
        sr_cdemo_sk, 
        sr_hdemo_sk, 
        sr_addr_sk, 
        sr_store_sk, 
        sr_reason_sk, 
        sr_ticket_number, 
        sr_return_quantity, 
        sr_return_amt, 
        sr_return_tax, 
        sr_return_amt_inc_tax, 
        sr_fee, 
        sr_return_ship_cost, 
        sr_refunded_cash, 
        sr_reversed_charge, 
        sr_store_credit, 
        sr_net_loss
 FROM TPCDS_1T.store_returns 
 ORDER BY sr_item_sk,
          sr_ticket_number
SEGMENTED BY MODULARHASH (sr_item_sk, sr_ticket_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.store_returns_DBD_12_seg_test132_b1 /*+basename(store_returns_DBD_12_seg_test132),createtype(D)*/
(
 sr_returned_date_sk ENCODING DELTAVAL, 
 sr_return_time_sk ENCODING DELTAVAL, 
 sr_item_sk ENCODING RLE, 
 sr_customer_sk ENCODING DELTAVAL, 
 sr_cdemo_sk ENCODING DELTAVAL, 
 sr_hdemo_sk ENCODING DELTAVAL, 
 sr_addr_sk ENCODING DELTAVAL, 
 sr_store_sk ENCODING BLOCKDICT_COMP, 
 sr_reason_sk ENCODING BLOCKDICT_COMP, 
 sr_ticket_number ENCODING DELTARANGE_COMP, 
 sr_return_quantity ENCODING BLOCKDICT_COMP, 
 sr_return_amt ENCODING DELTAVAL, 
 sr_return_tax ENCODING DELTARANGE_COMP, 
 sr_return_amt_inc_tax ENCODING DELTAVAL, 
 sr_fee ENCODING DELTAVAL, 
 sr_return_ship_cost ENCODING DELTAVAL, 
 sr_refunded_cash ENCODING DELTARANGE_COMP, 
 sr_reversed_charge ENCODING DELTARANGE_COMP, 
 sr_store_credit ENCODING DELTARANGE_COMP, 
 sr_net_loss ENCODING DELTAVAL
)
AS
 SELECT sr_returned_date_sk, 
        sr_return_time_sk, 
        sr_item_sk, 
        sr_customer_sk, 
        sr_cdemo_sk, 
        sr_hdemo_sk, 
        sr_addr_sk, 
        sr_store_sk, 
        sr_reason_sk, 
        sr_ticket_number, 
        sr_return_quantity, 
        sr_return_amt, 
        sr_return_tax, 
        sr_return_amt_inc_tax, 
        sr_fee, 
        sr_return_ship_cost, 
        sr_refunded_cash, 
        sr_reversed_charge, 
        sr_store_credit, 
        sr_net_loss
 FROM TPCDS_1T.store_returns 
 ORDER BY sr_item_sk,
          sr_ticket_number
SEGMENTED BY MODULARHASH (sr_item_sk, sr_ticket_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.store_returns_DBD_48_seg_test132_b0 /*+basename(store_returns_DBD_48_seg_test132),createtype(D)*/
(
 sr_returned_date_sk ENCODING DELTAVAL, 
 sr_return_time_sk ENCODING DELTAVAL, 
 sr_item_sk ENCODING RLE, 
 sr_customer_sk ENCODING DELTAVAL, 
 sr_cdemo_sk ENCODING DELTAVAL, 
 sr_hdemo_sk ENCODING DELTAVAL, 
 sr_addr_sk ENCODING DELTAVAL, 
 sr_store_sk ENCODING BLOCKDICT_COMP, 
 sr_reason_sk ENCODING BLOCKDICT_COMP, 
 sr_ticket_number ENCODING DELTARANGE_COMP, 
 sr_return_quantity ENCODING BLOCKDICT_COMP, 
 sr_return_amt ENCODING DELTAVAL, 
 sr_return_tax ENCODING DELTARANGE_COMP, 
 sr_return_amt_inc_tax ENCODING DELTAVAL, 
 sr_fee ENCODING DELTAVAL, 
 sr_return_ship_cost ENCODING DELTAVAL, 
 sr_refunded_cash ENCODING DELTARANGE_COMP, 
 sr_reversed_charge ENCODING DELTARANGE_COMP, 
 sr_store_credit ENCODING DELTARANGE_COMP, 
 sr_net_loss ENCODING DELTAVAL
)
AS
 SELECT sr_returned_date_sk, 
        sr_return_time_sk, 
        sr_item_sk, 
        sr_customer_sk, 
        sr_cdemo_sk, 
        sr_hdemo_sk, 
        sr_addr_sk, 
        sr_store_sk, 
        sr_reason_sk, 
        sr_ticket_number, 
        sr_return_quantity, 
        sr_return_amt, 
        sr_return_tax, 
        sr_return_amt_inc_tax, 
        sr_fee, 
        sr_return_ship_cost, 
        sr_refunded_cash, 
        sr_reversed_charge, 
        sr_store_credit, 
        sr_net_loss
 FROM TPCDS_1T.store_returns 
 ORDER BY sr_item_sk,
          sr_ticket_number
SEGMENTED BY HASH (sr_item_sk, sr_ticket_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.store_returns_DBD_48_seg_test132_b1 /*+basename(store_returns_DBD_48_seg_test132),createtype(D)*/
(
 sr_returned_date_sk ENCODING DELTAVAL, 
 sr_return_time_sk ENCODING DELTAVAL, 
 sr_item_sk ENCODING RLE, 
 sr_customer_sk ENCODING DELTAVAL, 
 sr_cdemo_sk ENCODING DELTAVAL, 
 sr_hdemo_sk ENCODING DELTAVAL, 
 sr_addr_sk ENCODING DELTAVAL, 
 sr_store_sk ENCODING BLOCKDICT_COMP, 
 sr_reason_sk ENCODING BLOCKDICT_COMP, 
 sr_ticket_number ENCODING DELTARANGE_COMP, 
 sr_return_quantity ENCODING BLOCKDICT_COMP, 
 sr_return_amt ENCODING DELTAVAL, 
 sr_return_tax ENCODING DELTARANGE_COMP, 
 sr_return_amt_inc_tax ENCODING DELTAVAL, 
 sr_fee ENCODING DELTAVAL, 
 sr_return_ship_cost ENCODING DELTAVAL, 
 sr_refunded_cash ENCODING DELTARANGE_COMP, 
 sr_reversed_charge ENCODING DELTARANGE_COMP, 
 sr_store_credit ENCODING DELTARANGE_COMP, 
 sr_net_loss ENCODING DELTAVAL
)
AS
 SELECT sr_returned_date_sk, 
        sr_return_time_sk, 
        sr_item_sk, 
        sr_customer_sk, 
        sr_cdemo_sk, 
        sr_hdemo_sk, 
        sr_addr_sk, 
        sr_store_sk, 
        sr_reason_sk, 
        sr_ticket_number, 
        sr_return_quantity, 
        sr_return_amt, 
        sr_return_tax, 
        sr_return_amt_inc_tax, 
        sr_fee, 
        sr_return_ship_cost, 
        sr_refunded_cash, 
        sr_reversed_charge, 
        sr_store_credit, 
        sr_net_loss
 FROM TPCDS_1T.store_returns 
 ORDER BY sr_item_sk,
          sr_ticket_number
SEGMENTED BY HASH (sr_item_sk, sr_ticket_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.household_demographics_DBD_49_seg_test132_b0 /*+basename(household_demographics_DBD_49_seg_test132),createtype(D)*/
(
 hd_demo_sk ENCODING COMMONDELTA_COMP, 
 hd_income_band_sk ENCODING COMMONDELTA_COMP, 
 hd_buy_potential ENCODING AUTO, 
 hd_dep_count ENCODING COMMONDELTA_COMP, 
 hd_vehicle_count ENCODING COMMONDELTA_COMP
)
AS
 SELECT hd_demo_sk, 
        hd_income_band_sk, 
        hd_buy_potential, 
        hd_dep_count, 
        hd_vehicle_count
 FROM TPCDS_1T.household_demographics 
 ORDER BY hd_demo_sk
SEGMENTED BY HASH (hd_demo_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.household_demographics_DBD_49_seg_test132_b1 /*+basename(household_demographics_DBD_49_seg_test132),createtype(D)*/
(
 hd_demo_sk ENCODING COMMONDELTA_COMP, 
 hd_income_band_sk ENCODING COMMONDELTA_COMP, 
 hd_buy_potential ENCODING AUTO, 
 hd_dep_count ENCODING COMMONDELTA_COMP, 
 hd_vehicle_count ENCODING COMMONDELTA_COMP
)
AS
 SELECT hd_demo_sk, 
        hd_income_band_sk, 
        hd_buy_potential, 
        hd_dep_count, 
        hd_vehicle_count
 FROM TPCDS_1T.household_demographics 
 ORDER BY hd_demo_sk
SEGMENTED BY HASH (hd_demo_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.web_page_DBD_13_rep_test132 /*+createtype(D)*/
(
 wp_web_page_sk ENCODING COMMONDELTA_COMP, 
 wp_web_page_id ENCODING AUTO, 
 wp_rec_start_date ENCODING RLE, 
 wp_rec_end_date ENCODING BLOCKDICT_COMP, 
 wp_creation_date_sk ENCODING DELTAVAL, 
 wp_access_date_sk ENCODING DELTAVAL, 
 wp_autogen_flag ENCODING RLE, 
 wp_customer_sk ENCODING RLE, 
 wp_url ENCODING RLE, 
 wp_type ENCODING AUTO, 
 wp_char_count ENCODING DELTAVAL, 
 wp_link_count ENCODING DELTAVAL, 
 wp_image_count ENCODING DELTAVAL, 
 wp_max_ad_count ENCODING DELTAVAL
)
AS
 SELECT wp_web_page_sk, 
        wp_web_page_id, 
        wp_rec_start_date, 
        wp_rec_end_date, 
        wp_creation_date_sk, 
        wp_access_date_sk, 
        wp_autogen_flag, 
        wp_customer_sk, 
        wp_url, 
        wp_type, 
        wp_char_count, 
        wp_link_count, 
        wp_image_count, 
        wp_max_ad_count
 FROM TPCDS_1T.web_page 
 ORDER BY wp_url,
          wp_autogen_flag,
          wp_rec_start_date,
          wp_web_page_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.promotion_DBD_50_seg_test132_b0 /*+basename(promotion_DBD_50_seg_test132),createtype(D)*/
(
 p_promo_sk ENCODING COMMONDELTA_COMP, 
 p_promo_id ENCODING AUTO, 
 p_start_date_sk ENCODING DELTAVAL, 
 p_end_date_sk ENCODING DELTAVAL, 
 p_item_sk ENCODING DELTAVAL, 
 p_cost ENCODING BLOCKDICT_COMP, 
 p_response_target ENCODING BLOCKDICT_COMP, 
 p_promo_name ENCODING AUTO, 
 p_channel_dmail ENCODING AUTO, 
 p_channel_email ENCODING AUTO, 
 p_channel_catalog ENCODING AUTO, 
 p_channel_tv ENCODING AUTO, 
 p_channel_radio ENCODING AUTO, 
 p_channel_press ENCODING AUTO, 
 p_channel_event ENCODING AUTO, 
 p_channel_demo ENCODING AUTO, 
 p_channel_details ENCODING AUTO, 
 p_purpose ENCODING AUTO, 
 p_discount_active ENCODING AUTO
)
AS
 SELECT p_promo_sk, 
        p_promo_id, 
        p_start_date_sk, 
        p_end_date_sk, 
        p_item_sk, 
        p_cost, 
        p_response_target, 
        p_promo_name, 
        p_channel_dmail, 
        p_channel_email, 
        p_channel_catalog, 
        p_channel_tv, 
        p_channel_radio, 
        p_channel_press, 
        p_channel_event, 
        p_channel_demo, 
        p_channel_details, 
        p_purpose, 
        p_discount_active
 FROM TPCDS_1T.promotion 
 ORDER BY p_promo_sk
SEGMENTED BY HASH (p_promo_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.promotion_DBD_50_seg_test132_b1 /*+basename(promotion_DBD_50_seg_test132),createtype(D)*/
(
 p_promo_sk ENCODING COMMONDELTA_COMP, 
 p_promo_id ENCODING AUTO, 
 p_start_date_sk ENCODING DELTAVAL, 
 p_end_date_sk ENCODING DELTAVAL, 
 p_item_sk ENCODING DELTAVAL, 
 p_cost ENCODING BLOCKDICT_COMP, 
 p_response_target ENCODING BLOCKDICT_COMP, 
 p_promo_name ENCODING AUTO, 
 p_channel_dmail ENCODING AUTO, 
 p_channel_email ENCODING AUTO, 
 p_channel_catalog ENCODING AUTO, 
 p_channel_tv ENCODING AUTO, 
 p_channel_radio ENCODING AUTO, 
 p_channel_press ENCODING AUTO, 
 p_channel_event ENCODING AUTO, 
 p_channel_demo ENCODING AUTO, 
 p_channel_details ENCODING AUTO, 
 p_purpose ENCODING AUTO, 
 p_discount_active ENCODING AUTO
)
AS
 SELECT p_promo_sk, 
        p_promo_id, 
        p_start_date_sk, 
        p_end_date_sk, 
        p_item_sk, 
        p_cost, 
        p_response_target, 
        p_promo_name, 
        p_channel_dmail, 
        p_channel_email, 
        p_channel_catalog, 
        p_channel_tv, 
        p_channel_radio, 
        p_channel_press, 
        p_channel_event, 
        p_channel_demo, 
        p_channel_details, 
        p_purpose, 
        p_discount_active
 FROM TPCDS_1T.promotion 
 ORDER BY p_promo_sk
SEGMENTED BY HASH (p_promo_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.catalog_page_DBD_14_rep_test132 /*+createtype(D)*/
(
 cp_catalog_page_sk ENCODING COMMONDELTA_COMP, 
 cp_catalog_page_id ENCODING AUTO, 
 cp_start_date_sk ENCODING RLE, 
 cp_end_date_sk ENCODING RLE, 
 cp_department ENCODING RLE, 
 cp_catalog_number ENCODING RLE, 
 cp_catalog_page_number ENCODING COMMONDELTA_COMP, 
 cp_description ENCODING AUTO, 
 cp_type ENCODING RLE
)
AS
 SELECT cp_catalog_page_sk, 
        cp_catalog_page_id, 
        cp_start_date_sk, 
        cp_end_date_sk, 
        cp_department, 
        cp_catalog_number, 
        cp_catalog_page_number, 
        cp_description, 
        cp_type
 FROM TPCDS_1T.catalog_page 
 ORDER BY cp_department,
          cp_type,
          cp_start_date_sk,
          cp_catalog_page_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS_1T.inventory_DBD_51_seg_test132_b0 /*+basename(inventory_DBD_51_seg_test132),createtype(D)*/
(
 inv_date_sk ENCODING RLE, 
 inv_item_sk ENCODING COMMONDELTA_COMP, 
 inv_warehouse_sk ENCODING BLOCKDICT_COMP, 
 inv_quantity_on_hand ENCODING DELTAVAL
)
AS
 SELECT inv_date_sk, 
        inv_item_sk, 
        inv_warehouse_sk, 
        inv_quantity_on_hand
 FROM TPCDS_1T.inventory 
 ORDER BY inv_date_sk,
          inv_item_sk,
          inv_warehouse_sk
SEGMENTED BY HASH (inv_date_sk, inv_item_sk, inv_warehouse_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.inventory_DBD_51_seg_test132_b1 /*+basename(inventory_DBD_51_seg_test132),createtype(D)*/
(
 inv_date_sk ENCODING RLE, 
 inv_item_sk ENCODING COMMONDELTA_COMP, 
 inv_warehouse_sk ENCODING BLOCKDICT_COMP, 
 inv_quantity_on_hand ENCODING DELTAVAL
)
AS
 SELECT inv_date_sk, 
        inv_item_sk, 
        inv_warehouse_sk, 
        inv_quantity_on_hand
 FROM TPCDS_1T.inventory 
 ORDER BY inv_date_sk,
          inv_item_sk,
          inv_warehouse_sk
SEGMENTED BY HASH (inv_date_sk, inv_item_sk, inv_warehouse_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.catalog_returns_DBD_15_seg_test132_b0 /*+basename(catalog_returns_DBD_15_seg_test132),createtype(D)*/
(
 cr_returned_date_sk ENCODING RLE, 
 cr_returned_time_sk ENCODING DELTAVAL, 
 cr_item_sk ENCODING BLOCKDICT_COMP, 
 cr_refunded_customer_sk ENCODING DELTAVAL, 
 cr_refunded_cdemo_sk ENCODING DELTAVAL, 
 cr_refunded_hdemo_sk ENCODING DELTAVAL, 
 cr_refunded_addr_sk ENCODING DELTAVAL, 
 cr_returning_customer_sk ENCODING DELTAVAL, 
 cr_returning_cdemo_sk ENCODING DELTAVAL, 
 cr_returning_hdemo_sk ENCODING DELTAVAL, 
 cr_returning_addr_sk ENCODING DELTAVAL, 
 cr_call_center_sk ENCODING RLE, 
 cr_catalog_page_sk ENCODING DELTAVAL, 
 cr_ship_mode_sk ENCODING RLE, 
 cr_warehouse_sk ENCODING RLE, 
 cr_reason_sk ENCODING RLE, 
 cr_order_number ENCODING DELTAVAL, 
 cr_return_quantity ENCODING BLOCKDICT_COMP, 
 cr_return_amount ENCODING DELTARANGE_COMP, 
 cr_return_tax ENCODING DELTARANGE_COMP, 
 cr_return_amt_inc_tax ENCODING DELTARANGE_COMP, 
 cr_fee ENCODING DELTAVAL, 
 cr_return_ship_cost ENCODING DELTARANGE_COMP, 
 cr_refunded_cash ENCODING DELTARANGE_COMP, 
 cr_reversed_charge ENCODING DELTARANGE_COMP, 
 cr_store_credit ENCODING DELTARANGE_COMP, 
 cr_net_loss ENCODING DELTARANGE_COMP
)
AS
 SELECT cr_returned_date_sk, 
        cr_returned_time_sk, 
        cr_item_sk, 
        cr_refunded_customer_sk, 
        cr_refunded_cdemo_sk, 
        cr_refunded_hdemo_sk, 
        cr_refunded_addr_sk, 
        cr_returning_customer_sk, 
        cr_returning_cdemo_sk, 
        cr_returning_hdemo_sk, 
        cr_returning_addr_sk, 
        cr_call_center_sk, 
        cr_catalog_page_sk, 
        cr_ship_mode_sk, 
        cr_warehouse_sk, 
        cr_reason_sk, 
        cr_order_number, 
        cr_return_quantity, 
        cr_return_amount, 
        cr_return_tax, 
        cr_return_amt_inc_tax, 
        cr_fee, 
        cr_return_ship_cost, 
        cr_refunded_cash, 
        cr_reversed_charge, 
        cr_store_credit, 
        cr_net_loss
 FROM TPCDS_1T.catalog_returns 
 ORDER BY cr_returned_date_sk,
          cr_call_center_sk,
          cr_warehouse_sk,
          cr_ship_mode_sk,
          cr_reason_sk,
          cr_item_sk,
          cr_order_number
SEGMENTED BY MODULARHASH (cr_returning_customer_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.catalog_returns_DBD_15_seg_test132_b1 /*+basename(catalog_returns_DBD_15_seg_test132),createtype(D)*/
(
 cr_returned_date_sk ENCODING RLE, 
 cr_returned_time_sk ENCODING DELTAVAL, 
 cr_item_sk ENCODING BLOCKDICT_COMP, 
 cr_refunded_customer_sk ENCODING DELTAVAL, 
 cr_refunded_cdemo_sk ENCODING DELTAVAL, 
 cr_refunded_hdemo_sk ENCODING DELTAVAL, 
 cr_refunded_addr_sk ENCODING DELTAVAL, 
 cr_returning_customer_sk ENCODING DELTAVAL, 
 cr_returning_cdemo_sk ENCODING DELTAVAL, 
 cr_returning_hdemo_sk ENCODING DELTAVAL, 
 cr_returning_addr_sk ENCODING DELTAVAL, 
 cr_call_center_sk ENCODING RLE, 
 cr_catalog_page_sk ENCODING DELTAVAL, 
 cr_ship_mode_sk ENCODING RLE, 
 cr_warehouse_sk ENCODING RLE, 
 cr_reason_sk ENCODING RLE, 
 cr_order_number ENCODING DELTAVAL, 
 cr_return_quantity ENCODING BLOCKDICT_COMP, 
 cr_return_amount ENCODING DELTARANGE_COMP, 
 cr_return_tax ENCODING DELTARANGE_COMP, 
 cr_return_amt_inc_tax ENCODING DELTARANGE_COMP, 
 cr_fee ENCODING DELTAVAL, 
 cr_return_ship_cost ENCODING DELTARANGE_COMP, 
 cr_refunded_cash ENCODING DELTARANGE_COMP, 
 cr_reversed_charge ENCODING DELTARANGE_COMP, 
 cr_store_credit ENCODING DELTARANGE_COMP, 
 cr_net_loss ENCODING DELTARANGE_COMP
)
AS
 SELECT cr_returned_date_sk, 
        cr_returned_time_sk, 
        cr_item_sk, 
        cr_refunded_customer_sk, 
        cr_refunded_cdemo_sk, 
        cr_refunded_hdemo_sk, 
        cr_refunded_addr_sk, 
        cr_returning_customer_sk, 
        cr_returning_cdemo_sk, 
        cr_returning_hdemo_sk, 
        cr_returning_addr_sk, 
        cr_call_center_sk, 
        cr_catalog_page_sk, 
        cr_ship_mode_sk, 
        cr_warehouse_sk, 
        cr_reason_sk, 
        cr_order_number, 
        cr_return_quantity, 
        cr_return_amount, 
        cr_return_tax, 
        cr_return_amt_inc_tax, 
        cr_fee, 
        cr_return_ship_cost, 
        cr_refunded_cash, 
        cr_reversed_charge, 
        cr_store_credit, 
        cr_net_loss
 FROM TPCDS_1T.catalog_returns 
 ORDER BY cr_returned_date_sk,
          cr_call_center_sk,
          cr_warehouse_sk,
          cr_ship_mode_sk,
          cr_reason_sk,
          cr_item_sk,
          cr_order_number
SEGMENTED BY MODULARHASH (cr_returning_customer_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.catalog_returns_DBD_52_seg_test132_b0 /*+basename(catalog_returns_DBD_52_seg_test132),createtype(D)*/
(
 cr_returned_date_sk ENCODING COMMONDELTA_COMP, 
 cr_returned_time_sk ENCODING DELTAVAL, 
 cr_item_sk ENCODING RLE, 
 cr_refunded_customer_sk ENCODING DELTAVAL, 
 cr_refunded_cdemo_sk ENCODING DELTAVAL, 
 cr_refunded_hdemo_sk ENCODING DELTAVAL, 
 cr_refunded_addr_sk ENCODING DELTAVAL, 
 cr_returning_customer_sk ENCODING DELTAVAL, 
 cr_returning_cdemo_sk ENCODING DELTAVAL, 
 cr_returning_hdemo_sk ENCODING DELTAVAL, 
 cr_returning_addr_sk ENCODING DELTAVAL, 
 cr_call_center_sk ENCODING BLOCKDICT_COMP, 
 cr_catalog_page_sk ENCODING DELTARANGE_COMP, 
 cr_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 cr_warehouse_sk ENCODING BLOCKDICT_COMP, 
 cr_reason_sk ENCODING BLOCKDICT_COMP, 
 cr_order_number ENCODING DELTARANGE_COMP, 
 cr_return_quantity ENCODING BLOCKDICT_COMP, 
 cr_return_amount ENCODING DELTARANGE_COMP, 
 cr_return_tax ENCODING DELTARANGE_COMP, 
 cr_return_amt_inc_tax ENCODING DELTARANGE_COMP, 
 cr_fee ENCODING DELTAVAL, 
 cr_return_ship_cost ENCODING DELTARANGE_COMP, 
 cr_refunded_cash ENCODING DELTARANGE_COMP, 
 cr_reversed_charge ENCODING DELTARANGE_COMP, 
 cr_store_credit ENCODING DELTARANGE_COMP, 
 cr_net_loss ENCODING DELTARANGE_COMP
)
AS
 SELECT cr_returned_date_sk, 
        cr_returned_time_sk, 
        cr_item_sk, 
        cr_refunded_customer_sk, 
        cr_refunded_cdemo_sk, 
        cr_refunded_hdemo_sk, 
        cr_refunded_addr_sk, 
        cr_returning_customer_sk, 
        cr_returning_cdemo_sk, 
        cr_returning_hdemo_sk, 
        cr_returning_addr_sk, 
        cr_call_center_sk, 
        cr_catalog_page_sk, 
        cr_ship_mode_sk, 
        cr_warehouse_sk, 
        cr_reason_sk, 
        cr_order_number, 
        cr_return_quantity, 
        cr_return_amount, 
        cr_return_tax, 
        cr_return_amt_inc_tax, 
        cr_fee, 
        cr_return_ship_cost, 
        cr_refunded_cash, 
        cr_reversed_charge, 
        cr_store_credit, 
        cr_net_loss
 FROM TPCDS_1T.catalog_returns 
 ORDER BY cr_item_sk,
          cr_order_number
SEGMENTED BY HASH (cr_item_sk, cr_order_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.catalog_returns_DBD_52_seg_test132_b1 /*+basename(catalog_returns_DBD_52_seg_test132),createtype(D)*/
(
 cr_returned_date_sk ENCODING COMMONDELTA_COMP, 
 cr_returned_time_sk ENCODING DELTAVAL, 
 cr_item_sk ENCODING RLE, 
 cr_refunded_customer_sk ENCODING DELTAVAL, 
 cr_refunded_cdemo_sk ENCODING DELTAVAL, 
 cr_refunded_hdemo_sk ENCODING DELTAVAL, 
 cr_refunded_addr_sk ENCODING DELTAVAL, 
 cr_returning_customer_sk ENCODING DELTAVAL, 
 cr_returning_cdemo_sk ENCODING DELTAVAL, 
 cr_returning_hdemo_sk ENCODING DELTAVAL, 
 cr_returning_addr_sk ENCODING DELTAVAL, 
 cr_call_center_sk ENCODING BLOCKDICT_COMP, 
 cr_catalog_page_sk ENCODING DELTARANGE_COMP, 
 cr_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 cr_warehouse_sk ENCODING BLOCKDICT_COMP, 
 cr_reason_sk ENCODING BLOCKDICT_COMP, 
 cr_order_number ENCODING DELTARANGE_COMP, 
 cr_return_quantity ENCODING BLOCKDICT_COMP, 
 cr_return_amount ENCODING DELTARANGE_COMP, 
 cr_return_tax ENCODING DELTARANGE_COMP, 
 cr_return_amt_inc_tax ENCODING DELTARANGE_COMP, 
 cr_fee ENCODING DELTAVAL, 
 cr_return_ship_cost ENCODING DELTARANGE_COMP, 
 cr_refunded_cash ENCODING DELTARANGE_COMP, 
 cr_reversed_charge ENCODING DELTARANGE_COMP, 
 cr_store_credit ENCODING DELTARANGE_COMP, 
 cr_net_loss ENCODING DELTARANGE_COMP
)
AS
 SELECT cr_returned_date_sk, 
        cr_returned_time_sk, 
        cr_item_sk, 
        cr_refunded_customer_sk, 
        cr_refunded_cdemo_sk, 
        cr_refunded_hdemo_sk, 
        cr_refunded_addr_sk, 
        cr_returning_customer_sk, 
        cr_returning_cdemo_sk, 
        cr_returning_hdemo_sk, 
        cr_returning_addr_sk, 
        cr_call_center_sk, 
        cr_catalog_page_sk, 
        cr_ship_mode_sk, 
        cr_warehouse_sk, 
        cr_reason_sk, 
        cr_order_number, 
        cr_return_quantity, 
        cr_return_amount, 
        cr_return_tax, 
        cr_return_amt_inc_tax, 
        cr_fee, 
        cr_return_ship_cost, 
        cr_refunded_cash, 
        cr_reversed_charge, 
        cr_store_credit, 
        cr_net_loss
 FROM TPCDS_1T.catalog_returns 
 ORDER BY cr_item_sk,
          cr_order_number
SEGMENTED BY HASH (cr_item_sk, cr_order_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.web_returns_DBD_16_seg_test132_b0 /*+basename(web_returns_DBD_16_seg_test132),createtype(D)*/
(
 wr_returned_date_sk ENCODING DELTAVAL, 
 wr_returned_time_sk ENCODING DELTAVAL, 
 wr_item_sk ENCODING RLE, 
 wr_refunded_customer_sk ENCODING DELTAVAL, 
 wr_refunded_cdemo_sk ENCODING DELTAVAL, 
 wr_refunded_hdemo_sk ENCODING DELTAVAL, 
 wr_refunded_addr_sk ENCODING DELTAVAL, 
 wr_returning_customer_sk ENCODING DELTAVAL, 
 wr_returning_cdemo_sk ENCODING DELTAVAL, 
 wr_returning_hdemo_sk ENCODING DELTAVAL, 
 wr_returning_addr_sk ENCODING DELTAVAL, 
 wr_web_page_sk ENCODING DELTAVAL, 
 wr_reason_sk ENCODING BLOCKDICT_COMP, 
 wr_order_number ENCODING DELTARANGE_COMP, 
 wr_return_quantity ENCODING BLOCKDICT_COMP, 
 wr_return_amt ENCODING DELTAVAL, 
 wr_return_tax ENCODING DELTARANGE_COMP, 
 wr_return_amt_inc_tax ENCODING DELTAVAL, 
 wr_fee ENCODING DELTAVAL, 
 wr_return_ship_cost ENCODING DELTAVAL, 
 wr_refunded_cash ENCODING DELTARANGE_COMP, 
 wr_reversed_charge ENCODING DELTARANGE_COMP, 
 wr_account_credit ENCODING DELTARANGE_COMP, 
 wr_net_loss ENCODING DELTAVAL
)
AS
 SELECT wr_returned_date_sk, 
        wr_returned_time_sk, 
        wr_item_sk, 
        wr_refunded_customer_sk, 
        wr_refunded_cdemo_sk, 
        wr_refunded_hdemo_sk, 
        wr_refunded_addr_sk, 
        wr_returning_customer_sk, 
        wr_returning_cdemo_sk, 
        wr_returning_hdemo_sk, 
        wr_returning_addr_sk, 
        wr_web_page_sk, 
        wr_reason_sk, 
        wr_order_number, 
        wr_return_quantity, 
        wr_return_amt, 
        wr_return_tax, 
        wr_return_amt_inc_tax, 
        wr_fee, 
        wr_return_ship_cost, 
        wr_refunded_cash, 
        wr_reversed_charge, 
        wr_account_credit, 
        wr_net_loss
 FROM TPCDS_1T.web_returns 
 ORDER BY wr_item_sk,
          wr_order_number
SEGMENTED BY MODULARHASH (wr_item_sk, wr_order_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.web_returns_DBD_16_seg_test132_b1 /*+basename(web_returns_DBD_16_seg_test132),createtype(D)*/
(
 wr_returned_date_sk ENCODING DELTAVAL, 
 wr_returned_time_sk ENCODING DELTAVAL, 
 wr_item_sk ENCODING RLE, 
 wr_refunded_customer_sk ENCODING DELTAVAL, 
 wr_refunded_cdemo_sk ENCODING DELTAVAL, 
 wr_refunded_hdemo_sk ENCODING DELTAVAL, 
 wr_refunded_addr_sk ENCODING DELTAVAL, 
 wr_returning_customer_sk ENCODING DELTAVAL, 
 wr_returning_cdemo_sk ENCODING DELTAVAL, 
 wr_returning_hdemo_sk ENCODING DELTAVAL, 
 wr_returning_addr_sk ENCODING DELTAVAL, 
 wr_web_page_sk ENCODING DELTAVAL, 
 wr_reason_sk ENCODING BLOCKDICT_COMP, 
 wr_order_number ENCODING DELTARANGE_COMP, 
 wr_return_quantity ENCODING BLOCKDICT_COMP, 
 wr_return_amt ENCODING DELTAVAL, 
 wr_return_tax ENCODING DELTARANGE_COMP, 
 wr_return_amt_inc_tax ENCODING DELTAVAL, 
 wr_fee ENCODING DELTAVAL, 
 wr_return_ship_cost ENCODING DELTAVAL, 
 wr_refunded_cash ENCODING DELTARANGE_COMP, 
 wr_reversed_charge ENCODING DELTARANGE_COMP, 
 wr_account_credit ENCODING DELTARANGE_COMP, 
 wr_net_loss ENCODING DELTAVAL
)
AS
 SELECT wr_returned_date_sk, 
        wr_returned_time_sk, 
        wr_item_sk, 
        wr_refunded_customer_sk, 
        wr_refunded_cdemo_sk, 
        wr_refunded_hdemo_sk, 
        wr_refunded_addr_sk, 
        wr_returning_customer_sk, 
        wr_returning_cdemo_sk, 
        wr_returning_hdemo_sk, 
        wr_returning_addr_sk, 
        wr_web_page_sk, 
        wr_reason_sk, 
        wr_order_number, 
        wr_return_quantity, 
        wr_return_amt, 
        wr_return_tax, 
        wr_return_amt_inc_tax, 
        wr_fee, 
        wr_return_ship_cost, 
        wr_refunded_cash, 
        wr_reversed_charge, 
        wr_account_credit, 
        wr_net_loss
 FROM TPCDS_1T.web_returns 
 ORDER BY wr_item_sk,
          wr_order_number
SEGMENTED BY MODULARHASH (wr_item_sk, wr_order_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.web_sales_DBD_17_seg_test132_b0 /*+basename(web_sales_DBD_17_seg_test132),createtype(D)*/
(
 ws_sold_date_sk ENCODING RLE, 
 ws_sold_time_sk ENCODING DELTAVAL, 
 ws_ship_date_sk ENCODING DELTAVAL, 
 ws_item_sk ENCODING RLE, 
 ws_bill_customer_sk ENCODING DELTAVAL, 
 ws_bill_cdemo_sk ENCODING DELTAVAL, 
 ws_bill_hdemo_sk ENCODING DELTAVAL, 
 ws_bill_addr_sk ENCODING DELTAVAL, 
 ws_ship_customer_sk ENCODING DELTAVAL, 
 ws_ship_cdemo_sk ENCODING DELTAVAL, 
 ws_ship_hdemo_sk ENCODING DELTAVAL, 
 ws_ship_addr_sk ENCODING DELTAVAL, 
 ws_web_page_sk ENCODING BLOCKDICT_COMP, 
 ws_web_site_sk ENCODING BLOCKDICT_COMP, 
 ws_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 ws_warehouse_sk ENCODING RLE, 
 ws_promo_sk ENCODING DELTAVAL, 
 ws_order_number ENCODING DELTARANGE_COMP, 
 ws_quantity ENCODING DELTAVAL, 
 ws_wholesale_cost ENCODING DELTAVAL, 
 ws_list_price ENCODING DELTAVAL, 
 ws_sales_price ENCODING DELTAVAL, 
 ws_ext_discount_amt ENCODING DELTARANGE_COMP, 
 ws_ext_sales_price ENCODING DELTARANGE_COMP, 
 ws_ext_wholesale_cost ENCODING DELTAVAL, 
 ws_ext_list_price ENCODING DELTAVAL, 
 ws_ext_tax ENCODING DELTARANGE_COMP, 
 ws_coupon_amt ENCODING DELTARANGE_COMP, 
 ws_ext_ship_cost ENCODING DELTARANGE_COMP, 
 ws_net_paid ENCODING DELTARANGE_COMP, 
 ws_net_paid_inc_tax ENCODING DELTARANGE_COMP, 
 ws_net_paid_inc_ship ENCODING DELTARANGE_COMP, 
 ws_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP, 
 ws_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT ws_sold_date_sk, 
        ws_sold_time_sk, 
        ws_ship_date_sk, 
        ws_item_sk, 
        ws_bill_customer_sk, 
        ws_bill_cdemo_sk, 
        ws_bill_hdemo_sk, 
        ws_bill_addr_sk, 
        ws_ship_customer_sk, 
        ws_ship_cdemo_sk, 
        ws_ship_hdemo_sk, 
        ws_ship_addr_sk, 
        ws_web_page_sk, 
        ws_web_site_sk, 
        ws_ship_mode_sk, 
        ws_warehouse_sk, 
        ws_promo_sk, 
        ws_order_number, 
        ws_quantity, 
        ws_wholesale_cost, 
        ws_list_price, 
        ws_sales_price, 
        ws_ext_discount_amt, 
        ws_ext_sales_price, 
        ws_ext_wholesale_cost, 
        ws_ext_list_price, 
        ws_ext_tax, 
        ws_coupon_amt, 
        ws_ext_ship_cost, 
        ws_net_paid, 
        ws_net_paid_inc_tax, 
        ws_net_paid_inc_ship, 
        ws_net_paid_inc_ship_tax, 
        ws_net_profit
 FROM TPCDS_1T.web_sales 
 ORDER BY ws_sold_date_sk,
          ws_item_sk,
          ws_warehouse_sk,
          ws_order_number
SEGMENTED BY MODULARHASH (ws_sold_date_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.web_sales_DBD_17_seg_test132_b1 /*+basename(web_sales_DBD_17_seg_test132),createtype(D)*/
(
 ws_sold_date_sk ENCODING RLE, 
 ws_sold_time_sk ENCODING DELTAVAL, 
 ws_ship_date_sk ENCODING DELTAVAL, 
 ws_item_sk ENCODING RLE, 
 ws_bill_customer_sk ENCODING DELTAVAL, 
 ws_bill_cdemo_sk ENCODING DELTAVAL, 
 ws_bill_hdemo_sk ENCODING DELTAVAL, 
 ws_bill_addr_sk ENCODING DELTAVAL, 
 ws_ship_customer_sk ENCODING DELTAVAL, 
 ws_ship_cdemo_sk ENCODING DELTAVAL, 
 ws_ship_hdemo_sk ENCODING DELTAVAL, 
 ws_ship_addr_sk ENCODING DELTAVAL, 
 ws_web_page_sk ENCODING BLOCKDICT_COMP, 
 ws_web_site_sk ENCODING BLOCKDICT_COMP, 
 ws_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 ws_warehouse_sk ENCODING RLE, 
 ws_promo_sk ENCODING DELTAVAL, 
 ws_order_number ENCODING DELTARANGE_COMP, 
 ws_quantity ENCODING DELTAVAL, 
 ws_wholesale_cost ENCODING DELTAVAL, 
 ws_list_price ENCODING DELTAVAL, 
 ws_sales_price ENCODING DELTAVAL, 
 ws_ext_discount_amt ENCODING DELTARANGE_COMP, 
 ws_ext_sales_price ENCODING DELTARANGE_COMP, 
 ws_ext_wholesale_cost ENCODING DELTAVAL, 
 ws_ext_list_price ENCODING DELTAVAL, 
 ws_ext_tax ENCODING DELTARANGE_COMP, 
 ws_coupon_amt ENCODING DELTARANGE_COMP, 
 ws_ext_ship_cost ENCODING DELTARANGE_COMP, 
 ws_net_paid ENCODING DELTARANGE_COMP, 
 ws_net_paid_inc_tax ENCODING DELTARANGE_COMP, 
 ws_net_paid_inc_ship ENCODING DELTARANGE_COMP, 
 ws_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP, 
 ws_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT ws_sold_date_sk, 
        ws_sold_time_sk, 
        ws_ship_date_sk, 
        ws_item_sk, 
        ws_bill_customer_sk, 
        ws_bill_cdemo_sk, 
        ws_bill_hdemo_sk, 
        ws_bill_addr_sk, 
        ws_ship_customer_sk, 
        ws_ship_cdemo_sk, 
        ws_ship_hdemo_sk, 
        ws_ship_addr_sk, 
        ws_web_page_sk, 
        ws_web_site_sk, 
        ws_ship_mode_sk, 
        ws_warehouse_sk, 
        ws_promo_sk, 
        ws_order_number, 
        ws_quantity, 
        ws_wholesale_cost, 
        ws_list_price, 
        ws_sales_price, 
        ws_ext_discount_amt, 
        ws_ext_sales_price, 
        ws_ext_wholesale_cost, 
        ws_ext_list_price, 
        ws_ext_tax, 
        ws_coupon_amt, 
        ws_ext_ship_cost, 
        ws_net_paid, 
        ws_net_paid_inc_tax, 
        ws_net_paid_inc_ship, 
        ws_net_paid_inc_ship_tax, 
        ws_net_profit
 FROM TPCDS_1T.web_sales 
 ORDER BY ws_sold_date_sk,
          ws_item_sk,
          ws_warehouse_sk,
          ws_order_number
SEGMENTED BY MODULARHASH (ws_sold_date_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.catalog_sales_DBD_18_seg_test132_b0 /*+basename(catalog_sales_DBD_18_seg_test132),createtype(D)*/
(
 cs_sold_date_sk ENCODING RLE, 
 cs_sold_time_sk ENCODING DELTAVAL, 
 cs_ship_date_sk ENCODING DELTAVAL, 
 cs_bill_customer_sk ENCODING DELTAVAL, 
 cs_bill_cdemo_sk ENCODING DELTAVAL, 
 cs_bill_hdemo_sk ENCODING DELTAVAL, 
 cs_bill_addr_sk ENCODING DELTAVAL, 
 cs_ship_customer_sk ENCODING DELTAVAL, 
 cs_ship_cdemo_sk ENCODING DELTAVAL, 
 cs_ship_hdemo_sk ENCODING DELTAVAL, 
 cs_ship_addr_sk ENCODING DELTAVAL, 
 cs_call_center_sk ENCODING RLE, 
 cs_catalog_page_sk ENCODING DELTAVAL, 
 cs_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 cs_warehouse_sk ENCODING BLOCKDICT_COMP, 
 cs_item_sk ENCODING RLE, 
 cs_promo_sk ENCODING DELTAVAL, 
 cs_order_number ENCODING DELTARANGE_COMP, 
 cs_quantity ENCODING DELTAVAL, 
 cs_wholesale_cost ENCODING DELTAVAL, 
 cs_list_price ENCODING DELTAVAL, 
 cs_sales_price ENCODING DELTAVAL, 
 cs_ext_discount_amt ENCODING DELTARANGE_COMP, 
 cs_ext_sales_price ENCODING DELTARANGE_COMP, 
 cs_ext_wholesale_cost ENCODING DELTAVAL, 
 cs_ext_list_price ENCODING DELTAVAL, 
 cs_ext_tax ENCODING DELTARANGE_COMP, 
 cs_coupon_amt ENCODING DELTARANGE_COMP, 
 cs_ext_ship_cost ENCODING DELTARANGE_COMP, 
 cs_net_paid ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_tax ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP, 
 cs_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT cs_sold_date_sk, 
        cs_sold_time_sk, 
        cs_ship_date_sk, 
        cs_bill_customer_sk, 
        cs_bill_cdemo_sk, 
        cs_bill_hdemo_sk, 
        cs_bill_addr_sk, 
        cs_ship_customer_sk, 
        cs_ship_cdemo_sk, 
        cs_ship_hdemo_sk, 
        cs_ship_addr_sk, 
        cs_call_center_sk, 
        cs_catalog_page_sk, 
        cs_ship_mode_sk, 
        cs_warehouse_sk, 
        cs_item_sk, 
        cs_promo_sk, 
        cs_order_number, 
        cs_quantity, 
        cs_wholesale_cost, 
        cs_list_price, 
        cs_sales_price, 
        cs_ext_discount_amt, 
        cs_ext_sales_price, 
        cs_ext_wholesale_cost, 
        cs_ext_list_price, 
        cs_ext_tax, 
        cs_coupon_amt, 
        cs_ext_ship_cost, 
        cs_net_paid, 
        cs_net_paid_inc_tax, 
        cs_net_paid_inc_ship, 
        cs_net_paid_inc_ship_tax, 
        cs_net_profit
 FROM TPCDS_1T.catalog_sales 
 ORDER BY cs_sold_date_sk,
          cs_item_sk,
          cs_call_center_sk,
          cs_order_number
SEGMENTED BY MODULARHASH (cs_sold_date_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.catalog_sales_DBD_19_seg_test132_b0 /*+basename(catalog_sales_DBD_19_seg_test132),createtype(D)*/
(
 cs_item_sk ENCODING RLE
)
AS
 SELECT cs_item_sk
 FROM TPCDS_1T.catalog_sales 
 ORDER BY cs_item_sk
SEGMENTED BY MODULARHASH (cs_item_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.catalog_sales_DBD_18_seg_test132_b1 /*+basename(catalog_sales_DBD_18_seg_test132),createtype(D)*/
(
 cs_sold_date_sk ENCODING RLE, 
 cs_sold_time_sk ENCODING DELTAVAL, 
 cs_ship_date_sk ENCODING DELTAVAL, 
 cs_bill_customer_sk ENCODING DELTAVAL, 
 cs_bill_cdemo_sk ENCODING DELTAVAL, 
 cs_bill_hdemo_sk ENCODING DELTAVAL, 
 cs_bill_addr_sk ENCODING DELTAVAL, 
 cs_ship_customer_sk ENCODING DELTAVAL, 
 cs_ship_cdemo_sk ENCODING DELTAVAL, 
 cs_ship_hdemo_sk ENCODING DELTAVAL, 
 cs_ship_addr_sk ENCODING DELTAVAL, 
 cs_call_center_sk ENCODING RLE, 
 cs_catalog_page_sk ENCODING DELTAVAL, 
 cs_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 cs_warehouse_sk ENCODING BLOCKDICT_COMP, 
 cs_item_sk ENCODING RLE, 
 cs_promo_sk ENCODING DELTAVAL, 
 cs_order_number ENCODING DELTARANGE_COMP, 
 cs_quantity ENCODING DELTAVAL, 
 cs_wholesale_cost ENCODING DELTAVAL, 
 cs_list_price ENCODING DELTAVAL, 
 cs_sales_price ENCODING DELTAVAL, 
 cs_ext_discount_amt ENCODING DELTARANGE_COMP, 
 cs_ext_sales_price ENCODING DELTARANGE_COMP, 
 cs_ext_wholesale_cost ENCODING DELTAVAL, 
 cs_ext_list_price ENCODING DELTAVAL, 
 cs_ext_tax ENCODING DELTARANGE_COMP, 
 cs_coupon_amt ENCODING DELTARANGE_COMP, 
 cs_ext_ship_cost ENCODING DELTARANGE_COMP, 
 cs_net_paid ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_tax ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP, 
 cs_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT cs_sold_date_sk, 
        cs_sold_time_sk, 
        cs_ship_date_sk, 
        cs_bill_customer_sk, 
        cs_bill_cdemo_sk, 
        cs_bill_hdemo_sk, 
        cs_bill_addr_sk, 
        cs_ship_customer_sk, 
        cs_ship_cdemo_sk, 
        cs_ship_hdemo_sk, 
        cs_ship_addr_sk, 
        cs_call_center_sk, 
        cs_catalog_page_sk, 
        cs_ship_mode_sk, 
        cs_warehouse_sk, 
        cs_item_sk, 
        cs_promo_sk, 
        cs_order_number, 
        cs_quantity, 
        cs_wholesale_cost, 
        cs_list_price, 
        cs_sales_price, 
        cs_ext_discount_amt, 
        cs_ext_sales_price, 
        cs_ext_wholesale_cost, 
        cs_ext_list_price, 
        cs_ext_tax, 
        cs_coupon_amt, 
        cs_ext_ship_cost, 
        cs_net_paid, 
        cs_net_paid_inc_tax, 
        cs_net_paid_inc_ship, 
        cs_net_paid_inc_ship_tax, 
        cs_net_profit
 FROM TPCDS_1T.catalog_sales 
 ORDER BY cs_sold_date_sk,
          cs_item_sk,
          cs_call_center_sk,
          cs_order_number
SEGMENTED BY MODULARHASH (cs_sold_date_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.catalog_sales_DBD_19_seg_test132_b1 /*+basename(catalog_sales_DBD_19_seg_test132),createtype(D)*/
(
 cs_item_sk ENCODING RLE
)
AS
 SELECT cs_item_sk
 FROM TPCDS_1T.catalog_sales 
 ORDER BY cs_item_sk
SEGMENTED BY MODULARHASH (cs_item_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.catalog_sales_DBD_53_seg_test132_b0 /*+basename(catalog_sales_DBD_53_seg_test132),createtype(D)*/
(
 cs_sold_date_sk ENCODING DELTARANGE_COMP, 
 cs_sold_time_sk ENCODING DELTAVAL, 
 cs_ship_date_sk ENCODING COMMONDELTA_COMP, 
 cs_bill_customer_sk ENCODING DELTAVAL, 
 cs_bill_cdemo_sk ENCODING DELTAVAL, 
 cs_bill_hdemo_sk ENCODING DELTAVAL, 
 cs_bill_addr_sk ENCODING DELTAVAL, 
 cs_ship_customer_sk ENCODING DELTAVAL, 
 cs_ship_cdemo_sk ENCODING DELTAVAL, 
 cs_ship_hdemo_sk ENCODING DELTAVAL, 
 cs_ship_addr_sk ENCODING DELTAVAL, 
 cs_call_center_sk ENCODING BLOCKDICT_COMP, 
 cs_catalog_page_sk ENCODING DELTARANGE_COMP, 
 cs_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 cs_warehouse_sk ENCODING BLOCKDICT_COMP, 
 cs_item_sk ENCODING RLE, 
 cs_promo_sk ENCODING DELTAVAL, 
 cs_order_number ENCODING DELTARANGE_COMP, 
 cs_quantity ENCODING DELTAVAL, 
 cs_wholesale_cost ENCODING DELTAVAL, 
 cs_list_price ENCODING DELTAVAL, 
 cs_sales_price ENCODING DELTAVAL, 
 cs_ext_discount_amt ENCODING DELTARANGE_COMP, 
 cs_ext_sales_price ENCODING DELTARANGE_COMP, 
 cs_ext_wholesale_cost ENCODING DELTAVAL, 
 cs_ext_list_price ENCODING DELTAVAL, 
 cs_ext_tax ENCODING DELTARANGE_COMP, 
 cs_coupon_amt ENCODING DELTARANGE_COMP, 
 cs_ext_ship_cost ENCODING DELTARANGE_COMP, 
 cs_net_paid ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_tax ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP, 
 cs_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT cs_sold_date_sk, 
        cs_sold_time_sk, 
        cs_ship_date_sk, 
        cs_bill_customer_sk, 
        cs_bill_cdemo_sk, 
        cs_bill_hdemo_sk, 
        cs_bill_addr_sk, 
        cs_ship_customer_sk, 
        cs_ship_cdemo_sk, 
        cs_ship_hdemo_sk, 
        cs_ship_addr_sk, 
        cs_call_center_sk, 
        cs_catalog_page_sk, 
        cs_ship_mode_sk, 
        cs_warehouse_sk, 
        cs_item_sk, 
        cs_promo_sk, 
        cs_order_number, 
        cs_quantity, 
        cs_wholesale_cost, 
        cs_list_price, 
        cs_sales_price, 
        cs_ext_discount_amt, 
        cs_ext_sales_price, 
        cs_ext_wholesale_cost, 
        cs_ext_list_price, 
        cs_ext_tax, 
        cs_coupon_amt, 
        cs_ext_ship_cost, 
        cs_net_paid, 
        cs_net_paid_inc_tax, 
        cs_net_paid_inc_ship, 
        cs_net_paid_inc_ship_tax, 
        cs_net_profit
 FROM TPCDS_1T.catalog_sales 
 ORDER BY cs_item_sk,
          cs_order_number
SEGMENTED BY HASH (cs_item_sk, cs_order_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.catalog_sales_DBD_53_seg_test132_b1 /*+basename(catalog_sales_DBD_53_seg_test132),createtype(D)*/
(
 cs_sold_date_sk ENCODING DELTARANGE_COMP, 
 cs_sold_time_sk ENCODING DELTAVAL, 
 cs_ship_date_sk ENCODING COMMONDELTA_COMP, 
 cs_bill_customer_sk ENCODING DELTAVAL, 
 cs_bill_cdemo_sk ENCODING DELTAVAL, 
 cs_bill_hdemo_sk ENCODING DELTAVAL, 
 cs_bill_addr_sk ENCODING DELTAVAL, 
 cs_ship_customer_sk ENCODING DELTAVAL, 
 cs_ship_cdemo_sk ENCODING DELTAVAL, 
 cs_ship_hdemo_sk ENCODING DELTAVAL, 
 cs_ship_addr_sk ENCODING DELTAVAL, 
 cs_call_center_sk ENCODING BLOCKDICT_COMP, 
 cs_catalog_page_sk ENCODING DELTARANGE_COMP, 
 cs_ship_mode_sk ENCODING BLOCKDICT_COMP, 
 cs_warehouse_sk ENCODING BLOCKDICT_COMP, 
 cs_item_sk ENCODING RLE, 
 cs_promo_sk ENCODING DELTAVAL, 
 cs_order_number ENCODING DELTARANGE_COMP, 
 cs_quantity ENCODING DELTAVAL, 
 cs_wholesale_cost ENCODING DELTAVAL, 
 cs_list_price ENCODING DELTAVAL, 
 cs_sales_price ENCODING DELTAVAL, 
 cs_ext_discount_amt ENCODING DELTARANGE_COMP, 
 cs_ext_sales_price ENCODING DELTARANGE_COMP, 
 cs_ext_wholesale_cost ENCODING DELTAVAL, 
 cs_ext_list_price ENCODING DELTAVAL, 
 cs_ext_tax ENCODING DELTARANGE_COMP, 
 cs_coupon_amt ENCODING DELTARANGE_COMP, 
 cs_ext_ship_cost ENCODING DELTARANGE_COMP, 
 cs_net_paid ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_tax ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship ENCODING DELTARANGE_COMP, 
 cs_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP, 
 cs_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT cs_sold_date_sk, 
        cs_sold_time_sk, 
        cs_ship_date_sk, 
        cs_bill_customer_sk, 
        cs_bill_cdemo_sk, 
        cs_bill_hdemo_sk, 
        cs_bill_addr_sk, 
        cs_ship_customer_sk, 
        cs_ship_cdemo_sk, 
        cs_ship_hdemo_sk, 
        cs_ship_addr_sk, 
        cs_call_center_sk, 
        cs_catalog_page_sk, 
        cs_ship_mode_sk, 
        cs_warehouse_sk, 
        cs_item_sk, 
        cs_promo_sk, 
        cs_order_number, 
        cs_quantity, 
        cs_wholesale_cost, 
        cs_list_price, 
        cs_sales_price, 
        cs_ext_discount_amt, 
        cs_ext_sales_price, 
        cs_ext_wholesale_cost, 
        cs_ext_list_price, 
        cs_ext_tax, 
        cs_coupon_amt, 
        cs_ext_ship_cost, 
        cs_net_paid, 
        cs_net_paid_inc_tax, 
        cs_net_paid_inc_ship, 
        cs_net_paid_inc_ship_tax, 
        cs_net_profit
 FROM TPCDS_1T.catalog_sales 
 ORDER BY cs_item_sk,
          cs_order_number
SEGMENTED BY HASH (cs_item_sk, cs_order_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.store_sales_DBD_20_seg_test132_b0 /*+basename(store_sales_DBD_20_seg_test132),createtype(D)*/
(
 ss_sold_date_sk ENCODING RLE, 
 ss_sold_time_sk ENCODING DELTAVAL, 
 ss_item_sk ENCODING RLE, 
 ss_customer_sk ENCODING DELTAVAL, 
 ss_cdemo_sk ENCODING DELTAVAL, 
 ss_hdemo_sk ENCODING DELTAVAL, 
 ss_addr_sk ENCODING DELTAVAL, 
 ss_store_sk ENCODING RLE, 
 ss_promo_sk ENCODING DELTAVAL, 
 ss_ticket_number ENCODING DELTARANGE_COMP, 
 ss_quantity ENCODING DELTAVAL, 
 ss_wholesale_cost ENCODING DELTAVAL, 
 ss_list_price ENCODING DELTAVAL, 
 ss_sales_price ENCODING DELTAVAL, 
 ss_ext_discount_amt ENCODING DELTARANGE_COMP, 
 ss_ext_sales_price ENCODING DELTAVAL, 
 ss_ext_wholesale_cost ENCODING DELTAVAL, 
 ss_ext_list_price ENCODING DELTAVAL, 
 ss_ext_tax ENCODING DELTARANGE_COMP, 
 ss_coupon_amt ENCODING DELTARANGE_COMP, 
 ss_net_paid ENCODING DELTAVAL, 
 ss_net_paid_inc_tax ENCODING DELTAVAL, 
 ss_net_profit ENCODING DELTAVAL
)
AS
 SELECT ss_sold_date_sk, 
        ss_sold_time_sk, 
        ss_item_sk, 
        ss_customer_sk, 
        ss_cdemo_sk, 
        ss_hdemo_sk, 
        ss_addr_sk, 
        ss_store_sk, 
        ss_promo_sk, 
        ss_ticket_number, 
        ss_quantity, 
        ss_wholesale_cost, 
        ss_list_price, 
        ss_sales_price, 
        ss_ext_discount_amt, 
        ss_ext_sales_price, 
        ss_ext_wholesale_cost, 
        ss_ext_list_price, 
        ss_ext_tax, 
        ss_coupon_amt, 
        ss_net_paid, 
        ss_net_paid_inc_tax, 
        ss_net_profit
 FROM TPCDS_1T.store_sales 
 ORDER BY ss_sold_date_sk,
          ss_item_sk,
          ss_store_sk,
          ss_ticket_number
SEGMENTED BY MODULARHASH (ss_item_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.store_sales_DBD_20_seg_test132_b1 /*+basename(store_sales_DBD_20_seg_test132),createtype(D)*/
(
 ss_sold_date_sk ENCODING RLE, 
 ss_sold_time_sk ENCODING DELTAVAL, 
 ss_item_sk ENCODING RLE, 
 ss_customer_sk ENCODING DELTAVAL, 
 ss_cdemo_sk ENCODING DELTAVAL, 
 ss_hdemo_sk ENCODING DELTAVAL, 
 ss_addr_sk ENCODING DELTAVAL, 
 ss_store_sk ENCODING RLE, 
 ss_promo_sk ENCODING DELTAVAL, 
 ss_ticket_number ENCODING DELTARANGE_COMP, 
 ss_quantity ENCODING DELTAVAL, 
 ss_wholesale_cost ENCODING DELTAVAL, 
 ss_list_price ENCODING DELTAVAL, 
 ss_sales_price ENCODING DELTAVAL, 
 ss_ext_discount_amt ENCODING DELTARANGE_COMP, 
 ss_ext_sales_price ENCODING DELTAVAL, 
 ss_ext_wholesale_cost ENCODING DELTAVAL, 
 ss_ext_list_price ENCODING DELTAVAL, 
 ss_ext_tax ENCODING DELTARANGE_COMP, 
 ss_coupon_amt ENCODING DELTARANGE_COMP, 
 ss_net_paid ENCODING DELTAVAL, 
 ss_net_paid_inc_tax ENCODING DELTAVAL, 
 ss_net_profit ENCODING DELTAVAL
)
AS
 SELECT ss_sold_date_sk, 
        ss_sold_time_sk, 
        ss_item_sk, 
        ss_customer_sk, 
        ss_cdemo_sk, 
        ss_hdemo_sk, 
        ss_addr_sk, 
        ss_store_sk, 
        ss_promo_sk, 
        ss_ticket_number, 
        ss_quantity, 
        ss_wholesale_cost, 
        ss_list_price, 
        ss_sales_price, 
        ss_ext_discount_amt, 
        ss_ext_sales_price, 
        ss_ext_wholesale_cost, 
        ss_ext_list_price, 
        ss_ext_tax, 
        ss_coupon_amt, 
        ss_net_paid, 
        ss_net_paid_inc_tax, 
        ss_net_profit
 FROM TPCDS_1T.store_sales 
 ORDER BY ss_sold_date_sk,
          ss_item_sk,
          ss_store_sk,
          ss_ticket_number
SEGMENTED BY MODULARHASH (ss_item_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS_1T.store_sales_DBD_54_seg_test132_b0 /*+basename(store_sales_DBD_54_seg_test132),createtype(D)*/
(
 ss_sold_date_sk ENCODING DELTAVAL, 
 ss_sold_time_sk ENCODING DELTAVAL, 
 ss_item_sk ENCODING RLE, 
 ss_customer_sk ENCODING DELTAVAL, 
 ss_cdemo_sk ENCODING DELTAVAL, 
 ss_hdemo_sk ENCODING DELTAVAL, 
 ss_addr_sk ENCODING DELTAVAL, 
 ss_store_sk ENCODING BLOCKDICT_COMP, 
 ss_promo_sk ENCODING DELTAVAL, 
 ss_ticket_number ENCODING DELTARANGE_COMP, 
 ss_quantity ENCODING DELTAVAL, 
 ss_wholesale_cost ENCODING DELTAVAL, 
 ss_list_price ENCODING DELTAVAL, 
 ss_sales_price ENCODING DELTAVAL, 
 ss_ext_discount_amt ENCODING DELTARANGE_COMP, 
 ss_ext_sales_price ENCODING DELTAVAL, 
 ss_ext_wholesale_cost ENCODING DELTAVAL, 
 ss_ext_list_price ENCODING DELTAVAL, 
 ss_ext_tax ENCODING DELTARANGE_COMP, 
 ss_coupon_amt ENCODING DELTARANGE_COMP, 
 ss_net_paid ENCODING DELTAVAL, 
 ss_net_paid_inc_tax ENCODING DELTAVAL, 
 ss_net_profit ENCODING DELTAVAL
)
AS
 SELECT ss_sold_date_sk, 
        ss_sold_time_sk, 
        ss_item_sk, 
        ss_customer_sk, 
        ss_cdemo_sk, 
        ss_hdemo_sk, 
        ss_addr_sk, 
        ss_store_sk, 
        ss_promo_sk, 
        ss_ticket_number, 
        ss_quantity, 
        ss_wholesale_cost, 
        ss_list_price, 
        ss_sales_price, 
        ss_ext_discount_amt, 
        ss_ext_sales_price, 
        ss_ext_wholesale_cost, 
        ss_ext_list_price, 
        ss_ext_tax, 
        ss_coupon_amt, 
        ss_net_paid, 
        ss_net_paid_inc_tax, 
        ss_net_profit
 FROM TPCDS_1T.store_sales 
 ORDER BY ss_item_sk,
          ss_ticket_number
SEGMENTED BY HASH (ss_item_sk, ss_ticket_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS_1T.store_sales_DBD_54_seg_test132_b1 /*+basename(store_sales_DBD_54_seg_test132),createtype(D)*/
(
 ss_sold_date_sk ENCODING DELTAVAL, 
 ss_sold_time_sk ENCODING DELTAVAL, 
 ss_item_sk ENCODING RLE, 
 ss_customer_sk ENCODING DELTAVAL, 
 ss_cdemo_sk ENCODING DELTAVAL, 
 ss_hdemo_sk ENCODING DELTAVAL, 
 ss_addr_sk ENCODING DELTAVAL, 
 ss_store_sk ENCODING BLOCKDICT_COMP, 
 ss_promo_sk ENCODING DELTAVAL, 
 ss_ticket_number ENCODING DELTARANGE_COMP, 
 ss_quantity ENCODING DELTAVAL, 
 ss_wholesale_cost ENCODING DELTAVAL, 
 ss_list_price ENCODING DELTAVAL, 
 ss_sales_price ENCODING DELTAVAL, 
 ss_ext_discount_amt ENCODING DELTARANGE_COMP, 
 ss_ext_sales_price ENCODING DELTAVAL, 
 ss_ext_wholesale_cost ENCODING DELTAVAL, 
 ss_ext_list_price ENCODING DELTAVAL, 
 ss_ext_tax ENCODING DELTARANGE_COMP, 
 ss_coupon_amt ENCODING DELTARANGE_COMP, 
 ss_net_paid ENCODING DELTAVAL, 
 ss_net_paid_inc_tax ENCODING DELTAVAL, 
 ss_net_profit ENCODING DELTAVAL
)
AS
 SELECT ss_sold_date_sk, 
        ss_sold_time_sk, 
        ss_item_sk, 
        ss_customer_sk, 
        ss_cdemo_sk, 
        ss_hdemo_sk, 
        ss_addr_sk, 
        ss_store_sk, 
        ss_promo_sk, 
        ss_ticket_number, 
        ss_quantity, 
        ss_wholesale_cost, 
        ss_list_price, 
        ss_sales_price, 
        ss_ext_discount_amt, 
        ss_ext_sales_price, 
        ss_ext_wholesale_cost, 
        ss_ext_list_price, 
        ss_ext_tax, 
        ss_coupon_amt, 
        ss_net_paid, 
        ss_net_paid_inc_tax, 
        ss_net_profit
 FROM TPCDS_1T.store_sales 
 ORDER BY ss_item_sk,
          ss_ticket_number
SEGMENTED BY HASH (ss_item_sk, ss_ticket_number) ALL NODES OFFSET 1;


\echo NOTICE: The above create projection statement could error out if design created and implemented in the same cluster

CREATE PROJECTION TPCDS_1T.call_center_b0 /*+basename(call_center),createtype(P)*/ 
(
 cc_call_center_sk,
 cc_call_center_id,
 cc_rec_start_date,
 cc_rec_end_date,
 cc_closed_date_sk,
 cc_open_date_sk,
 cc_name,
 cc_class,
 cc_employees,
 cc_sq_ft,
 cc_hours,
 cc_manager,
 cc_mkt_id,
 cc_mkt_class,
 cc_mkt_desc,
 cc_market_manager,
 cc_division,
 cc_division_name,
 cc_company,
 cc_company_name,
 cc_street_number,
 cc_street_name,
 cc_street_type,
 cc_suite_number,
 cc_city,
 cc_county,
 cc_state,
 cc_zip,
 cc_country,
 cc_gmt_offset,
 cc_tax_percentage
)
AS
 SELECT call_center.cc_call_center_sk,
        call_center.cc_call_center_id,
        call_center.cc_rec_start_date,
        call_center.cc_rec_end_date,
        call_center.cc_closed_date_sk,
        call_center.cc_open_date_sk,
        call_center.cc_name,
        call_center.cc_class,
        call_center.cc_employees,
        call_center.cc_sq_ft,
        call_center.cc_hours,
        call_center.cc_manager,
        call_center.cc_mkt_id,
        call_center.cc_mkt_class,
        call_center.cc_mkt_desc,
        call_center.cc_market_manager,
        call_center.cc_division,
        call_center.cc_division_name,
        call_center.cc_company,
        call_center.cc_company_name,
        call_center.cc_street_number,
        call_center.cc_street_name,
        call_center.cc_street_type,
        call_center.cc_suite_number,
        call_center.cc_city,
        call_center.cc_county,
        call_center.cc_state,
        call_center.cc_zip,
        call_center.cc_country,
        call_center.cc_gmt_offset,
        call_center.cc_tax_percentage
 FROM TPCDS_1T.call_center
 ORDER BY call_center.cc_call_center_sk
SEGMENTED BY hash(call_center.cc_call_center_sk) ALL NODES OFFSET 0;



CREATE PROJECTION TPCDS_1T.store_sales_Manual_20_seg_test1 /*+createtype(D)*/ 
(
 ss_sold_date_sk ENCODING RLE,
 ss_sold_time_sk ENCODING DELTAVAL,
 ss_item_sk ENCODING RLE,
 ss_customer_sk ENCODING DELTAVAL,
 ss_cdemo_sk ENCODING DELTAVAL,
 ss_hdemo_sk ENCODING DELTAVAL,
 ss_addr_sk ENCODING DELTAVAL,
 ss_store_sk ENCODING RLE,
 ss_promo_sk ENCODING DELTAVAL,
 ss_ticket_number ENCODING DELTARANGE_COMP,
 ss_quantity ENCODING DELTAVAL,
 ss_wholesale_cost ENCODING DELTAVAL,
 ss_list_price ENCODING DELTAVAL,
 ss_sales_price ENCODING DELTAVAL,
 ss_ext_discount_amt ENCODING DELTARANGE_COMP,
 ss_ext_sales_price ENCODING DELTAVAL,
 ss_ext_wholesale_cost ENCODING DELTAVAL,
 ss_ext_list_price ENCODING DELTAVAL,
 ss_ext_tax ENCODING DELTARANGE_COMP,
 ss_coupon_amt ENCODING DELTARANGE_COMP,
 ss_net_paid ENCODING DELTAVAL,
 ss_net_paid_inc_tax ENCODING DELTAVAL,
 ss_net_profit ENCODING DELTAVAL
)
AS
 SELECT store_sales.ss_sold_date_sk,
        store_sales.ss_sold_time_sk,
        store_sales.ss_item_sk,
        store_sales.ss_customer_sk,
        store_sales.ss_cdemo_sk,
        store_sales.ss_hdemo_sk,
        store_sales.ss_addr_sk,
        store_sales.ss_store_sk,
        store_sales.ss_promo_sk,
        store_sales.ss_ticket_number,
        store_sales.ss_quantity,
        store_sales.ss_wholesale_cost,
        store_sales.ss_list_price,
        store_sales.ss_sales_price,
        store_sales.ss_ext_discount_amt,
        store_sales.ss_ext_sales_price,
        store_sales.ss_ext_wholesale_cost,
        store_sales.ss_ext_list_price,
        store_sales.ss_ext_tax,
        store_sales.ss_coupon_amt,
        store_sales.ss_net_paid,
        store_sales.ss_net_paid_inc_tax,
        store_sales.ss_net_profit
 FROM TPCDS_1T.store_sales
 ORDER BY store_sales.ss_customer_sk,
		  store_sales.ss_sold_date_sk
SEGMENTED BY hash(store_sales.ss_customer_sk) ALL NODES KSAFE 1;

CREATE PROJECTION TPCDS_1T.catalog_sales_Manual_53_seg_test1 /*+createtype(D)*/ 
(
 cs_sold_date_sk ENCODING DELTARANGE_COMP,
 cs_sold_time_sk ENCODING DELTAVAL,
 cs_ship_date_sk ENCODING COMMONDELTA_COMP,
 cs_bill_customer_sk ENCODING DELTAVAL,
 cs_bill_cdemo_sk ENCODING DELTAVAL,
 cs_bill_hdemo_sk ENCODING DELTAVAL,
 cs_bill_addr_sk ENCODING DELTAVAL,
 cs_ship_customer_sk ENCODING DELTAVAL,
 cs_ship_cdemo_sk ENCODING DELTAVAL,
 cs_ship_hdemo_sk ENCODING DELTAVAL,
 cs_ship_addr_sk ENCODING DELTAVAL,
 cs_call_center_sk ENCODING BLOCKDICT_COMP,
 cs_catalog_page_sk ENCODING DELTARANGE_COMP,
 cs_ship_mode_sk ENCODING BLOCKDICT_COMP,
 cs_warehouse_sk ENCODING BLOCKDICT_COMP,
 cs_item_sk ENCODING RLE,
 cs_promo_sk ENCODING DELTAVAL,
 cs_order_number ENCODING DELTARANGE_COMP,
 cs_quantity ENCODING DELTAVAL,
 cs_wholesale_cost ENCODING DELTAVAL,
 cs_list_price ENCODING DELTAVAL,
 cs_sales_price ENCODING DELTAVAL,
 cs_ext_discount_amt ENCODING DELTARANGE_COMP,
 cs_ext_sales_price ENCODING DELTARANGE_COMP,
 cs_ext_wholesale_cost ENCODING DELTAVAL,
 cs_ext_list_price ENCODING DELTAVAL,
 cs_ext_tax ENCODING DELTARANGE_COMP,
 cs_coupon_amt ENCODING DELTARANGE_COMP,
 cs_ext_ship_cost ENCODING DELTARANGE_COMP,
 cs_net_paid ENCODING DELTARANGE_COMP,
 cs_net_paid_inc_tax ENCODING DELTARANGE_COMP,
 cs_net_paid_inc_ship ENCODING DELTARANGE_COMP,
 cs_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP,
 cs_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT catalog_sales.cs_sold_date_sk,
        catalog_sales.cs_sold_time_sk,
        catalog_sales.cs_ship_date_sk,
        catalog_sales.cs_bill_customer_sk,
        catalog_sales.cs_bill_cdemo_sk,
        catalog_sales.cs_bill_hdemo_sk,
        catalog_sales.cs_bill_addr_sk,
        catalog_sales.cs_ship_customer_sk,
        catalog_sales.cs_ship_cdemo_sk,
        catalog_sales.cs_ship_hdemo_sk,
        catalog_sales.cs_ship_addr_sk,
        catalog_sales.cs_call_center_sk,
        catalog_sales.cs_catalog_page_sk,
        catalog_sales.cs_ship_mode_sk,
        catalog_sales.cs_warehouse_sk,
        catalog_sales.cs_item_sk,
        catalog_sales.cs_promo_sk,
        catalog_sales.cs_order_number,
        catalog_sales.cs_quantity,
        catalog_sales.cs_wholesale_cost,
        catalog_sales.cs_list_price,
        catalog_sales.cs_sales_price,
        catalog_sales.cs_ext_discount_amt,
        catalog_sales.cs_ext_sales_price,
        catalog_sales.cs_ext_wholesale_cost,
        catalog_sales.cs_ext_list_price,
        catalog_sales.cs_ext_tax,
        catalog_sales.cs_coupon_amt,
        catalog_sales.cs_ext_ship_cost,
        catalog_sales.cs_net_paid,
        catalog_sales.cs_net_paid_inc_tax,
        catalog_sales.cs_net_paid_inc_ship,
        catalog_sales.cs_net_paid_inc_ship_tax,
        catalog_sales.cs_net_profit
 FROM TPCDS_1T.catalog_sales
 ORDER BY catalog_sales.cs_bill_customer_sk,
          catalog_sales.cs_sold_date_sk
SEGMENTED BY hash(catalog_sales.cs_bill_customer_sk) ALL NODES KSAFE 1;

--DROP PROJECTION TPCDS_1T.store_sales_Manual_201_seg_test1;
CREATE PROJECTION TPCDS_1T.store_sales_Manual_201_seg_test1 /*+createtype(D)*/ 
(
 ss_sold_date_sk ENCODING RLE,
 ss_sold_time_sk ENCODING DELTAVAL,
 ss_item_sk ENCODING RLE,
 ss_customer_sk ENCODING DELTAVAL,
 ss_cdemo_sk ENCODING DELTAVAL,
 ss_hdemo_sk ENCODING DELTAVAL,
 ss_addr_sk ENCODING DELTAVAL,
 ss_store_sk ENCODING RLE,
 ss_promo_sk ENCODING DELTAVAL,
 ss_ticket_number ENCODING DELTARANGE_COMP,
 ss_quantity ENCODING DELTAVAL,
 ss_wholesale_cost ENCODING DELTAVAL,
 ss_list_price ENCODING DELTAVAL,
 ss_sales_price ENCODING DELTAVAL,
 ss_ext_discount_amt ENCODING DELTARANGE_COMP,
 ss_ext_sales_price ENCODING DELTAVAL,
 ss_ext_wholesale_cost ENCODING DELTAVAL,
 ss_ext_list_price ENCODING DELTAVAL,
 ss_ext_tax ENCODING DELTARANGE_COMP,
 ss_coupon_amt ENCODING DELTARANGE_COMP,
 ss_net_paid ENCODING DELTAVAL,
 ss_net_paid_inc_tax ENCODING DELTAVAL,
 ss_net_profit ENCODING DELTAVAL
)
AS
 SELECT store_sales.ss_sold_date_sk,
        store_sales.ss_sold_time_sk,
        store_sales.ss_item_sk,
        store_sales.ss_customer_sk,
        store_sales.ss_cdemo_sk,
        store_sales.ss_hdemo_sk,
        store_sales.ss_addr_sk,
        store_sales.ss_store_sk,
        store_sales.ss_promo_sk,
        store_sales.ss_ticket_number,
        store_sales.ss_quantity,
        store_sales.ss_wholesale_cost,
        store_sales.ss_list_price,
        store_sales.ss_sales_price,
        store_sales.ss_ext_discount_amt,
        store_sales.ss_ext_sales_price,
        store_sales.ss_ext_wholesale_cost,
        store_sales.ss_ext_list_price,
        store_sales.ss_ext_tax,
        store_sales.ss_coupon_amt,
        store_sales.ss_net_paid,
        store_sales.ss_net_paid_inc_tax,
        store_sales.ss_net_profit
 FROM TPCDS_1T.store_sales
 ORDER BY store_sales.ss_item_sk,
		  store_sales.ss_ticket_number,
		  store_sales.ss_sold_date_sk
SEGMENTED BY hash(store_sales.ss_item_sk, store_sales.ss_ticket_number) ALL NODES KSAFE 1;


--q75 web_sales, web_returns
--drop PROJECTION TPCDS_1T.web_sales_Manual_17_seg_test1;
CREATE PROJECTION TPCDS_1T.web_sales_Manual_17_seg_test1 /*+createtype(D)*/ 
(
 ws_sold_date_sk ENCODING RLE,
 ws_sold_time_sk ENCODING DELTAVAL,
 ws_ship_date_sk ENCODING DELTAVAL,
 ws_item_sk ENCODING RLE,
 ws_bill_customer_sk ENCODING DELTAVAL,
 ws_bill_cdemo_sk ENCODING DELTAVAL,
 ws_bill_hdemo_sk ENCODING DELTAVAL,
 ws_bill_addr_sk ENCODING DELTAVAL,
 ws_ship_customer_sk ENCODING DELTAVAL,
 ws_ship_cdemo_sk ENCODING DELTAVAL,
 ws_ship_hdemo_sk ENCODING DELTAVAL,
 ws_ship_addr_sk ENCODING DELTAVAL,
 ws_web_page_sk ENCODING BLOCKDICT_COMP,
 ws_web_site_sk ENCODING BLOCKDICT_COMP,
 ws_ship_mode_sk ENCODING BLOCKDICT_COMP,
 ws_warehouse_sk ENCODING RLE,
 ws_promo_sk ENCODING DELTAVAL,
 ws_order_number ENCODING DELTARANGE_COMP,
 ws_quantity ENCODING DELTAVAL,
 ws_wholesale_cost ENCODING DELTAVAL,
 ws_list_price ENCODING DELTAVAL,
 ws_sales_price ENCODING DELTAVAL,
 ws_ext_discount_amt ENCODING DELTARANGE_COMP,
 ws_ext_sales_price ENCODING DELTARANGE_COMP,
 ws_ext_wholesale_cost ENCODING DELTAVAL,
 ws_ext_list_price ENCODING DELTAVAL,
 ws_ext_tax ENCODING DELTARANGE_COMP,
 ws_coupon_amt ENCODING DELTARANGE_COMP,
 ws_ext_ship_cost ENCODING DELTARANGE_COMP,
 ws_net_paid ENCODING DELTARANGE_COMP,
 ws_net_paid_inc_tax ENCODING DELTARANGE_COMP,
 ws_net_paid_inc_ship ENCODING DELTARANGE_COMP,
 ws_net_paid_inc_ship_tax ENCODING DELTARANGE_COMP,
 ws_net_profit ENCODING DELTARANGE_COMP
)
AS
 SELECT web_sales.ws_sold_date_sk,
        web_sales.ws_sold_time_sk,
        web_sales.ws_ship_date_sk,
        web_sales.ws_item_sk,
        web_sales.ws_bill_customer_sk,
        web_sales.ws_bill_cdemo_sk,
        web_sales.ws_bill_hdemo_sk,
        web_sales.ws_bill_addr_sk,
        web_sales.ws_ship_customer_sk,
        web_sales.ws_ship_cdemo_sk,
        web_sales.ws_ship_hdemo_sk,
        web_sales.ws_ship_addr_sk,
        web_sales.ws_web_page_sk,
        web_sales.ws_web_site_sk,
        web_sales.ws_ship_mode_sk,
        web_sales.ws_warehouse_sk,
        web_sales.ws_promo_sk,
        web_sales.ws_order_number,
        web_sales.ws_quantity,
        web_sales.ws_wholesale_cost,
        web_sales.ws_list_price,
        web_sales.ws_sales_price,
        web_sales.ws_ext_discount_amt,
        web_sales.ws_ext_sales_price,
        web_sales.ws_ext_wholesale_cost,
        web_sales.ws_ext_list_price,
        web_sales.ws_ext_tax,
        web_sales.ws_coupon_amt,
        web_sales.ws_ext_ship_cost,
        web_sales.ws_net_paid,
        web_sales.ws_net_paid_inc_tax,
        web_sales.ws_net_paid_inc_ship,
        web_sales.ws_net_paid_inc_ship_tax,
        web_sales.ws_net_profit
 FROM TPCDS_1T.web_sales
 ORDER BY web_sales.ws_item_sk,
          web_sales.ws_order_number,
          web_sales.ws_sold_date_sk
SEGMENTED BY hash(web_sales.ws_item_sk, web_sales.ws_order_number) ALL NODES KSAFE 1;


--select refresh('TPCDS_1T.web_sales,TPCDS_1T.store_sales, TPCDS_1T.catalog_sales');
--select make_ahm_now();



