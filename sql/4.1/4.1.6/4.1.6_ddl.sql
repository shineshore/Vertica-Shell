CREATE SCHEMA TPCDS;


CREATE TABLE TPCDS.dbgen_version
(
    dv_version varchar(16),
    dv_create_date date,
    dv_create_time time,
    dv_cmdline_args varchar(200)
);


CREATE TABLE TPCDS.customer_address
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


CREATE TABLE TPCDS.customer_demographics
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


CREATE TABLE TPCDS.date_dim
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


CREATE TABLE TPCDS.warehouse
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


CREATE TABLE TPCDS.ship_mode
(
    sm_ship_mode_sk int NOT NULL,
    sm_ship_mode_id char(16) NOT NULL,
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20)
);


CREATE TABLE TPCDS.time_dim
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


CREATE TABLE TPCDS.reason
(
    r_reason_sk int NOT NULL,
    r_reason_id char(16) NOT NULL,
    r_reason_desc char(100)
);


CREATE TABLE TPCDS.income_band
(
    ib_income_band_sk int NOT NULL,
    ib_lower_bound int,
    ib_upper_bound int
);


CREATE TABLE TPCDS.item
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


CREATE TABLE TPCDS.store
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


CREATE TABLE TPCDS.call_center
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


CREATE TABLE TPCDS.customer
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


CREATE TABLE TPCDS.web_site
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


CREATE TABLE TPCDS.store_returns
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


CREATE TABLE TPCDS.household_demographics
(
    hd_demo_sk int NOT NULL,
    hd_income_band_sk int,
    hd_buy_potential char(15),
    hd_dep_count int,
    hd_vehicle_count int
);


CREATE TABLE TPCDS.web_page
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


CREATE TABLE TPCDS.promotion
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


CREATE TABLE TPCDS.catalog_page
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


CREATE TABLE TPCDS.inventory
(
    inv_date_sk int NOT NULL,
    inv_item_sk int NOT NULL,
    inv_warehouse_sk int NOT NULL,
    inv_quantity_on_hand int
);


CREATE TABLE TPCDS.catalog_returns
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


CREATE TABLE TPCDS.web_returns
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


CREATE TABLE TPCDS.web_sales
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


CREATE TABLE TPCDS.catalog_sales
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


CREATE TABLE TPCDS.store_sales
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


CREATE PROJECTION TPCDS.dbgen_version_DBD_1_seg_test132_b0 /*+basename(dbgen_version_DBD_1_seg_test132),createtype(D)*/ 
(
 dv_version,
 dv_create_date,
 dv_create_time,
 dv_cmdline_args
)
AS
 SELECT dbgen_version.dv_version,
        dbgen_version.dv_create_date,
        dbgen_version.dv_create_time,
        dbgen_version.dv_cmdline_args
 FROM TPCDS.dbgen_version
 ORDER BY dbgen_version.dv_version
SEGMENTED BY hash(dbgen_version.dv_version) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.dbgen_version_DBD_1_seg_test132_b1 /*+basename(dbgen_version_DBD_1_seg_test132),createtype(D)*/ 
(
 dv_version,
 dv_create_date,
 dv_create_time,
 dv_cmdline_args
)
AS
 SELECT dbgen_version.dv_version,
        dbgen_version.dv_create_date,
        dbgen_version.dv_create_time,
        dbgen_version.dv_cmdline_args
 FROM TPCDS.dbgen_version
 ORDER BY dbgen_version.dv_version
SEGMENTED BY hash(dbgen_version.dv_version) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.customer_address_DBD_2_rep_test132
(
 ca_address_sk ENCODING DELTARANGE_COMP,
 ca_address_id,
 ca_street_number,
 ca_street_name,
 ca_street_type ENCODING RLE,
 ca_suite_number,
 ca_city,
 ca_county,
 ca_state ENCODING RLE,
 ca_zip,
 ca_country ENCODING RLE,
 ca_gmt_offset ENCODING RLE,
 ca_location_type ENCODING RLE
)
AS
 SELECT customer_address.ca_address_sk,
        customer_address.ca_address_id,
        customer_address.ca_street_number,
        customer_address.ca_street_name,
        customer_address.ca_street_type,
        customer_address.ca_suite_number,
        customer_address.ca_city,
        customer_address.ca_county,
        customer_address.ca_state,
        customer_address.ca_zip,
        customer_address.ca_country,
        customer_address.ca_gmt_offset,
        customer_address.ca_location_type
 FROM TPCDS.customer_address
 ORDER BY customer_address.ca_state,
          customer_address.ca_gmt_offset,
          customer_address.ca_country,
          customer_address.ca_location_type,
          customer_address.ca_street_type,
          customer_address.ca_address_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.customer_demographics_DBD_41_seg_test132_b0 /*+basename(customer_demographics_DBD_41_seg_test132),createtype(D)*/ 
(
 cd_demo_sk ENCODING COMMONDELTA_COMP,
 cd_gender,
 cd_marital_status,
 cd_education_status,
 cd_purchase_estimate ENCODING COMMONDELTA_COMP,
 cd_credit_rating,
 cd_dep_count ENCODING COMMONDELTA_COMP,
 cd_dep_employed_count ENCODING RLE,
 cd_dep_college_count ENCODING RLE
)
AS
 SELECT customer_demographics.cd_demo_sk,
        customer_demographics.cd_gender,
        customer_demographics.cd_marital_status,
        customer_demographics.cd_education_status,
        customer_demographics.cd_purchase_estimate,
        customer_demographics.cd_credit_rating,
        customer_demographics.cd_dep_count,
        customer_demographics.cd_dep_employed_count,
        customer_demographics.cd_dep_college_count
 FROM TPCDS.customer_demographics
 ORDER BY customer_demographics.cd_demo_sk
SEGMENTED BY hash(customer_demographics.cd_demo_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.customer_demographics_DBD_41_seg_test132_b1 /*+basename(customer_demographics_DBD_41_seg_test132),createtype(D)*/ 
(
 cd_demo_sk ENCODING COMMONDELTA_COMP,
 cd_gender,
 cd_marital_status,
 cd_education_status,
 cd_purchase_estimate ENCODING COMMONDELTA_COMP,
 cd_credit_rating,
 cd_dep_count ENCODING COMMONDELTA_COMP,
 cd_dep_employed_count ENCODING RLE,
 cd_dep_college_count ENCODING RLE
)
AS
 SELECT customer_demographics.cd_demo_sk,
        customer_demographics.cd_gender,
        customer_demographics.cd_marital_status,
        customer_demographics.cd_education_status,
        customer_demographics.cd_purchase_estimate,
        customer_demographics.cd_credit_rating,
        customer_demographics.cd_dep_count,
        customer_demographics.cd_dep_employed_count,
        customer_demographics.cd_dep_college_count
 FROM TPCDS.customer_demographics
 ORDER BY customer_demographics.cd_demo_sk
SEGMENTED BY hash(customer_demographics.cd_demo_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.date_dim_DBD_3_rep_test132
(
 d_date_sk ENCODING COMMONDELTA_COMP,
 d_date_id,
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
 d_day_name,
 d_quarter_name,
 d_holiday,
 d_weekend,
 d_following_holiday,
 d_first_dom ENCODING COMMONDELTA_COMP,
 d_last_dom ENCODING COMMONDELTA_COMP,
 d_same_day_ly ENCODING COMMONDELTA_COMP,
 d_same_day_lq ENCODING COMMONDELTA_COMP,
 d_current_day,
 d_current_week,
 d_current_month,
 d_current_quarter,
 d_current_year
)
AS
 SELECT date_dim.d_date_sk,
        date_dim.d_date_id,
        date_dim.d_date,
        date_dim.d_month_seq,
        date_dim.d_week_seq,
        date_dim.d_quarter_seq,
        date_dim.d_year,
        date_dim.d_dow,
        date_dim.d_moy,
        date_dim.d_dom,
        date_dim.d_qoy,
        date_dim.d_fy_year,
        date_dim.d_fy_quarter_seq,
        date_dim.d_fy_week_seq,
        date_dim.d_day_name,
        date_dim.d_quarter_name,
        date_dim.d_holiday,
        date_dim.d_weekend,
        date_dim.d_following_holiday,
        date_dim.d_first_dom,
        date_dim.d_last_dom,
        date_dim.d_same_day_ly,
        date_dim.d_same_day_lq,
        date_dim.d_current_day,
        date_dim.d_current_week,
        date_dim.d_current_month,
        date_dim.d_current_quarter,
        date_dim.d_current_year
 FROM TPCDS.date_dim
 ORDER BY date_dim.d_year,
          date_dim.d_date_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.date_dim_DBD_42_seg_test132_b0 /*+basename(date_dim_DBD_42_seg_test132),createtype(D)*/ 
(
 d_date_sk ENCODING COMMONDELTA_COMP,
 d_date_id,
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
 d_day_name,
 d_quarter_name,
 d_holiday,
 d_weekend,
 d_following_holiday,
 d_first_dom ENCODING COMMONDELTA_COMP,
 d_last_dom ENCODING COMMONDELTA_COMP,
 d_same_day_ly ENCODING COMMONDELTA_COMP,
 d_same_day_lq ENCODING COMMONDELTA_COMP,
 d_current_day,
 d_current_week,
 d_current_month,
 d_current_quarter,
 d_current_year
)
AS
 SELECT date_dim.d_date_sk,
        date_dim.d_date_id,
        date_dim.d_date,
        date_dim.d_month_seq,
        date_dim.d_week_seq,
        date_dim.d_quarter_seq,
        date_dim.d_year,
        date_dim.d_dow,
        date_dim.d_moy,
        date_dim.d_dom,
        date_dim.d_qoy,
        date_dim.d_fy_year,
        date_dim.d_fy_quarter_seq,
        date_dim.d_fy_week_seq,
        date_dim.d_day_name,
        date_dim.d_quarter_name,
        date_dim.d_holiday,
        date_dim.d_weekend,
        date_dim.d_following_holiday,
        date_dim.d_first_dom,
        date_dim.d_last_dom,
        date_dim.d_same_day_ly,
        date_dim.d_same_day_lq,
        date_dim.d_current_day,
        date_dim.d_current_week,
        date_dim.d_current_month,
        date_dim.d_current_quarter,
        date_dim.d_current_year
 FROM TPCDS.date_dim
 ORDER BY date_dim.d_date_sk
SEGMENTED BY hash(date_dim.d_date_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.date_dim_DBD_42_seg_test132_b1 /*+basename(date_dim_DBD_42_seg_test132),createtype(D)*/ 
(
 d_date_sk ENCODING COMMONDELTA_COMP,
 d_date_id,
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
 d_day_name,
 d_quarter_name,
 d_holiday,
 d_weekend,
 d_following_holiday,
 d_first_dom ENCODING COMMONDELTA_COMP,
 d_last_dom ENCODING COMMONDELTA_COMP,
 d_same_day_ly ENCODING COMMONDELTA_COMP,
 d_same_day_lq ENCODING COMMONDELTA_COMP,
 d_current_day,
 d_current_week,
 d_current_month,
 d_current_quarter,
 d_current_year
)
AS
 SELECT date_dim.d_date_sk,
        date_dim.d_date_id,
        date_dim.d_date,
        date_dim.d_month_seq,
        date_dim.d_week_seq,
        date_dim.d_quarter_seq,
        date_dim.d_year,
        date_dim.d_dow,
        date_dim.d_moy,
        date_dim.d_dom,
        date_dim.d_qoy,
        date_dim.d_fy_year,
        date_dim.d_fy_quarter_seq,
        date_dim.d_fy_week_seq,
        date_dim.d_day_name,
        date_dim.d_quarter_name,
        date_dim.d_holiday,
        date_dim.d_weekend,
        date_dim.d_following_holiday,
        date_dim.d_first_dom,
        date_dim.d_last_dom,
        date_dim.d_same_day_ly,
        date_dim.d_same_day_lq,
        date_dim.d_current_day,
        date_dim.d_current_week,
        date_dim.d_current_month,
        date_dim.d_current_quarter,
        date_dim.d_current_year
 FROM TPCDS.date_dim
 ORDER BY date_dim.d_date_sk
SEGMENTED BY hash(date_dim.d_date_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.ship_mode_DBD_4_rep_test132
(
 sm_ship_mode_sk ENCODING COMMONDELTA_COMP,
 sm_ship_mode_id,
 sm_type,
 sm_code,
 sm_carrier,
 sm_contract
)
AS
 SELECT ship_mode.sm_ship_mode_sk,
        ship_mode.sm_ship_mode_id,
        ship_mode.sm_type,
        ship_mode.sm_code,
        ship_mode.sm_carrier,
        ship_mode.sm_contract
 FROM TPCDS.ship_mode
 ORDER BY ship_mode.sm_ship_mode_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.time_dim_DBD_5_rep_test132
(
 t_time_sk ENCODING COMMONDELTA_COMP,
 t_time_id,
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
 SELECT time_dim.t_time_sk,
        time_dim.t_time_id,
        time_dim.t_time,
        time_dim.t_hour,
        time_dim.t_minute,
        time_dim.t_second,
        time_dim.t_am_pm,
        time_dim.t_shift,
        time_dim.t_sub_shift,
        time_dim.t_meal_time
 FROM TPCDS.time_dim
 ORDER BY time_dim.t_am_pm,
          time_dim.t_shift,
          time_dim.t_sub_shift,
          time_dim.t_meal_time,
          time_dim.t_hour,
          time_dim.t_time_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.reason_DBD_6_rep_test132
(
 r_reason_sk ENCODING COMMONDELTA_COMP,
 r_reason_id,
 r_reason_desc
)
AS
 SELECT reason.r_reason_sk,
        reason.r_reason_id,
        reason.r_reason_desc
 FROM TPCDS.reason
 ORDER BY reason.r_reason_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.income_band_DBD_7_rep_test132
(
 ib_income_band_sk ENCODING COMMONDELTA_COMP,
 ib_lower_bound ENCODING COMMONDELTA_COMP,
 ib_upper_bound ENCODING COMMONDELTA_COMP
)
AS
 SELECT income_band.ib_income_band_sk,
        income_band.ib_lower_bound,
        income_band.ib_upper_bound
 FROM TPCDS.income_band
 ORDER BY income_band.ib_income_band_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.item_DBD_44_seg_test132_b0 /*+basename(item_DBD_44_seg_test132),createtype(D)*/ 
(
 i_item_sk ENCODING COMMONDELTA_COMP,
 i_item_id,
 i_rec_start_date ENCODING BLOCKDICT_COMP,
 i_rec_end_date ENCODING BLOCKDICT_COMP,
 i_item_desc,
 i_current_price ENCODING DELTARANGE_COMP,
 i_wholesale_cost ENCODING DELTARANGE_COMP,
 i_brand_id ENCODING BLOCKDICT_COMP,
 i_brand,
 i_class_id ENCODING BLOCKDICT_COMP,
 i_class,
 i_category_id ENCODING BLOCKDICT_COMP,
 i_category,
 i_manufact_id ENCODING DELTAVAL,
 i_manufact,
 i_size,
 i_formulation,
 i_color,
 i_units,
 i_container,
 i_manager_id ENCODING DELTAVAL,
 i_product_name
)
AS
 SELECT item.i_item_sk,
        item.i_item_id,
        item.i_rec_start_date,
        item.i_rec_end_date,
        item.i_item_desc,
        item.i_current_price,
        item.i_wholesale_cost,
        item.i_brand_id,
        item.i_brand,
        item.i_class_id,
        item.i_class,
        item.i_category_id,
        item.i_category,
        item.i_manufact_id,
        item.i_manufact,
        item.i_size,
        item.i_formulation,
        item.i_color,
        item.i_units,
        item.i_container,
        item.i_manager_id,
        item.i_product_name
 FROM TPCDS.item
 ORDER BY item.i_item_sk
SEGMENTED BY hash(item.i_item_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.item_DBD_44_seg_test132_b1 /*+basename(item_DBD_44_seg_test132),createtype(D)*/ 
(
 i_item_sk ENCODING COMMONDELTA_COMP,
 i_item_id,
 i_rec_start_date ENCODING BLOCKDICT_COMP,
 i_rec_end_date ENCODING BLOCKDICT_COMP,
 i_item_desc,
 i_current_price ENCODING DELTARANGE_COMP,
 i_wholesale_cost ENCODING DELTARANGE_COMP,
 i_brand_id ENCODING BLOCKDICT_COMP,
 i_brand,
 i_class_id ENCODING BLOCKDICT_COMP,
 i_class,
 i_category_id ENCODING BLOCKDICT_COMP,
 i_category,
 i_manufact_id ENCODING DELTAVAL,
 i_manufact,
 i_size,
 i_formulation,
 i_color,
 i_units,
 i_container,
 i_manager_id ENCODING DELTAVAL,
 i_product_name
)
AS
 SELECT item.i_item_sk,
        item.i_item_id,
        item.i_rec_start_date,
        item.i_rec_end_date,
        item.i_item_desc,
        item.i_current_price,
        item.i_wholesale_cost,
        item.i_brand_id,
        item.i_brand,
        item.i_class_id,
        item.i_class,
        item.i_category_id,
        item.i_category,
        item.i_manufact_id,
        item.i_manufact,
        item.i_size,
        item.i_formulation,
        item.i_color,
        item.i_units,
        item.i_container,
        item.i_manager_id,
        item.i_product_name
 FROM TPCDS.item
 ORDER BY item.i_item_sk
SEGMENTED BY hash(item.i_item_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.item_DBD_8_rep_test132
(
 i_item_sk ENCODING COMMONDELTA_COMP,
 i_item_id,
 i_rec_start_date ENCODING BLOCKDICT_COMP,
 i_rec_end_date ENCODING BLOCKDICT_COMP,
 i_item_desc,
 i_current_price ENCODING DELTARANGE_COMP,
 i_wholesale_cost ENCODING DELTARANGE_COMP,
 i_brand_id ENCODING BLOCKDICT_COMP,
 i_brand,
 i_class_id ENCODING BLOCKDICT_COMP,
 i_class,
 i_category_id ENCODING RLE,
 i_category ENCODING RLE,
 i_manufact_id ENCODING DELTAVAL,
 i_manufact,
 i_size,
 i_formulation,
 i_color,
 i_units,
 i_container,
 i_manager_id ENCODING DELTAVAL,
 i_product_name
)
AS
 SELECT item.i_item_sk,
        item.i_item_id,
        item.i_rec_start_date,
        item.i_rec_end_date,
        item.i_item_desc,
        item.i_current_price,
        item.i_wholesale_cost,
        item.i_brand_id,
        item.i_brand,
        item.i_class_id,
        item.i_class,
        item.i_category_id,
        item.i_category,
        item.i_manufact_id,
        item.i_manufact,
        item.i_size,
        item.i_formulation,
        item.i_color,
        item.i_units,
        item.i_container,
        item.i_manager_id,
        item.i_product_name
 FROM TPCDS.item
 ORDER BY item.i_category,
          item.i_item_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.item_DBD_9_rep_test132
(
 i_item_sk ENCODING DELTAVAL,
 i_item_id,
 i_current_price ENCODING RLE
)
AS
 SELECT item.i_item_sk,
        item.i_item_id,
        item.i_current_price
 FROM TPCDS.item
 ORDER BY item.i_current_price,
          item.i_item_id
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.store_DBD_45_seg_test132_b0 /*+basename(store_DBD_45_seg_test132),createtype(D)*/ 
(
 s_store_sk,
 s_store_id,
 s_rec_start_date,
 s_rec_end_date ENCODING BLOCKDICT_COMP,
 s_closed_date_sk ENCODING BLOCKDICT_COMP,
 s_store_name,
 s_number_employees,
 s_floor_space,
 s_hours,
 s_manager,
 s_market_id,
 s_geography_class,
 s_market_desc,
 s_market_manager,
 s_division_id ENCODING COMMONDELTA_COMP,
 s_division_name,
 s_company_id ENCODING COMMONDELTA_COMP,
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
 s_gmt_offset ENCODING COMMONDELTA_COMP,
 s_tax_precentage
)
AS
 SELECT store.s_store_sk,
        store.s_store_id,
        store.s_rec_start_date,
        store.s_rec_end_date,
        store.s_closed_date_sk,
        store.s_store_name,
        store.s_number_employees,
        store.s_floor_space,
        store.s_hours,
        store.s_manager,
        store.s_market_id,
        store.s_geography_class,
        store.s_market_desc,
        store.s_market_manager,
        store.s_division_id,
        store.s_division_name,
        store.s_company_id,
        store.s_company_name,
        store.s_street_number,
        store.s_street_name,
        store.s_street_type,
        store.s_suite_number,
        store.s_city,
        store.s_county,
        store.s_state,
        store.s_zip,
        store.s_country,
        store.s_gmt_offset,
        store.s_tax_precentage
 FROM TPCDS.store
 ORDER BY store.s_store_sk
SEGMENTED BY hash(store.s_store_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.store_DBD_45_seg_test132_b1 /*+basename(store_DBD_45_seg_test132),createtype(D)*/ 
(
 s_store_sk,
 s_store_id,
 s_rec_start_date,
 s_rec_end_date ENCODING BLOCKDICT_COMP,
 s_closed_date_sk ENCODING BLOCKDICT_COMP,
 s_store_name,
 s_number_employees,
 s_floor_space,
 s_hours,
 s_manager,
 s_market_id,
 s_geography_class,
 s_market_desc,
 s_market_manager,
 s_division_id ENCODING COMMONDELTA_COMP,
 s_division_name,
 s_company_id ENCODING COMMONDELTA_COMP,
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
 s_gmt_offset ENCODING COMMONDELTA_COMP,
 s_tax_precentage
)
AS
 SELECT store.s_store_sk,
        store.s_store_id,
        store.s_rec_start_date,
        store.s_rec_end_date,
        store.s_closed_date_sk,
        store.s_store_name,
        store.s_number_employees,
        store.s_floor_space,
        store.s_hours,
        store.s_manager,
        store.s_market_id,
        store.s_geography_class,
        store.s_market_desc,
        store.s_market_manager,
        store.s_division_id,
        store.s_division_name,
        store.s_company_id,
        store.s_company_name,
        store.s_street_number,
        store.s_street_name,
        store.s_street_type,
        store.s_suite_number,
        store.s_city,
        store.s_county,
        store.s_state,
        store.s_zip,
        store.s_country,
        store.s_gmt_offset,
        store.s_tax_precentage
 FROM TPCDS.store
 ORDER BY store.s_store_sk
SEGMENTED BY hash(store.s_store_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.customer_DBD_10_rep_test132
(
 c_customer_sk ENCODING COMMONDELTA_COMP,
 c_customer_id,
 c_current_cdemo_sk ENCODING DELTAVAL,
 c_current_hdemo_sk ENCODING DELTAVAL,
 c_current_addr_sk ENCODING DELTAVAL,
 c_first_shipto_date_sk ENCODING DELTAVAL,
 c_first_sales_date_sk ENCODING DELTAVAL,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day ENCODING DELTAVAL,
 c_birth_month ENCODING BLOCKDICT_COMP,
 c_birth_year ENCODING BLOCKDICT_COMP,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date
)
AS
 SELECT customer.c_customer_sk,
        customer.c_customer_id,
        customer.c_current_cdemo_sk,
        customer.c_current_hdemo_sk,
        customer.c_current_addr_sk,
        customer.c_first_shipto_date_sk,
        customer.c_first_sales_date_sk,
        customer.c_salutation,
        customer.c_first_name,
        customer.c_last_name,
        customer.c_preferred_cust_flag,
        customer.c_birth_day,
        customer.c_birth_month,
        customer.c_birth_year,
        customer.c_birth_country,
        customer.c_login,
        customer.c_email_address,
        customer.c_last_review_date
 FROM TPCDS.customer
 ORDER BY customer.c_customer_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.customer_DBD_47_seg_test132_b0 /*+basename(customer_DBD_47_seg_test132),createtype(D)*/ 
(
 c_customer_sk ENCODING COMMONDELTA_COMP,
 c_customer_id,
 c_current_cdemo_sk ENCODING DELTAVAL,
 c_current_hdemo_sk ENCODING DELTAVAL,
 c_current_addr_sk ENCODING DELTAVAL,
 c_first_shipto_date_sk ENCODING DELTAVAL,
 c_first_sales_date_sk ENCODING DELTAVAL,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day ENCODING DELTAVAL,
 c_birth_month ENCODING BLOCKDICT_COMP,
 c_birth_year ENCODING BLOCKDICT_COMP,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date
)
AS
 SELECT customer.c_customer_sk,
        customer.c_customer_id,
        customer.c_current_cdemo_sk,
        customer.c_current_hdemo_sk,
        customer.c_current_addr_sk,
        customer.c_first_shipto_date_sk,
        customer.c_first_sales_date_sk,
        customer.c_salutation,
        customer.c_first_name,
        customer.c_last_name,
        customer.c_preferred_cust_flag,
        customer.c_birth_day,
        customer.c_birth_month,
        customer.c_birth_year,
        customer.c_birth_country,
        customer.c_login,
        customer.c_email_address,
        customer.c_last_review_date
 FROM TPCDS.customer
 ORDER BY customer.c_customer_sk
SEGMENTED BY hash(customer.c_customer_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.customer_DBD_47_seg_test132_b1 /*+basename(customer_DBD_47_seg_test132),createtype(D)*/ 
(
 c_customer_sk ENCODING COMMONDELTA_COMP,
 c_customer_id,
 c_current_cdemo_sk ENCODING DELTAVAL,
 c_current_hdemo_sk ENCODING DELTAVAL,
 c_current_addr_sk ENCODING DELTAVAL,
 c_first_shipto_date_sk ENCODING DELTAVAL,
 c_first_sales_date_sk ENCODING DELTAVAL,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day ENCODING DELTAVAL,
 c_birth_month ENCODING BLOCKDICT_COMP,
 c_birth_year ENCODING BLOCKDICT_COMP,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date
)
AS
 SELECT customer.c_customer_sk,
        customer.c_customer_id,
        customer.c_current_cdemo_sk,
        customer.c_current_hdemo_sk,
        customer.c_current_addr_sk,
        customer.c_first_shipto_date_sk,
        customer.c_first_sales_date_sk,
        customer.c_salutation,
        customer.c_first_name,
        customer.c_last_name,
        customer.c_preferred_cust_flag,
        customer.c_birth_day,
        customer.c_birth_month,
        customer.c_birth_year,
        customer.c_birth_country,
        customer.c_login,
        customer.c_email_address,
        customer.c_last_review_date
 FROM TPCDS.customer
 ORDER BY customer.c_customer_sk
SEGMENTED BY hash(customer.c_customer_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.web_site_DBD_11_rep_test132
(
 web_site_sk ENCODING COMMONDELTA_COMP,
 web_site_id,
 web_rec_start_date ENCODING RLE,
 web_rec_end_date ENCODING BLOCKDICT_COMP,
 web_name,
 web_open_date_sk ENCODING COMMONDELTA_COMP,
 web_close_date_sk ENCODING DELTAVAL,
 web_class ENCODING RLE,
 web_manager,
 web_mkt_id ENCODING COMMONDELTA_COMP,
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
 web_country ENCODING RLE,
 web_gmt_offset ENCODING COMMONDELTA_COMP,
 web_tax_percentage
)
AS
 SELECT web_site.web_site_sk,
        web_site.web_site_id,
        web_site.web_rec_start_date,
        web_site.web_rec_end_date,
        web_site.web_name,
        web_site.web_open_date_sk,
        web_site.web_close_date_sk,
        web_site.web_class,
        web_site.web_manager,
        web_site.web_mkt_id,
        web_site.web_mkt_class,
        web_site.web_mkt_desc,
        web_site.web_market_manager,
        web_site.web_company_id,
        web_site.web_company_name,
        web_site.web_street_number,
        web_site.web_street_name,
        web_site.web_street_type,
        web_site.web_suite_number,
        web_site.web_city,
        web_site.web_county,
        web_site.web_state,
        web_site.web_zip,
        web_site.web_country,
        web_site.web_gmt_offset,
        web_site.web_tax_percentage
 FROM TPCDS.web_site
 ORDER BY web_site.web_class,
          web_site.web_country,
          web_site.web_rec_start_date,
          web_site.web_site_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.store_returns_DBD_12_seg_test132_b0 /*+basename(store_returns_DBD_12_seg_test132),createtype(D)*/ 
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
 SELECT store_returns.sr_returned_date_sk,
        store_returns.sr_return_time_sk,
        store_returns.sr_item_sk,
        store_returns.sr_customer_sk,
        store_returns.sr_cdemo_sk,
        store_returns.sr_hdemo_sk,
        store_returns.sr_addr_sk,
        store_returns.sr_store_sk,
        store_returns.sr_reason_sk,
        store_returns.sr_ticket_number,
        store_returns.sr_return_quantity,
        store_returns.sr_return_amt,
        store_returns.sr_return_tax,
        store_returns.sr_return_amt_inc_tax,
        store_returns.sr_fee,
        store_returns.sr_return_ship_cost,
        store_returns.sr_refunded_cash,
        store_returns.sr_reversed_charge,
        store_returns.sr_store_credit,
        store_returns.sr_net_loss
 FROM TPCDS.store_returns
 ORDER BY store_returns.sr_item_sk,
          store_returns.sr_ticket_number
SEGMENTED BY hash(store_returns.sr_item_sk, store_returns.sr_ticket_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.store_returns_DBD_12_seg_test132_b1 /*+basename(store_returns_DBD_12_seg_test132),createtype(D)*/ 
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
 SELECT store_returns.sr_returned_date_sk,
        store_returns.sr_return_time_sk,
        store_returns.sr_item_sk,
        store_returns.sr_customer_sk,
        store_returns.sr_cdemo_sk,
        store_returns.sr_hdemo_sk,
        store_returns.sr_addr_sk,
        store_returns.sr_store_sk,
        store_returns.sr_reason_sk,
        store_returns.sr_ticket_number,
        store_returns.sr_return_quantity,
        store_returns.sr_return_amt,
        store_returns.sr_return_tax,
        store_returns.sr_return_amt_inc_tax,
        store_returns.sr_fee,
        store_returns.sr_return_ship_cost,
        store_returns.sr_refunded_cash,
        store_returns.sr_reversed_charge,
        store_returns.sr_store_credit,
        store_returns.sr_net_loss
 FROM TPCDS.store_returns
 ORDER BY store_returns.sr_item_sk,
          store_returns.sr_ticket_number
SEGMENTED BY hash(store_returns.sr_item_sk, store_returns.sr_ticket_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.store_returns_DBD_48_seg_test132_b0 /*+basename(store_returns_DBD_48_seg_test132),createtype(D)*/ 
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
 SELECT store_returns.sr_returned_date_sk,
        store_returns.sr_return_time_sk,
        store_returns.sr_item_sk,
        store_returns.sr_customer_sk,
        store_returns.sr_cdemo_sk,
        store_returns.sr_hdemo_sk,
        store_returns.sr_addr_sk,
        store_returns.sr_store_sk,
        store_returns.sr_reason_sk,
        store_returns.sr_ticket_number,
        store_returns.sr_return_quantity,
        store_returns.sr_return_amt,
        store_returns.sr_return_tax,
        store_returns.sr_return_amt_inc_tax,
        store_returns.sr_fee,
        store_returns.sr_return_ship_cost,
        store_returns.sr_refunded_cash,
        store_returns.sr_reversed_charge,
        store_returns.sr_store_credit,
        store_returns.sr_net_loss
 FROM TPCDS.store_returns
 ORDER BY store_returns.sr_item_sk,
          store_returns.sr_ticket_number
SEGMENTED BY hash(store_returns.sr_item_sk, store_returns.sr_ticket_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.store_returns_DBD_48_seg_test132_b1 /*+basename(store_returns_DBD_48_seg_test132),createtype(D)*/ 
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
 SELECT store_returns.sr_returned_date_sk,
        store_returns.sr_return_time_sk,
        store_returns.sr_item_sk,
        store_returns.sr_customer_sk,
        store_returns.sr_cdemo_sk,
        store_returns.sr_hdemo_sk,
        store_returns.sr_addr_sk,
        store_returns.sr_store_sk,
        store_returns.sr_reason_sk,
        store_returns.sr_ticket_number,
        store_returns.sr_return_quantity,
        store_returns.sr_return_amt,
        store_returns.sr_return_tax,
        store_returns.sr_return_amt_inc_tax,
        store_returns.sr_fee,
        store_returns.sr_return_ship_cost,
        store_returns.sr_refunded_cash,
        store_returns.sr_reversed_charge,
        store_returns.sr_store_credit,
        store_returns.sr_net_loss
 FROM TPCDS.store_returns
 ORDER BY store_returns.sr_item_sk,
          store_returns.sr_ticket_number
SEGMENTED BY hash(store_returns.sr_item_sk, store_returns.sr_ticket_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.household_demographics_DBD_49_seg_test132_b0 /*+basename(household_demographics_DBD_49_seg_test132),createtype(D)*/ 
(
 hd_demo_sk ENCODING COMMONDELTA_COMP,
 hd_income_band_sk ENCODING COMMONDELTA_COMP,
 hd_buy_potential,
 hd_dep_count ENCODING COMMONDELTA_COMP,
 hd_vehicle_count ENCODING COMMONDELTA_COMP
)
AS
 SELECT household_demographics.hd_demo_sk,
        household_demographics.hd_income_band_sk,
        household_demographics.hd_buy_potential,
        household_demographics.hd_dep_count,
        household_demographics.hd_vehicle_count
 FROM TPCDS.household_demographics
 ORDER BY household_demographics.hd_demo_sk
SEGMENTED BY hash(household_demographics.hd_demo_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.household_demographics_DBD_49_seg_test132_b1 /*+basename(household_demographics_DBD_49_seg_test132),createtype(D)*/ 
(
 hd_demo_sk ENCODING COMMONDELTA_COMP,
 hd_income_band_sk ENCODING COMMONDELTA_COMP,
 hd_buy_potential,
 hd_dep_count ENCODING COMMONDELTA_COMP,
 hd_vehicle_count ENCODING COMMONDELTA_COMP
)
AS
 SELECT household_demographics.hd_demo_sk,
        household_demographics.hd_income_band_sk,
        household_demographics.hd_buy_potential,
        household_demographics.hd_dep_count,
        household_demographics.hd_vehicle_count
 FROM TPCDS.household_demographics
 ORDER BY household_demographics.hd_demo_sk
SEGMENTED BY hash(household_demographics.hd_demo_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.web_page_DBD_13_rep_test132
(
 wp_web_page_sk ENCODING COMMONDELTA_COMP,
 wp_web_page_id,
 wp_rec_start_date ENCODING RLE,
 wp_rec_end_date ENCODING BLOCKDICT_COMP,
 wp_creation_date_sk ENCODING DELTAVAL,
 wp_access_date_sk ENCODING DELTAVAL,
 wp_autogen_flag ENCODING RLE,
 wp_customer_sk ENCODING RLE,
 wp_url ENCODING RLE,
 wp_type,
 wp_char_count ENCODING DELTAVAL,
 wp_link_count ENCODING DELTAVAL,
 wp_image_count ENCODING DELTAVAL,
 wp_max_ad_count ENCODING DELTAVAL
)
AS
 SELECT web_page.wp_web_page_sk,
        web_page.wp_web_page_id,
        web_page.wp_rec_start_date,
        web_page.wp_rec_end_date,
        web_page.wp_creation_date_sk,
        web_page.wp_access_date_sk,
        web_page.wp_autogen_flag,
        web_page.wp_customer_sk,
        web_page.wp_url,
        web_page.wp_type,
        web_page.wp_char_count,
        web_page.wp_link_count,
        web_page.wp_image_count,
        web_page.wp_max_ad_count
 FROM TPCDS.web_page
 ORDER BY web_page.wp_url,
          web_page.wp_autogen_flag,
          web_page.wp_rec_start_date,
          web_page.wp_web_page_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.promotion_DBD_50_seg_test132_b0 /*+basename(promotion_DBD_50_seg_test132),createtype(D)*/ 
(
 p_promo_sk ENCODING COMMONDELTA_COMP,
 p_promo_id,
 p_start_date_sk ENCODING DELTAVAL,
 p_end_date_sk ENCODING DELTAVAL,
 p_item_sk ENCODING DELTAVAL,
 p_cost ENCODING BLOCKDICT_COMP,
 p_response_target ENCODING BLOCKDICT_COMP,
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
)
AS
 SELECT promotion.p_promo_sk,
        promotion.p_promo_id,
        promotion.p_start_date_sk,
        promotion.p_end_date_sk,
        promotion.p_item_sk,
        promotion.p_cost,
        promotion.p_response_target,
        promotion.p_promo_name,
        promotion.p_channel_dmail,
        promotion.p_channel_email,
        promotion.p_channel_catalog,
        promotion.p_channel_tv,
        promotion.p_channel_radio,
        promotion.p_channel_press,
        promotion.p_channel_event,
        promotion.p_channel_demo,
        promotion.p_channel_details,
        promotion.p_purpose,
        promotion.p_discount_active
 FROM TPCDS.promotion
 ORDER BY promotion.p_promo_sk
SEGMENTED BY hash(promotion.p_promo_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.promotion_DBD_50_seg_test132_b1 /*+basename(promotion_DBD_50_seg_test132),createtype(D)*/ 
(
 p_promo_sk ENCODING COMMONDELTA_COMP,
 p_promo_id,
 p_start_date_sk ENCODING DELTAVAL,
 p_end_date_sk ENCODING DELTAVAL,
 p_item_sk ENCODING DELTAVAL,
 p_cost ENCODING BLOCKDICT_COMP,
 p_response_target ENCODING BLOCKDICT_COMP,
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
)
AS
 SELECT promotion.p_promo_sk,
        promotion.p_promo_id,
        promotion.p_start_date_sk,
        promotion.p_end_date_sk,
        promotion.p_item_sk,
        promotion.p_cost,
        promotion.p_response_target,
        promotion.p_promo_name,
        promotion.p_channel_dmail,
        promotion.p_channel_email,
        promotion.p_channel_catalog,
        promotion.p_channel_tv,
        promotion.p_channel_radio,
        promotion.p_channel_press,
        promotion.p_channel_event,
        promotion.p_channel_demo,
        promotion.p_channel_details,
        promotion.p_purpose,
        promotion.p_discount_active
 FROM TPCDS.promotion
 ORDER BY promotion.p_promo_sk
SEGMENTED BY hash(promotion.p_promo_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.catalog_page_DBD_14_rep_test132
(
 cp_catalog_page_sk ENCODING COMMONDELTA_COMP,
 cp_catalog_page_id,
 cp_start_date_sk ENCODING RLE,
 cp_end_date_sk ENCODING RLE,
 cp_department ENCODING RLE,
 cp_catalog_number ENCODING RLE,
 cp_catalog_page_number ENCODING COMMONDELTA_COMP,
 cp_description,
 cp_type ENCODING RLE
)
AS
 SELECT catalog_page.cp_catalog_page_sk,
        catalog_page.cp_catalog_page_id,
        catalog_page.cp_start_date_sk,
        catalog_page.cp_end_date_sk,
        catalog_page.cp_department,
        catalog_page.cp_catalog_number,
        catalog_page.cp_catalog_page_number,
        catalog_page.cp_description,
        catalog_page.cp_type
 FROM TPCDS.catalog_page
 ORDER BY catalog_page.cp_department,
          catalog_page.cp_type,
          catalog_page.cp_start_date_sk,
          catalog_page.cp_catalog_page_sk
UNSEGMENTED ALL NODES;

CREATE PROJECTION TPCDS.inventory_DBD_51_seg_test132_b0 /*+basename(inventory_DBD_51_seg_test132),createtype(D)*/ 
(
 inv_date_sk ENCODING RLE,
 inv_item_sk ENCODING COMMONDELTA_COMP,
 inv_warehouse_sk ENCODING BLOCKDICT_COMP,
 inv_quantity_on_hand ENCODING DELTAVAL
)
AS
 SELECT inventory.inv_date_sk,
        inventory.inv_item_sk,
        inventory.inv_warehouse_sk,
        inventory.inv_quantity_on_hand
 FROM TPCDS.inventory
 ORDER BY inventory.inv_date_sk,
          inventory.inv_item_sk,
          inventory.inv_warehouse_sk
SEGMENTED BY hash(inventory.inv_date_sk, inventory.inv_item_sk, inventory.inv_warehouse_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.inventory_DBD_51_seg_test132_b1 /*+basename(inventory_DBD_51_seg_test132),createtype(D)*/ 
(
 inv_date_sk ENCODING RLE,
 inv_item_sk ENCODING COMMONDELTA_COMP,
 inv_warehouse_sk ENCODING BLOCKDICT_COMP,
 inv_quantity_on_hand ENCODING DELTAVAL
)
AS
 SELECT inventory.inv_date_sk,
        inventory.inv_item_sk,
        inventory.inv_warehouse_sk,
        inventory.inv_quantity_on_hand
 FROM TPCDS.inventory
 ORDER BY inventory.inv_date_sk,
          inventory.inv_item_sk,
          inventory.inv_warehouse_sk
SEGMENTED BY hash(inventory.inv_date_sk, inventory.inv_item_sk, inventory.inv_warehouse_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.catalog_returns_DBD_15_seg_test132_b0 /*+basename(catalog_returns_DBD_15_seg_test132),createtype(D)*/ 
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
 SELECT catalog_returns.cr_returned_date_sk,
        catalog_returns.cr_returned_time_sk,
        catalog_returns.cr_item_sk,
        catalog_returns.cr_refunded_customer_sk,
        catalog_returns.cr_refunded_cdemo_sk,
        catalog_returns.cr_refunded_hdemo_sk,
        catalog_returns.cr_refunded_addr_sk,
        catalog_returns.cr_returning_customer_sk,
        catalog_returns.cr_returning_cdemo_sk,
        catalog_returns.cr_returning_hdemo_sk,
        catalog_returns.cr_returning_addr_sk,
        catalog_returns.cr_call_center_sk,
        catalog_returns.cr_catalog_page_sk,
        catalog_returns.cr_ship_mode_sk,
        catalog_returns.cr_warehouse_sk,
        catalog_returns.cr_reason_sk,
        catalog_returns.cr_order_number,
        catalog_returns.cr_return_quantity,
        catalog_returns.cr_return_amount,
        catalog_returns.cr_return_tax,
        catalog_returns.cr_return_amt_inc_tax,
        catalog_returns.cr_fee,
        catalog_returns.cr_return_ship_cost,
        catalog_returns.cr_refunded_cash,
        catalog_returns.cr_reversed_charge,
        catalog_returns.cr_store_credit,
        catalog_returns.cr_net_loss
 FROM TPCDS.catalog_returns
 ORDER BY catalog_returns.cr_returned_date_sk,
          catalog_returns.cr_call_center_sk,
          catalog_returns.cr_warehouse_sk,
          catalog_returns.cr_ship_mode_sk,
          catalog_returns.cr_reason_sk,
          catalog_returns.cr_item_sk,
          catalog_returns.cr_order_number
SEGMENTED BY hash(catalog_returns.cr_returning_customer_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.catalog_returns_DBD_15_seg_test132_b1 /*+basename(catalog_returns_DBD_15_seg_test132),createtype(D)*/ 
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
 SELECT catalog_returns.cr_returned_date_sk,
        catalog_returns.cr_returned_time_sk,
        catalog_returns.cr_item_sk,
        catalog_returns.cr_refunded_customer_sk,
        catalog_returns.cr_refunded_cdemo_sk,
        catalog_returns.cr_refunded_hdemo_sk,
        catalog_returns.cr_refunded_addr_sk,
        catalog_returns.cr_returning_customer_sk,
        catalog_returns.cr_returning_cdemo_sk,
        catalog_returns.cr_returning_hdemo_sk,
        catalog_returns.cr_returning_addr_sk,
        catalog_returns.cr_call_center_sk,
        catalog_returns.cr_catalog_page_sk,
        catalog_returns.cr_ship_mode_sk,
        catalog_returns.cr_warehouse_sk,
        catalog_returns.cr_reason_sk,
        catalog_returns.cr_order_number,
        catalog_returns.cr_return_quantity,
        catalog_returns.cr_return_amount,
        catalog_returns.cr_return_tax,
        catalog_returns.cr_return_amt_inc_tax,
        catalog_returns.cr_fee,
        catalog_returns.cr_return_ship_cost,
        catalog_returns.cr_refunded_cash,
        catalog_returns.cr_reversed_charge,
        catalog_returns.cr_store_credit,
        catalog_returns.cr_net_loss
 FROM TPCDS.catalog_returns
 ORDER BY catalog_returns.cr_returned_date_sk,
          catalog_returns.cr_call_center_sk,
          catalog_returns.cr_warehouse_sk,
          catalog_returns.cr_ship_mode_sk,
          catalog_returns.cr_reason_sk,
          catalog_returns.cr_item_sk,
          catalog_returns.cr_order_number
SEGMENTED BY hash(catalog_returns.cr_returning_customer_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.catalog_returns_DBD_52_seg_test132_b0 /*+basename(catalog_returns_DBD_52_seg_test132),createtype(D)*/ 
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
 SELECT catalog_returns.cr_returned_date_sk,
        catalog_returns.cr_returned_time_sk,
        catalog_returns.cr_item_sk,
        catalog_returns.cr_refunded_customer_sk,
        catalog_returns.cr_refunded_cdemo_sk,
        catalog_returns.cr_refunded_hdemo_sk,
        catalog_returns.cr_refunded_addr_sk,
        catalog_returns.cr_returning_customer_sk,
        catalog_returns.cr_returning_cdemo_sk,
        catalog_returns.cr_returning_hdemo_sk,
        catalog_returns.cr_returning_addr_sk,
        catalog_returns.cr_call_center_sk,
        catalog_returns.cr_catalog_page_sk,
        catalog_returns.cr_ship_mode_sk,
        catalog_returns.cr_warehouse_sk,
        catalog_returns.cr_reason_sk,
        catalog_returns.cr_order_number,
        catalog_returns.cr_return_quantity,
        catalog_returns.cr_return_amount,
        catalog_returns.cr_return_tax,
        catalog_returns.cr_return_amt_inc_tax,
        catalog_returns.cr_fee,
        catalog_returns.cr_return_ship_cost,
        catalog_returns.cr_refunded_cash,
        catalog_returns.cr_reversed_charge,
        catalog_returns.cr_store_credit,
        catalog_returns.cr_net_loss
 FROM TPCDS.catalog_returns
 ORDER BY catalog_returns.cr_item_sk,
          catalog_returns.cr_order_number
SEGMENTED BY hash(catalog_returns.cr_item_sk, catalog_returns.cr_order_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.catalog_returns_DBD_52_seg_test132_b1 /*+basename(catalog_returns_DBD_52_seg_test132),createtype(D)*/ 
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
 SELECT catalog_returns.cr_returned_date_sk,
        catalog_returns.cr_returned_time_sk,
        catalog_returns.cr_item_sk,
        catalog_returns.cr_refunded_customer_sk,
        catalog_returns.cr_refunded_cdemo_sk,
        catalog_returns.cr_refunded_hdemo_sk,
        catalog_returns.cr_refunded_addr_sk,
        catalog_returns.cr_returning_customer_sk,
        catalog_returns.cr_returning_cdemo_sk,
        catalog_returns.cr_returning_hdemo_sk,
        catalog_returns.cr_returning_addr_sk,
        catalog_returns.cr_call_center_sk,
        catalog_returns.cr_catalog_page_sk,
        catalog_returns.cr_ship_mode_sk,
        catalog_returns.cr_warehouse_sk,
        catalog_returns.cr_reason_sk,
        catalog_returns.cr_order_number,
        catalog_returns.cr_return_quantity,
        catalog_returns.cr_return_amount,
        catalog_returns.cr_return_tax,
        catalog_returns.cr_return_amt_inc_tax,
        catalog_returns.cr_fee,
        catalog_returns.cr_return_ship_cost,
        catalog_returns.cr_refunded_cash,
        catalog_returns.cr_reversed_charge,
        catalog_returns.cr_store_credit,
        catalog_returns.cr_net_loss
 FROM TPCDS.catalog_returns
 ORDER BY catalog_returns.cr_item_sk,
          catalog_returns.cr_order_number
SEGMENTED BY hash(catalog_returns.cr_item_sk, catalog_returns.cr_order_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.web_returns_DBD_16_seg_test132_b0 /*+basename(web_returns_DBD_16_seg_test132),createtype(D)*/ 
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
 SELECT web_returns.wr_returned_date_sk,
        web_returns.wr_returned_time_sk,
        web_returns.wr_item_sk,
        web_returns.wr_refunded_customer_sk,
        web_returns.wr_refunded_cdemo_sk,
        web_returns.wr_refunded_hdemo_sk,
        web_returns.wr_refunded_addr_sk,
        web_returns.wr_returning_customer_sk,
        web_returns.wr_returning_cdemo_sk,
        web_returns.wr_returning_hdemo_sk,
        web_returns.wr_returning_addr_sk,
        web_returns.wr_web_page_sk,
        web_returns.wr_reason_sk,
        web_returns.wr_order_number,
        web_returns.wr_return_quantity,
        web_returns.wr_return_amt,
        web_returns.wr_return_tax,
        web_returns.wr_return_amt_inc_tax,
        web_returns.wr_fee,
        web_returns.wr_return_ship_cost,
        web_returns.wr_refunded_cash,
        web_returns.wr_reversed_charge,
        web_returns.wr_account_credit,
        web_returns.wr_net_loss
 FROM TPCDS.web_returns
 ORDER BY web_returns.wr_item_sk,
          web_returns.wr_order_number
SEGMENTED BY hash(web_returns.wr_item_sk, web_returns.wr_order_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.web_returns_DBD_16_seg_test132_b1 /*+basename(web_returns_DBD_16_seg_test132),createtype(D)*/ 
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
 SELECT web_returns.wr_returned_date_sk,
        web_returns.wr_returned_time_sk,
        web_returns.wr_item_sk,
        web_returns.wr_refunded_customer_sk,
        web_returns.wr_refunded_cdemo_sk,
        web_returns.wr_refunded_hdemo_sk,
        web_returns.wr_refunded_addr_sk,
        web_returns.wr_returning_customer_sk,
        web_returns.wr_returning_cdemo_sk,
        web_returns.wr_returning_hdemo_sk,
        web_returns.wr_returning_addr_sk,
        web_returns.wr_web_page_sk,
        web_returns.wr_reason_sk,
        web_returns.wr_order_number,
        web_returns.wr_return_quantity,
        web_returns.wr_return_amt,
        web_returns.wr_return_tax,
        web_returns.wr_return_amt_inc_tax,
        web_returns.wr_fee,
        web_returns.wr_return_ship_cost,
        web_returns.wr_refunded_cash,
        web_returns.wr_reversed_charge,
        web_returns.wr_account_credit,
        web_returns.wr_net_loss
 FROM TPCDS.web_returns
 ORDER BY web_returns.wr_item_sk,
          web_returns.wr_order_number
SEGMENTED BY hash(web_returns.wr_item_sk, web_returns.wr_order_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.web_sales_DBD_17_seg_test132_b0 /*+basename(web_sales_DBD_17_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.web_sales
 ORDER BY web_sales.ws_sold_date_sk,
          web_sales.ws_item_sk,
          web_sales.ws_warehouse_sk,
          web_sales.ws_order_number
SEGMENTED BY hash(web_sales.ws_sold_date_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.web_sales_DBD_17_seg_test132_b1 /*+basename(web_sales_DBD_17_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.web_sales
 ORDER BY web_sales.ws_sold_date_sk,
          web_sales.ws_item_sk,
          web_sales.ws_warehouse_sk,
          web_sales.ws_order_number
SEGMENTED BY hash(web_sales.ws_sold_date_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.catalog_sales_DBD_18_seg_test132_b0 /*+basename(catalog_sales_DBD_18_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.catalog_sales
 ORDER BY catalog_sales.cs_sold_date_sk,
          catalog_sales.cs_item_sk,
          catalog_sales.cs_call_center_sk,
          catalog_sales.cs_order_number
SEGMENTED BY hash(catalog_sales.cs_sold_date_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.catalog_sales_DBD_18_seg_test132_b1 /*+basename(catalog_sales_DBD_18_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.catalog_sales
 ORDER BY catalog_sales.cs_sold_date_sk,
          catalog_sales.cs_item_sk,
          catalog_sales.cs_call_center_sk,
          catalog_sales.cs_order_number
SEGMENTED BY hash(catalog_sales.cs_sold_date_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.catalog_sales_DBD_19_seg_test132_b0 /*+basename(catalog_sales_DBD_19_seg_test132),createtype(D)*/ 
(
 cs_item_sk ENCODING RLE
)
AS
 SELECT catalog_sales.cs_item_sk
 FROM TPCDS.catalog_sales
 ORDER BY catalog_sales.cs_item_sk
SEGMENTED BY hash(catalog_sales.cs_item_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.catalog_sales_DBD_19_seg_test132_b1 /*+basename(catalog_sales_DBD_19_seg_test132),createtype(D)*/ 
(
 cs_item_sk ENCODING RLE
)
AS
 SELECT catalog_sales.cs_item_sk
 FROM TPCDS.catalog_sales
 ORDER BY catalog_sales.cs_item_sk
SEGMENTED BY hash(catalog_sales.cs_item_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.catalog_sales_DBD_53_seg_test132_b0 /*+basename(catalog_sales_DBD_53_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.catalog_sales
 ORDER BY catalog_sales.cs_item_sk,
          catalog_sales.cs_order_number
SEGMENTED BY hash(catalog_sales.cs_item_sk, catalog_sales.cs_order_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.catalog_sales_DBD_53_seg_test132_b1 /*+basename(catalog_sales_DBD_53_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.catalog_sales
 ORDER BY catalog_sales.cs_item_sk,
          catalog_sales.cs_order_number
SEGMENTED BY hash(catalog_sales.cs_item_sk, catalog_sales.cs_order_number) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.store_sales_DBD_20_seg_test132_b0 /*+basename(store_sales_DBD_20_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.store_sales
 ORDER BY store_sales.ss_sold_date_sk,
          store_sales.ss_item_sk,
          store_sales.ss_store_sk,
          store_sales.ss_ticket_number
SEGMENTED BY hash(store_sales.ss_item_sk) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.store_sales_DBD_20_seg_test132_b1 /*+basename(store_sales_DBD_20_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.store_sales
 ORDER BY store_sales.ss_sold_date_sk,
          store_sales.ss_item_sk,
          store_sales.ss_store_sk,
          store_sales.ss_ticket_number
SEGMENTED BY hash(store_sales.ss_item_sk) ALL NODES OFFSET 1;

CREATE PROJECTION TPCDS.store_sales_DBD_54_seg_test132_b0 /*+basename(store_sales_DBD_54_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.store_sales
 ORDER BY store_sales.ss_item_sk,
          store_sales.ss_ticket_number
SEGMENTED BY hash(store_sales.ss_item_sk, store_sales.ss_ticket_number) ALL NODES OFFSET 0;

CREATE PROJECTION TPCDS.store_sales_DBD_54_seg_test132_b1 /*+basename(store_sales_DBD_54_seg_test132),createtype(D)*/ 
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
 FROM TPCDS.store_sales
 ORDER BY store_sales.ss_item_sk,
          store_sales.ss_ticket_number
SEGMENTED BY hash(store_sales.ss_item_sk, store_sales.ss_ticket_number) ALL NODES OFFSET 1;


SELECT MARK_DESIGN_KSAFE(0);
