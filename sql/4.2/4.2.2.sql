\timing

DROP TABLE IF EXISTS tpcds.customer_ext CASCADE;
CREATE TABLE tpcds.customer_ext
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

copy tpcds.customer_ext with source Hdfs(url='http://134.96.238.132:50070/webhdfs/v1/test/customer.dat', username='etladmin') direct;

select count(1) from tpcds.customer_ext;

\q
