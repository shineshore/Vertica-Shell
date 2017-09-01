CREATE SCHEMA TPC;

CREATE TABLE TPC.store_sales
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

CREATE TABLE TPC.date_dim
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


CREATE TABLE TPC.item
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


CREATE PROJECTION TPC.store_sales /*+createtype(L)*/ 
(
 ss_sold_date_sk,
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
 FROM TPC.store_sales
 ORDER BY store_sales.ss_sold_date_sk,
          store_sales.ss_item_sk,
          store_sales.ss_ticket_number
SEGMENTED BY hash(store_sales.ss_customer_sk) ALL NODES KSAFE 1;



CREATE PROJECTION TPC.date_dim /*+createtype(D)*/ 
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
 FROM TPC.date_dim
 ORDER BY date_dim.d_year,
          date_dim.d_moy,
          date_dim.d_dom,
          date_dim.d_date_sk
UNSEGMENTED ALL NODES;


CREATE PROJECTION TPC.item /*+createtype(D)*/ 
(
 i_item_sk ENCODING DELTAVAL,
 i_item_id,
 i_rec_start_date ENCODING BLOCKDICT_COMP,
 i_rec_end_date ENCODING BLOCKDICT_COMP,
 i_item_desc,
 i_current_price ENCODING DELTARANGE_COMP,
 i_wholesale_cost ENCODING DELTARANGE_COMP,
 i_brand_id ENCODING DELTARANGE_COMP,
 i_brand,
 i_class_id ENCODING COMMONDELTA_COMP,
 i_class ENCODING RLE,
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
 FROM TPC.item
 ORDER BY item.i_category,
          item.i_class,
          item.i_current_price,
          item.i_item_id,
          item.i_item_desc
SEGMENTED BY hash(item.i_product_name) ALL NODES KSAFE 1;


copy tpc.store_sales from '/data/ds/400g/store_sales.dat' on any node delimiter '|' direct;
copy tpc.date_dim from '/data/ds/400g/date_dim.dat' on any node delimiter '|' direct;
copy tpc.item from '/data/ds/400g/item.dat' on any node delimiter '|' direct;


select dt.d_year
       ,item.i_brand_id brand_id
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  tpc.date_dim dt
      ,tpc.store_sales
      ,tpc.item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 436
   and dt.d_moy=12
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;

