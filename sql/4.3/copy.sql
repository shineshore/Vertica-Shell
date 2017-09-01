copy tpc.store_sales from '/data/SOURCE/tpcds_200g/store_sales.dat' on any node delimiter '|' direct;
copy tpc.store_returns from '/data/SOURCE/tpcds_200g/store_returns.dat' on any node delimiter '|' direct;
copy tpc.catalog_sales from '/data/SOURCE/tpcds_200g/catalog_sales.dat' on any node delimiter '|' direct;
copy tpc.date_dim from '/data/SOURCE/tpcds_200g/date_dim.dat' on any node delimiter '|' direct;
copy tpc.store from '/data/SOURCE/tpcds_200g/store.dat' on any node delimiter '|' direct;
copy tpc.item from '/data/SOURCE/tpcds_200g/item.dat' on any node delimiter '|' direct;
