create table product2 (sku varbinary(128), description varbinary(128), price bigint, primary key(sku));
create table customer (customer_id bigint not null auto_increment, email varbinary(128), primary key(customer_id));
create table corder2 (order_id bigint not null auto_increment, customer_id bigint, sku varbinary(128), price bigint, primary key(order_id));
