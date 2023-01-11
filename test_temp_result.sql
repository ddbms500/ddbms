create table tempBook (Book_id int, Book_publisher_id int, Book_title char(100));
insert into tempBook (Book_id, Book_publisher_id, Book_title) select Book_id, Book_publisher_id, Book_title from table15;
insert into tempBook (Book_id, Book_publisher_id, Book_title) select Book_id, Book_publisher_id, Book_title from table17;
insert into tempBook (Book_id, Book_publisher_id, Book_title) select Book_id, Book_publisher_id, Book_title from table19;
create table tempOrders (Orders_book_id int, Orders_customer_id int, Orders_quantity int);
insert into tempOrders (Orders_book_id, Orders_customer_id, Orders_quantity) select Orders_book_id, Orders_customer_id, Orders_quantity from table5;
insert into tempOrders (Orders_book_id, Orders_customer_id, Orders_quantity) select Orders_book_id, Orders_customer_id, Orders_quantity from table7;
create table tempPublisher (Publisher_id int, Publisher_name char(100));
insert into tempPublisher (Publisher_id, Publisher_name) select Publisher_id, Publisher_name from table9;
insert into tempPublisher (Publisher_id, Publisher_name) select Publisher_id, Publisher_name from table11;
select Orders_quantity, Publisher_name, Customer_name, Book_title from tempPublisher, tempOrders, tempBook, table13 
where table13.Customer_id=tempOrders.Orders_customer_id 
and tempBook.Book_id=tempOrders.Orders_book_id 
and tempBook.Book_publisher_id=tempPublisher.Publisher_id;