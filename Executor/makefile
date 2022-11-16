out = sql_single_node
#out = sql_table_structure
$(out): $(out).cpp 
	g++ -finput-charset=utf-8 -I/usr/include/mysql -L/usr/lib64/mysql $(out).cpp -o $(out) -lmysqlclient -lmysqlpp
