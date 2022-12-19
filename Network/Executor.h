//
// Created by sxy on 22-11-14.
//

#ifndef DDBMS_EXECUTOR_H
#define DDBMS_EXECUTOR_H
#include <iostream>
#include "Defs/QueryTree.h"
#include "MetaData/MetaData.h"
#include <mysql++/mysql++.h>

class Executor {
public:
    Executor(MetaData* meta_data) {
        meta_data_ = meta_data;
        mysql_connection = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
    }
    void exec_sql_local(std::string sql);

    // 执行本地站点上的执行计划
    // 传入根节点的index
    void exec_query_tree(int root_index);

    void print_records(int root_index);

    // 把query_tree的子树的根节点编号传送给其他站点, 然后获取其他站点的返回结果(临时表)
    void request_remote_execution_result(int root_index);

    bool Data_Insert_Delete(std::vector<std::string> sql_vec, std::vector<std::string> site_vec);
    std::vector<std::string> Data_Select_Single(std::vector<std::string> sql_vec, std::vector<std::string> site_vec);

    mysqlpp::Connection* mysql_connection = nullptr;
    MetaData* meta_data_;
    std::string sql_ = ""; // used for midterm test
    // 用query_tree中的节点编号来索引
    std::unordered_map<int, std::vector<std::string>> temp_table_data;
    // 每个record存成一个string, value之间用_来分割
};

#endif //DDBMS_EXECUTOR_H
