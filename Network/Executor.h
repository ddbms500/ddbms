//
// Created by sxy on 22-11-14.
//

#ifndef DDBMS_EXECUTOR_H
#define DDBMS_EXECUTOR_H
#include <iostream>
#include "Defs/QueryTree.h"
#include "MetaData/MetaData.h"
#include <mysql++/mysql++.h>
#include <ctime>
#include <thread>
#include <future>

#define MAXTHREAD 4

class Executor {
public:
    Executor(MetaData* meta_data) {
        meta_data_ = meta_data;
        mysql_connection = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
        qt = &meta_data_->query_tree;

    }
    void exec_sql_local(std::string sql);
    // 执行本地站点上的执行计划
    // 传入根节点的index
    void exec_query_tree(int root_index);

    void print_records(int root_index);

    // 把query_tree的子树的根节点编号传送给其他站点, 然后获取其他站点的返回结果(临时表)
    void request_remote_execution_result(int root_index);//这个树节点中会包含站点信息

    //针对非select语句，输入SQL语句和站点编号。
    bool Data_Insert_Delete(std::vector<std::string> sql_vec, std::vector<std::string> site_vec);//INSERT DELETE调用
    void Data_Insert_Delete_Thread(std::string sql_,std::string site_,bool* result);//被Data_Insert_Delete调用

    bool request_remote_execution_result(std::string sql,std::string site_,std::vector<std::string>* records);//传入非当前站点执行的sql，site_和records，把结果存到了records中

    void Data_Select_Single_Thread(std::string sql,std::string site_,std::vector<std::string>* results);//被Data_Select_Single调用。返回string的vector，存要删除的所有记录的主键的值
    std::vector<std::string> Data_Select_Single(std::vector<std::string> sql_vec, std::vector<std::string> site_vec);//直接的SELECT语句-调用（还有通过执行树调用的），返回交集

    bool Data_Select_Thread(int root_index,std::promise<bool> &resultObj);//被Data_Select调用。返回执行是否成功
    bool Data_Select(int root_index);//SELECT调用（执行树）。返回执行是否成功。
    bool Data_Select_Thread_Remote(int root_index, std::string site_name);//被Data_Select和Data_Select_Thread调用，传入结点的index和对应的站点。先在远程站点创建临时表，然后再执行sql语句获得结果，最后在本站点上创建临时表。

    void Data_Select_Print(int root_index);//Select操作结束后，再从最开始的根节点中，打印取出来的树

    
    // QueryTree get_tree( int root_index);
    std::string get_sql(QueryNode* node,int tree_id) ;//得到node结点中对应的sql语句



    mysqlpp::Connection* mysql_connection = nullptr;
    QueryTree* qt ;
    MetaData* meta_data_;
    std::string sqwl_ = ""; // used for midterm test
    // 用query_tree中的节点编号来索引
    std::unordered_map<int, std::vector<std::string>> temp_table_data;
    // 每个record存成一个string, value之间用_来分割
};

#endif //DDBMS_EXECUTOR_H
