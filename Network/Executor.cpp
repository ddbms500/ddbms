//
// Created by sxy on 22-11-14.
//
#include "Executor.h"
#include "NetworkRequest.h"
#include <string>
#include <thread>
#include <algorithm>
#include <fstream>
#include "Utils/RecordPrinter.h"
#include <ctime>

void Executor::drop_temp_table() {
    mysqlpp::Connection* conn = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
    for(auto table: temp_table_name) {
        std::string sql = "drop table " + table + ";";
        mysqlpp::Query query = conn->query(sql);
        query.execute();
        // if(query.exec()) {
        //     std::cout << "delete temp table success\n";
        // }
        // else {
        //     std::cout << "fail to delete temp table\b";
        // }
    }
}

void Executor::exec_sql_local(std::string sql) {
    // mysqlpp::Connection* conn = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
    
}

void Executor::generate_leaf_table(std::string site_name, int node_index) {
    QueryTree* query_tree = meta_data_->query_tree;
    auto need_prune = meta_data_->need_prune;
    if(need_prune->find(node_index)->second == true) return;
    
    QueryNode* project_node = &(query_tree->nodes[node_index]);
    QueryNode* select_node = &(query_tree->nodes[node_index - 1]);
    Table* table = meta_data_->get_table(project_node->table_name_);

    // 先在本地create临时表
    std::string sql = "create table table" + std::to_string(node_index);
    temp_table_name.push_back("table" + std::to_string(node_index));
    std::string table_meta = " (";
    for(int i = 0; i < project_node->attr_name_.size(); ++i) {
        if(i > 0) table_meta+= ", ";
        Attribute attr = table->get_attr(project_node->attr_name_[i]);
        table_meta += table->table_name_ + "_" + attr.attr_name_ + " ";
        table_meta += attribute_type_str[static_cast<int>(attr.type_)];
        if(attr.type_ == AttributeType::CHAR) table_meta += "(" + std::to_string(attr.data_length_) + ")";
    }
    table_meta += ")";
    sql += table_meta + ";";
    // std::cout << sql << std::endl;

    mysqlpp::Connection* conn  = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
    // std::cout << "created connection\n";
    mysqlpp::Query query = conn->query(sql);
    // std::cout << "created query\n";
    query.execute();
    // std::cout << "finish exec\n";
    // if(query.exec()) {
    //     std::cout << "create success" << std::endl;
    // }
    // else {
    //     std::cout << "fail to create temp table" << std::endl;
    // }

    // 判断该节点对应数据在本地还是远端
    if(site_name.compare(meta_data_->local_site_name) == 0) {
        // 如果是本地数据，直接select出来插入到临时表中
        sql = "insert into table" + std::to_string(node_index) + "(";
        std::string select_sql = "select ";
        int temp_cnt = 0;
        Fragment* fragment = &(table->fragments_[project_node->fragment_id_]);
        for(int i = 0; i < project_node->attr_name_.size(); ++i) {
            if(table->table_fragment_type_ == FragmentType::VERTICAL && fragment->is_attr_in_fragment(project_node->attr_name_[i]) == false)
                continue;
            if(temp_cnt > 0) select_sql += ", ", sql += ", ";
            select_sql += project_node->attr_name_[i];
            sql += table->table_name_ + "_" + project_node->attr_name_[i];
            temp_cnt ++;
        }
        sql += ")";
        select_sql += " from " + table->table_name_;
        if(select_node->select_predicates_.size() > 0) {
            select_sql += " where ";
            int i = 0;
            for(auto predicate: select_node->select_predicates_) {
                if(i > 0) select_sql += " and ";
                i++;
                select_sql += predicate.attribute_name_ + OperationTypeStr[static_cast<int>(predicate.operation_type_)];
                if(table->get_attr(predicate.attribute_name_).type_ == AttributeType::CHAR) {
                    select_sql += "'" + predicate.right_value_ + "'";
                }
                else {
                    select_sql += predicate.right_value_;
                }
            }
        }
        select_sql += ";";
        sql += select_sql;
        // std::cout << sql << std::endl;
        query = conn->query(sql);
        query.execute();
        // if(query.exec()) {
        //     std::cout << "load data to temp table success" << std::endl;
        // }
        // else {
        //     std::cout << "fail to load data into temp table" << std::endl;
        // }
    }
    else {
        // 如果该节点对应的分片不在当前站点，那么需要从该站点读取出来，然后插入到当前站点里面
        sql = "insert into table" + std::to_string(node_index) + "(";
        std::string select_sql = "select ";
        int temp_cnt = 0;
        Fragment* fragment = &(table->fragments_[project_node->fragment_id_]);
        for(int i = 0; i < project_node->attr_name_.size(); ++i) {
            if(table->table_fragment_type_ == FragmentType::VERTICAL && fragment->is_attr_in_fragment(project_node->attr_name_[i]) == false)
                continue;
            if(temp_cnt > 0) select_sql += ", ", sql += ", ";
            select_sql += project_node->attr_name_[i];
            sql += table->table_name_ + "_" + project_node->attr_name_[i];
            temp_cnt ++;
        }
        sql += ") values ";
        select_sql += " from " + table->table_name_;
        if(select_node->select_predicates_.size() > 0) {
            select_sql += " where ";
            int i = 0;
            for(auto predicate: select_node->select_predicates_) {
                if(i > 0) select_sql += " and ";
                i++;
                select_sql += predicate.attribute_name_ + OperationTypeStr[static_cast<int>(predicate.operation_type_)];
                if(table->get_attr(predicate.attribute_name_).type_ == AttributeType::CHAR) {
                    select_sql += "'" + predicate.right_value_ + "'";
                }
                else {
                    select_sql += predicate.right_value_;
                }
            }
        }
        select_sql += ";";
        std::cout << site_name << "  " << select_sql << std::endl;
        sql += request_remote_leaf_node(select_sql, site_name) + ";";
        query = conn->query(sql);
        query.execute();
        // if(query.exec()) {
        //     std::cout << table->table_name_ << ": load remote data to temp table success" << std::endl;
        // }
        // else {
        //     std::cout << table->table_name_ << ": fail to load remote data to temp table\n";
        // }
    }
    
}

std::string Executor::request_remote_leaf_node(std::string sql_,std::string site_){
    std::string aim_site = site_;
    std::string ip = meta_data_->site_map[aim_site].first;
    std::string port = "8800";
    std::string sql = sql_;
    brpc::Channel channel;
    initial_channel(channel, ip + ":" + port);
    whiteBear::DDBService_Stub stub(&channel);
    whiteBear::SelectSQLRequest request;
    whiteBear::SelectSQLResponse response;
    set_SelectSQLRequest(request, sql);
    // std::cout << "finish set request\n";
    brpc::Controller controller;
    stub.SelectSQL(&controller, &request, &response, nullptr);
//    std::cout << "finished send request and get response;\n";
    // temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(root_index), std::forward_as_tuple());
    // std::vector<std::string>* records = &(temp_table_data.find(root_index)->second);
    bool success;
    std::string errors;
    std::string values;
    // std::cout << "get response\n";
    get_SelectSQLResponse(response, success, errors, values);
    return values;
}

// 执行本地站点上的执行计划
void Executor::exec_query_tree(int root_index) {
    //逻辑是：去metadata里面用index找到这个点在的位置，然后从这个点开始DFS，如果是自己站点要做的，就做。如果不是，就多开一个线程request
//    exec_sql_local("");
//    request_remote_execution_result(-1);

    /** 执行逻辑
     * 首先遍历所有的叶子节点，在本地数据库temp中创建对应的表，表的名称为table0/table1..., 后面的数字为节点ID
     * projection和selection节点一起组成select语句作为叶子节点语句执行
     * 这一部分可以并发来做
     * 然后首先Union水平分片、merge竖直分片
     * 把表按照顺序连接起来
     * 所有的union、merge操作都可以使用create+insert+select语句完成：
     * 如节点20的merge操作: table10 join table 15
     * create table table20 (attrs);
     * insert into table20(attrs) select attrs from table10, table15 where table10.id = table15.id;
     * 在临时表中，所有的attr都用表名_属性名来代替
    */
    // std::vector<std::string>* total_records = &(temp_table_data.find(0)->second);
    // std::vector<std::string>* sub_records = &(temp_table_data.find(1)->second);
    // for(auto record = sub_records->begin(); record != sub_records->end(); ++record)
    //     total_records->emplace_back(*record);
    clock_t begin_time = clock();
    QueryTree* query_tree = meta_data_->query_tree;
    auto site_leaf_map = meta_data_->site_leaf_node;
    auto table_leaf_map = meta_data_->table_leaf_node;
    auto need_prune = meta_data_->need_prune;

    std::thread threads[20];
    int cnt = 0;

    // 首先把所有叶子节点都建表
    for(auto iter = site_leaf_map->begin(); iter != site_leaf_map->end(); ++iter) {
        std::string site_name = iter->first;
        std::vector<int>* nodes = &(iter->second);
        
        for(int i = 0; i < nodes->size(); ++i) {
            // nodes[i]对应projection节点，nodes[i]-1对应selection节点
            threads[cnt++] = std::thread(&Executor::generate_leaf_table, this, site_name, nodes->at(i));
        }
    }

    for(int i = 0; i < cnt; ++i) threads[i].join();
    
    // 把竖直分片join起来，水平分片union起来
    std::unordered_map<std::string, std::string> table_temp_name_map;
    mysqlpp::Connection* conn  = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
    mysqlpp::Query query = conn->query();
    for(auto iter = table_leaf_map->begin(); iter != table_leaf_map->end(); ++iter) {
        std::string table_name = iter->first;
        std::vector<int>* nodes = &(iter->second);
        Table* table = meta_data_->get_table(table_name);
        int cnt = 0;
        // 统计一下该表包含几个子节点
        for(int i = 0; i < nodes->size(); ++i) {
            if(need_prune->find(nodes->at(i))->second == false) {
                cnt ++;
            }
        }
        // 如果只有一个，那么直接把该表作为最终临时表
        if(cnt == 0) continue;
        if(cnt == 1) {
            table_temp_name_map[table_name] = "table" + std::to_string(nodes->at(0));
        }
        else {
            // 如果大于1个，需要join/union
            if(table->table_fragment_type_ == FragmentType::HORIZONTAL) {
                // 水平分片union
                std::string sql = "create table temp" + table->table_name_;
                temp_table_name.push_back("temp" + table->table_name_);
                table_temp_name_map[table_name] = "temp" + table->table_name_;  
                std::string table_meta = " (";
                QueryNode* project_node = &(query_tree->nodes[nodes->at(0)]);
                bool need_index = false;
                for(int i = 0; i < project_node->attr_name_.size(); ++i) {
                    if(i > 0) table_meta+= ", ";
                    Attribute attr = table->get_attr(project_node->attr_name_[i]);
                    if(attr.attr_name_.compare("id") == 0) {
                        need_index = true;
                    }
                    table_meta += table->table_name_ + "_" + attr.attr_name_ + " ";
                    table_meta += attribute_type_str[static_cast<int>(attr.type_)];
                    if(attr.type_ == AttributeType::CHAR) table_meta += "(" + std::to_string(attr.data_length_) + ")";
                }
                table_meta += ")";
                sql += table_meta + ";";
                // std::cout << sql << std::endl;
                // mysqlpp::Query query = conn->query(sql);
                // query.execute();
                query.execute(sql.c_str(), sql.length());
                // if(query.exec()) {
                //     std::cout << "create success" << std::endl;
                // }
                // else {
                //     std::cout << "fail to create temp table" << std::endl;
                // }

                for(int i = 0; i < nodes->size(); ++i) {
                    if(need_prune->find(nodes->at(i))->second == true) continue;
                    sql = "insert into " + table_temp_name_map[table_name] + " (";
                    std::string select_sql = "select ";
                    QueryNode* project_node = &(query_tree->nodes[nodes->at(i)]);
                    for(int i = 0; i < project_node->attr_name_.size(); ++i) {
                        if(i > 0) select_sql += ", ", sql += ", ";
                        select_sql += table->table_name_ + "_" + project_node->attr_name_[i];
                        sql += table->table_name_ + "_" + project_node->attr_name_[i];
                    }
                    sql += ") ";
                    select_sql += " from table" + std::to_string(project_node->node_id);
                    sql += select_sql + ";";
                    // std::cout << sql << std::endl;
                    // query = conn->query(sql);
                    // query.execute();
                    query.execute(sql.c_str(), sql.length());
                }

                if(need_index) {
                    std::string index_sql = "create index temp" + table->table_name_ + "_id_index on ";
                    index_sql += "temp" + table->table_name_ + " (" + table->table_name_ + "_id);";
                    // mysqlpp::Query query = conn->query(index_sql);
                    // query.execute();
                    // query.store(index_sql, index_sql.length());
                    query.execute(index_sql.c_str(), index_sql.length());
                }
            }
            else {
                // 竖直分片join
                std::string sql = "create table temp" + table->table_name_ + "(";
                table_temp_name_map[table->table_name_] = "temp" + table->table_name_;
                temp_table_name.push_back("temp" + table->table_name_);
                // 由于只有customer是竖直分片，并且只有三个属性，偷个懒，如果存在两个分片代表一定要三个属性同时存在
                for(int i = 0; i < table->attributes_.size(); ++i) {
                    if(i > 0) sql += ", ";
                    sql += table->table_name_ + "_" + table->attributes_[i].attr_name_ + " ";
                    sql += attribute_type_str[static_cast<int>(table->attributes_[i].type_)];
                    if(table->attributes_[i].type_ == AttributeType::CHAR) sql +="(" + std::to_string(table->attributes_[i].data_length_) +")";
                }
                sql += ");";
                // std::cout << sql << std::endl;
                // mysqlpp::Query query = conn->query(sql);
                // query.execute();
                query.execute(sql.c_str(), sql.length());

                sql = "insert into " + table_temp_name_map[table->table_name_] + " select ";
                QueryNode* node1 = &(query_tree->nodes[nodes->at(0)]);
                QueryNode* node2 = &(query_tree->nodes[nodes->at(1)]);
                std::string table_name1 = "table" + std::to_string(node1->node_id);
                std::string table_name2 = "table" + std::to_string(node2->node_id);

                std::string index_sql1 = "create index customer_index_1 on " + table_name1 + " (Customer_id);";
                std::string index_sql2 = "create index customer_index_2 on " + table_name2 + " (Customer_id);";
                query.execute(index_sql1.c_str(), index_sql1.length());
                query.execute(index_sql2.c_str(), index_sql2.length());

                sql += table_name1 + "." + "Customer_id" + ", ";
                sql += table_name1 + ".Customer_name, "+ table_name2+".Customer_rank from " + table_name1 + ", " + table_name2;
                sql += " where " + table_name1 + ".Customer_id = " + table_name2 + ".Customer_id;";
                // std::cout << sql << std::endl;
                // query = conn->query(sql);
                // query.execute();
                query.execute(sql.c_str(), sql.length());
            }
        }

        if(table_name.compare("Customer") == 0) {
            std::string index_sql = "create index Customer_index on " + table_temp_name_map["Customer"] + " (Customer_id);";
            // mysqlpp::Query query = conn->query(index_sql);
            // query.execute();
            // query.store(index_sql, index_sql.length());
            query.execute(index_sql.c_str(), index_sql.length());
        }
    }

    // 最后把所有表join起来
    std::string sql = "select ";
    int temp_cnt = 0;
    auto select_attr_map = meta_data_->select_attr_map;
    for(auto iter = select_attr_map->begin(); iter != select_attr_map->end(); ++iter) {
        std::string table_name = iter->first;
        std::vector<std::string>* select_attrs = &(iter->second);

        for(auto attr = select_attrs->begin(); attr != select_attrs->end(); ++ attr) {
            if(temp_cnt > 0) sql += ", ";
            temp_cnt ++;
            sql += table_name + "_" + *(attr);
        }
    }
    sql += " from ";
    for(auto iter = table_temp_name_map.begin(); iter != table_temp_name_map.end(); ++ iter) {
        if(iter != table_temp_name_map.begin()) sql += ", ";
        sql += iter->second;
    }
    auto join_operators = meta_data_->join_operators;
    if(join_operators->size() > 0) {
        sql += " where ";
        for(int i = 0; i < join_operators->size(); ++i) {
            if(i > 0) sql += " and ";
            Predicate predicate = join_operators->at(i);
            sql += table_temp_name_map[predicate.table_name_] + "." + predicate.table_name_ + "_" + predicate.attribute_name_;
            sql += "=";
            sql += table_temp_name_map[predicate.right_table_name_] + "." + predicate.right_table_name_ + "_" + predicate.right_attribute_name_;
        }
    }
    sql += ";";
    // std::cout << sql << std::endl;
    // mysqlpp::Connection* final_conn = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
    // mysqlpp::Query query_final = final_conn->query(sql);
    // query_final.exec();
    for(int i = 0; query.more_results(); ++i)
        query.store_next();
    // std::cout << "finish exec" << std::endl;
    mysqlpp::StoreQueryResult final_results = query.store(sql.c_str(), sql.length());
    //clock_t end_time = clock();

    int record_size = 0;
    int col_size = final_results.field_names()->size();
    RecordPrinter printer(col_size);
    for(auto iter = final_results.begin(); iter != final_results.end(); ++ iter) {
        std::vector<std::string> result;
        mysqlpp::Row row = *iter;
        for(int i = 0; i < col_size; ++i)
            result.emplace_back(row[i]);
        printer.print_record(result);
        record_size++;
    }
    printer.print_record_count(record_size);

    //clock_t end_time = clock();
    drop_temp_table();
    table_temp_name_map.clear();
    temp_table_name.clear();
    //std::cout << "Time used: " << (double)(end_time - begin_time) / CLOCKS_PER_SEC << " sec" << std::endl;
}

// 把query_tree的子树传送给其他站点, 然后获取其他站点的返回结果(临时表)
void Executor::request_remote_execution_result(int root_index) {
//    QueryNode* root = &(meta_data_->query_tree.nodes[root_index]);
//    std::string aim_site = root->get_site_name();
//    std::string ip = meta_data_->site_map[aim_site].first;
//    std::string port = meta_data_->site_map[aim_site].second;
    std::string ip = "192.168.212.150";
    std::string port = "8800";
    std::string sql = "";
    // std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+ std::to_string(root_index)+"_";
    // std::string sql = "SELECT * FROM ";
    // sql += table_name
    brpc::Channel channel;
    initial_channel(channel, ip + ":" + port);
    whiteBear::DDBService_Stub stub(&channel);
    whiteBear::ResultsSQLRequest request;
    whiteBear::ResultsSQLResponse response;
    set_ResultsSQLRequest(request, sql);
    brpc::Controller controller;
    stub.ResultsSQL(&controller, &request, &response, nullptr);
//    std::cout << "finished send request and get response;\n";
    temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(1), std::forward_as_tuple());
    std::vector<std::string>* records = &(temp_table_data.find(1)->second);
    bool success;
    std::string errors;
    get_ResultsSQLResponse(response, success, errors, records);
}

void Executor::print_records(int root_index) {
    int length = 10;
    for(int i = 0; i < length + 2; ++i) std::cout << "-";
    std::cout << std::endl;
    std::cout << "| name" << std::setw(6) << "|";
    std::cout << "\n";
    for(int i = 0; i < length + 2; ++i) std::cout << "-";
    std::cout << std::endl;
    std::vector<std::string>* records = &(temp_table_data.find(0)->second);
    for(auto record = records->begin(); record != records->end(); ++record) {
        std::cout << "| ";
        std::cout << std::setw(length - 3) << *record;
        std::cout << " |\n";
    }
    for(int i = 0; i < length + 2; ++i) std::cout << "-";
    std::cout << std::endl;
}

// QueryTree Executor::get_tree(int root_index) {
//     QueryTree sub_qt;
//     sub_qt.tree_id = qt->tree_id;
//     std::vector<QueryNode> sub_nodes;

//     //遍历原始树的节点
//     for(int i=0;i<qt->nodes.size();i++){
//         //判断是否是要的节点
//         if(qt->nodes[i].node_id==root_index){
//             sub_qt.root_index = i;//每棵子树的nodes
//             QueryNode temp_node = qt->nodes[i];
//             sub_nodes.push_back(temp_node);
//             //判断是否是叶子节点
//             if (temp_node.sons_.size()==0){
//                 sub_qt.nodes = sub_nodes;
//                 sub_qt.n = 1;
//                 return sub_qt;
//             }
//             else{
//                 //如果不是叶子节点,就找它的孩子代表的子树，合并
//                 std:vector<int> sons_id = qt->nodes[i].sons_;
//                 for(int k =0;k<sons_id.size();k++){
//                     QueryTree son_sub_tree = get_tree(Meta *meta_data_,sons_id[k]);
//                     //子树节点都加入本树
//                     for(int j=0;j<son_sub_tree.nodes.size();j++){
//                         sub_nodes.push_back(son_sub_tree.nodes[j]);
//                     }
//                 }
//                 //去除重复的，虽然永远不会重复吧
//                 std::vector<QueryNode> sub_nodes_unique;
//                 int flag = 0;
//                 for(QueryNode tmp_node:sub_nodes){
//                     flag = 0;
//                     for(QueryNode tmp_node_unique: sub_nodes_unique){
//                         if(tmp_node.node_id == tmp_node_unique.node_id){
//                             /* 之前出现过 */
//                             flag = 1;
//                             break;
//                         }
//                     }
//                     if(flag == 0){
//                         /* 之前没出现过，加入列表 */
//                         sub_nodes_unique.push_back(tmp_node);
//                     }
//                 }
//                 sub_qt.nodes = sub_nodes_unique;
//                 sub_qt.n = sub_nodes_unique.size()
//                 return sub_qt;
//                 }
//             }
//         }
//     if(sub_qt.root_index == -1){
//         printf("this node does not exist!\n");
//         sub_qt.nodes = sub_nodes;
//         return sub_qt;
//     }
// }

std::string  Executor::get_sql(QueryNode* node,int tree_id) {//传入-1意味着，是叶子节点
    //SELECT 只会在叶子节点，针对单表 -- From的table和Predicate的table都是原始名字，直接用就行
    // if(node->node_type_==NodeType::SELECTION){
    //     std::string select_sql = "SELECT * FROM "+node->table_name_ ;
    //     std::string predicate_sql = " where ";

    //     for(int i=0;i<node->select_predicates_.size();i++){
    //         Predicate tem_p = node->select_predicates_[i];
    //         //右侧条件
    //         std::string tem_pre_sql = tem_p.table_name_+"."+tem_p.attribute_name_ + 
    //             OperationTypeStr[static_cast<int>(tem_p.operation_type_)] + tem_p.right_value_;
    //         //如果还有左侧条件的话
    //         if(tem_p.left_operation_type_ != OperationType::DEFAULT){
    //             std::string left_pre_sql = tem_p.table_name_+"."+tem_p.attribute_name_ + 
    //             OperationTypeStr[static_cast<int>(tem_p.left_operation_type_)] + tem_p.left_value_;
    //             tem_pre_sql = tem_pre_sql + " and ";
    //             tem_pre_sql += left_pre_sql;
    //         }
    //         if (i!=0){
    //             predicate_sql += " and ";
    //         }
    //         predicate_sql += tem_pre_sql;
    //     }
    //     select_sql += predicate_sql;
    //     return select_sql;
    // }

    // //PROJECT,只会针对单表。project也有可能出现在叶子节点，这个时候表名称就是正常；否则就用孩子节点的编号。
    // else if(node->node_type_==NodeType::PROJECTION){
    //     std::string proj_sql = "SELECT ";
    //     for(int i=0;i<node->attr_name_.size();i++){
    //         std::string tem_attr = node->attr_name_[i];
    //         proj_sql += tem_attr;
    //         if (i!=node->attr_name_.size()-1){
    //             proj_sql = proj_sql + ",";
    //         }
    //     }
    //     proj_sql += " FROM ";
    //     // std::string table_pre = "";
    //     // if(tree_id!=-1){//根据sons的编号加入前缀,这里只会有一个孩子节点
    //     //     int son_node_id = node->sons_[0];
    //     //     table_pre = table_pre+ "tree_"+to_string(tree_id)+"node_"+to_string(son_node_id)+"_";
    //     // }
    //     // proj_sql += table_pre;
    //     // proj_sql += node->table_name_;
    //     if(tree_id!=-1){//不是根节点
    //         int son_node_id = node->sons_[0];
    //         std::string table_pre = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id)+"_";
    //         proj_sql += table_pre;
    //     }
    //     else{
    //         proj_sql += node->table_name_;
    //     }
    //     return proj_sql;
    // }

    // //JOIN 只会针对两个孩子节点的两个表
    // else if(node->node_type_==NodeType::JOIN){
    //     int son_node_id1 = node->sons_[0];
    //     std::string table_pre1 = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id1)+"_";
    //     // std::string table1 = table_pre1 + node->join_predicate_.table_name_;
    //     std::string table1 = table_pre1 ;

    //     int son_node_id2 = node->sons_[1];
    //     std::string table_pre2 = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id2)+"_";
    //     // std::string table2 = table_pre2 + node->join_predicate_.right_table_name_;
    //     std::string table2 = table_pre2;

    //     std::string join_sql = "SELECT * FROM "+ table1 +","+table2; 
    //     std::string predicate_sql = " where ";
    //     predicate_sql += table1+"."+node->join_predicate_.attribute_name_;
    //     predicate_sql = predicate_sql + " = ";
    //     predicate_sql += table2+"."+node->join_predicate_.right_attribute_name_;
    //     join_sql += predicate_sql;
    //     return join_sql;
    // }
    // //UNION，所有孩子节点的表
    // else{
    //     std::string union_sql;
    //     for(int i =0;i<node->sons_.size();i++){
    //         int son_node_id = node->sons_[i];
    //         std::string table_pre = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id)+"_";
    //         if (i==0){
    //             union_sql = "SELECT * FROM "+table_pre;
    //         }
    //         else{
    //             union_sql = union_sql + " UNION ";
    //             union_sql = union_sql + "SELECT * FROM "+table_pre;
    //         }
    //     }
    //     return union_sql;
    // }
}

bool Executor::Data_Select_Thread_Remote(int root_index, std::string site_name_,std::promise<bool> &resultObj ){
    //第一部分，发送子树（节点编号），在对应站点获得子树
    // whiteBear::ResultsSQLRequest request1;
    // whiteBear::ResultsSQLResponse response1;
    // std::string ip = meta_data_->site_map[site_name_].first;
    // std::string port = meta_data_->site_map[site_name_].second;
    // brpc::Channel channel;
    // initial_channel(channel, ip + ":" + port);
    // whiteBear::DDBService_Stub stub(&channel);
    // set_QTRequest(request1, root_index);
    // brpc::Controller controller1;
    // stub.RootIndexService(&controller1, &request1, &response1, nullptr);


    // //第二部分，再次发送节点编号，以及sql语句，获得结果，存储到temp_table中,然后在当前站点创建临时表
    // //在本地直接建表，是不需要属性名的，但是现在我也可以知道属性名吧
    // whiteBear::ResultsSQLRequest request2;
    // whiteBear::ResultsSQLResponse response2;
    // std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+ std::to_string(root_index)+"_";
    // std::string sql = "SELECT * FROM ";
    // sql += table_name;

    // set_ResultsSQLRequest(request2, sql);
    // brpc::Controller controller2;
    // stub.ResultsSQL(&controller2, &request2, &response2, nullptr);

    // // if(temp_table_data.find(root_index) == temp_table_data.end())
    // //     temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(root_index), std::forward_as_tuple());
    // // std::vector<std::string>* records = &(temp_table_data.find(root_index)->second);
    // std::vector<std::string>* records;
    // bool success2;
    // std::string errors2;
    // get_ResultsSQLResponse(response2, success2, errors2, records);

    // auto record = records->begin();

    // std::string create_sql = "CREATE TABLE ";
    // //获取属性名
    // create_sql += table_name +"(" ;
    // create_sql += *record ;
    // create_sql = create_sql+ ")";
    // record = record+1;

    // mysqlpp::Query query_create = mysql_connection->query(create_sql);
    // if (query_create.exec()) {
    //     std::cout << " Create Table "<<table_name<< "REMORT success!" << std::endl;
    // }
    // else {
    //     std::cout <<" Create Table "<<table_name<< "REMORT FAIL! " << std::endl;
    //     resultObj.set_value(false);
    //     return;
    // }

    // std::string insert_sql = "INSERT INTO ";
    // insert_sql += table_name + "VALUES";

    // for(record; record != records->end(); ++record) {
    //     insert_sql += "(" + *record +")";
    //     if (record != records->end()-1){
    //         insert_sql = insert_sql+",";
    //     }
    // }
    // mysqlpp::Query query_insert = mysql_connection->query(insert_sql);
    // if (query_insert.exec()) {
    //     std::cout << " INSERT Table "<<table_name<< "REMORT success!" << std::endl;
    //     resultObj.set_value(true);
    //     return;
    // }
    // else {
    //     std::cout <<" INSERT Table "<<table_name<< "REMORT FAIL! " << std::endl;
    //     resultObj.set_value(false);
    //     return;
    // }
}

//递归遍历孩子节点就可以了
bool Executor::Data_Select_Thread(int root_index,std::promise<bool> &resultObj) {
    // // QueryTree tree = get_tree(root_index);
    // // QueryTree sub_tree = ;
    // // QueryNode = child_node;
    // // QueryNode root_node = tree.nodes[root_index];
    // // std::vector<int> childs_id = root_node.sons_;
    // QueryNode cur_node = qt->nodes[root_index];

    // //判断是否是叶子节点
    // //叶子结点只会是select
    // if(cur_node.sons_.size()==0){
    //     //拼接语句，从mysql中取出结果然后存到临时表中，所有的临时表，都只有节点的名字
    //     //create table tree_1_node_5 select * from 
    //     if(cur_node.site_ == meta_data_->local_site_name){
    //         std::string create_sql = "create table ";
    //         std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
    //         std::string exec_sql = get_sql(&cur_node,-1);
    //         std::string final_sql = create_sql+table_name +" " + exec_sql;
    //         mysqlpp::Query query = mysql_connection->query(final_sql);
    //         try{
    //         if (query.exec()) {
    //             std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf success!" << std::endl;
    //             resultObj.set_value(true);
    //             return true; 
    //         }
    //         else {
    //             std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf FAIL! " << std::endl;
    //             resultObj.set_value(false);
    //             return false;
    //         }
    //         }
    //         catch(mysqlpp::Exception& er){
    //             std::cout << "Error: "<<" Create Table "<<table_name<< " leaf FAIL! "  << er.what() << std::endl;
    //         }
    //     }
    //     //基本不可能在叶子节点，还是在非本站点执行
    //     else{
    //         std::promise<bool> tem_result;
    //         std::future<bool> tem_bool;
    //         tem_bool = tem_result.get_future();
    //         Data_Select_Thread_Remote(cur_node.node_id,cur_node.site_,tem_result);
    //         bool success = tem_bool.get();

    //         if(success){
    //             std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf success!" << std::endl;
    //             resultObj.set_value(true);
    //             return true;
    //         }
    //         else {
    //             std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf FAIL! " << std::endl;
    //             resultObj.set_value(false);
    //             return false;
    //         }
    //     }
    // }
    
    // //非叶子结点
    // else{
    //     std::promise<bool> resultObjs[MAXTHREAD]; 
    //     std::thread select_threads[MAXTHREAD];
    //     std::future<bool> select_thread_bool[MAXTHREAD];
    //     int q = 0; // 计数
    //     for(q=0;q<cur_nodes.sons_.size();q++){
    //         int cur_son_id = cur_nodes.sons_[q];
    //         printf("child %d on processing ...\n", cur_nodes.sons_[q]);
    //         select_thread_bool[q] = resultObjs[q].get_future();
    //         std::string cur_son_site = qt->nodes[cur_son_id].site_
    //         if(cur_son_site == meta_data_->local_site_name){
    //             // res_sub_tree = Data_Select_Execute(sub_tree);
    //             select_threads[q] = std::thread(Data_Select_Thread,cur_son_id, std::ref(resultObjs[q]));
    //         }
    //         else{
    //             // res_sub_tree = RPC_Data_Select_Execute(sub_tree, site);
    //             // res_sub_tree = Data_Select_Execute(sub_tree); // 本地调试用
    //             select_threads[q] = std::thread(Data_Select_Thread_Remote, cur_son_id, cur_son_site, std::ref(resultObjs[q]));
    //             // select_threads[q] = std::thread(Data_Select_Execute_Thread, sub_tree, std::ref(resultObjs[q])); // 本地调试用
    //         }
    //     }

    //     for(q=0;q<cur_nodes.sons_.size();q++){
    //         bool success = select_thread_bool[q].get();
    //         if (success){
    //             resultObj.set_value(true);
    //             std::cout  <<"Executing son "<< std::to_string(q) << "Success" << std::endl;
    //         }
    //         else{
    //             resultObj.set_value(false);
    //             std::cout  <<"Executing son "<< std::to_string(q) << "FAIL!" << std::endl;
    //             // return;
    //         }
    //     }
    //     //等待所有线程完成
    //     for(q=0;q<cur_nodes.sons_.size(); q++){
    //         select_threads[q].join();
    //     }

    //     //根据根节点的sql语句整合
    //     std::string create_sql = "create table ";
    //     std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
    //     std::string exec_sql = get_sql(&cur_node,qt->tree_id);
    //     std::string final_sql = create_sql+table_name +" " + exec_sql;
    //     mysqlpp::Query query = mysql_connection->query(final_sql);
    //     if (query.exec()) {
    //         std::cout <<" Create Table"<<table_name<< " Success!" << std::endl;
    //         resultObj.set_value(true);
    //     }
    //     else {
    //         std::cout  <<"Create Table "<<table_name<< " FAIL! " << std::endl;
    //         resultObj.set_value(false);
    //         return false;
    //     }

    //     //删除所有孩子节点的临时表
    //     std::string delete_sql = "DROP TABLE ";
    //     for(q=0;q<cur_nodes.sons_.size(); q++){
    //         int cur_son_id = cur_nodes.sons_[q];
    //         std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_son_id)+"_";
    //         std::string delete_sql_son = delete_sql+table_name;
    //         mysqlpp::Query query_d = mysql_connection->query(delete_sql_son);
    //         if (query_d.exec()) {
    //         std::cout  <<"DROP Table "<<table_name<< " Success!" << std::endl;
    //         resultObj.set_value(true);
    //         }
    //         else {
    //             std::cout << "DROP Table "<<table_name<< "  FAIL! " << std::endl;
    //             resultObj.set_value(false);
    //             return false;
    //         }
    //     }
    // }
    // return true;
}

bool Executor::Data_Select(int root_index){//只是去除了所有的resultObj
    // QueryNode cur_node = qt->nodes[root_index];
    // //判断是否是叶子节点
    // //叶子结点只会是select
    // if(cur_node.sons_.size()==0){
    //     //拼接语句，从mysql中取出结果然后存到临时表中，所有的临时表，都只有节点的名字
    //     //create table tree_1_node_5 select * from 
    //     if(cur_node.site_ == meta_data_->local_site_name){
    //         std::string create_sql = "create table ";
    //         std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
    //         std::string exec_sql = get_sql(&cur_node,-1);
    //         std::string final_sql = create_sql+table_name +" " + exec_sql;
    //         mysqlpp::Query query = mysql_connection->query(final_sql);
    //         if (query.exec()) {
    //             std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf success!" << std::endl;
    //             return true; 
    //         }
    //         else {
    //             std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf FAIL! " << std::endl;
    //             return false;
    //         }
    //     }
    //     //基本不可能在叶子节点，还是在非本站点执行
    //     else{
    //         std::promise<bool> tem_result;
    //         std::future<bool> tem_bool;
    //         tem_bool = tem_result.get_future();
    //         Data_Select_Thread_Remote(cur_node.node_id,cur_node.site_,tem_result);
    //         bool success = tem_bool.get();

    //         if(success){
    //             std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf success!" << std::endl;
    //             return true;
    //         }
    //         else {
    //             std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf FAIL! " << std::endl;
    //             return false;
    //         }
    //     }
    // }
    
    // //非叶子结点
    // else{
    //     std::promise<bool> resultObjs[MAXTHREAD]; 
    //     std::thread select_threads[MAXTHREAD];
    //     std::future<bool> select_thread_bool[MAXTHREAD];
    //     int q = 0; // 计数
    //     for(q=0;q<cur_nodes.sons_.size();q++){
    //         int cur_son_id = cur_nodes.sons_[q];
    //         printf("child %d on processing ...\n", cur_nodes.sons_[q]);
    //         select_thread_bool[q] = resultObjs[q].get_future();
    //         std::string cur_son_site = qt->nodes[cur_son_id].site_
    //         if(cur_son_site == meta_data_->local_site_name){
    //             // res_sub_tree = Data_Select_Execute(sub_tree);
    //             select_threads[q] = std::thread(Data_Select_Thread,cur_son_id, std::ref(resultObjs[q]));
    //         }
    //         else{
    //             // res_sub_tree = RPC_Data_Select_Execute(sub_tree, site);
    //             // res_sub_tree = Data_Select_Execute(sub_tree); // 本地调试用
    //             select_threads[q] = std::thread(Data_Select_Thread_Remote, cur_son_id, cur_son_site, std::ref(resultObjs[q]));
    //             // select_threads[q] = std::thread(Data_Select_Execute_Thread, sub_tree, std::ref(resultObjs[q])); // 本地调试用
    //         }
    //     }

    //     for(q=0;q<cur_nodes.sons_.size();q++){
    //         bool success = select_thread_bool[q].get();
    //         if (success){
    //             std::cout  <<"Executing son "<< std::to_string(q) << "Success" << std::endl;
    //         }
    //         else{
    //             std::cout  <<"Executing son "<< std::to_string(q) << "FAIL!" << std::endl;
    //             // return;
    //         }
    //     }
    //     //等待所有线程完成
    //     for(q=0;q<cur_nodes.sons_.size(); q++){
    //         select_threads[q].join();
    //     }

    //     //根据根节点的sql语句整合
    //     std::string create_sql = "create table ";
    //     std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
    //     std::string exec_sql = get_sql(&cur_node,qt->tree_id);
    //     std::string final_sql = create_sql+table_name +" " + exec_sql;
    //     mysqlpp::Query query = mysql_connection->query(final_sql);
    //     if (query.exec()) {
    //         std::cout <<" Create Table"<<table_name<< " Success!" << std::endl;
    //     }
    //     else {
    //         std::cout  <<"Create Table "<<table_name<< " FAIL! " << std::endl;
    //         return false;
    //     }

    //     //删除所有孩子节点的临时表
    //     std::string delete_sql = "DROP TABLE ";
    //     for(q=0;q<cur_nodes.sons_.size(); q++){
    //         int cur_son_id = cur_nodes.sons_[q];
    //         std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_son_id)+"_";
    //         std::string delete_sql_son = delete_sql+table_name;
    //         mysqlpp::Query query_d = mysql_connection->query(delete_sql_son);
    //         if (query_d.exec()) {
    //         std::cout  <<"DROP Table "<<table_name<< " Success!" << std::endl;
    //         }
    //         else {
    //             std::cout << "DROP Table "<<table_name<< "  FAIL! " << std::endl;
    //             return false;
    //         }
    //     }
    // }
    // return true;
}

std::vector<std::string> Executor::Data_Select_Single(std::vector<std::string> sql_vec, std::vector<std::string> site_vec){
    // std::vector<std::vector<std::string>> results[site_vec.size()];
    for(int i = 0; i < sql_vec.size(); ++i)
        std::cout << sql_vec[i] << " " << site_vec[i] << std::endl;
    std::vector<std::string> result0;
    std::vector<std::string> result1;
    // std::thread load_thread0;
    // std::thread load_thread1;

    /* 开启一个分片并在对应site存储的线程，通过传promise类给线程，让线程把结果给存到results里面，实现结果返回 */
    // load_thread0 = std::thread(&Executor::Data_Select_Single_Thread, this, sql_vec[0],site_vec[0], &result0);
    // load_thread1 = std::thread(&Executor::Data_Select_Single_Thread, this, sql_vec[1],site_vec[1], &result1);
    Data_Select_Single_Thread(sql_vec[0], site_vec[0], &result0);
    Data_Select_Single_Thread(sql_vec[1], site_vec[1], &result1);

    //等所有线程执行完后，才继续
    // load_thread0.join();
    // load_thread1.join();

    std::cout << "finish select for delete" << std::endl;
    std::cout << result0.size() << std::endl;

    std::sort(result0.begin(), result0.end());
    std::sort(result1.begin(), result1.end());
    std::cout << result1.size() << std::endl;

    std::vector<std::string> v_intersection1(10);
    // std::vector<std::string> v_intersection2;

    std::vector<std::string>::iterator it = std::set_intersection(result0.begin(), result0.end(),
        result1.begin(), result1.end(), v_intersection1.begin());

    v_intersection1.resize(it - v_intersection1.begin());
    
    std::cout << v_intersection1.size()<<" ";
    std::cout << v_intersection1[0] << std::endl;

    // if(site_vec.size()>=3){
    //     std::sort(results[2].begin(), results[2].end());
    //     std::sort(v_intersection1.begin(),v_intersection1.end());
    //     std::set_intersection(results[2].begin(), results[2].end(),
    //         v_intersection1.begin(), v_intersection1.end(),
    //         std::back_inserter(v_intersection2));
    // }
    // else{
    //     return v_intersection1;
    // }

    // if(site_vec.size()>=4){
    //     std::sort(results[3].begin(), results[3].end());
    //     std::sort(v_intersection2.begin(),v_intersection2.end());
    //     std::vector<std::string> v_intersection3;
    //     std::set_intersection(results[3].begin(), results[3].end(),
    //         v_intersection2.begin(), v_intersection2.end(),
    //         std::back_inserter(v_intersection3));
    //     return v_intersection3;
    // }
    // else{
    //     return v_intersection2;
    // }
    return v_intersection1;

}

void Executor::Data_Select_Single_Thread(std::string sql_, std::string site_,std::vector<std::string>* results){
    // std::cout << site_ << std::endl;
    //当前站点
    try{
        if (site_.compare(meta_data_->local_site_name) == 0) {
            mysqlpp::Query query1 = mysql_connection->query(sql_);
            // std::cout << "step0\n";
            //只会取出来一个属性
            if (mysqlpp::StoreQueryResult res = query1.store()){
                // std::cout << "step1\n";
                    mysqlpp::StoreQueryResult::const_iterator it;
                    mysqlpp::FieldTypes::value_type ft = res.field_type(0);
                    for (it = res.begin(); it != res.end(); ++it) {
                        // std::cout << "loop\n";
                        std::string temp_str = "";
                        mysqlpp::Row row = *it;
                        if(strstr(ft.sql_name(),"CHAR")){
                            for(int i=0;i<row[0].size();i++){
                                temp_str = temp_str+row[0][i];
                            } 
                        }
                        if(strstr(ft.sql_name(),"INT")){
                            int aa = row[0];
                            temp_str = std::to_string(aa);
                        }
                        results->push_back(temp_str);
                    }
            }
            else {
                std::cout << "Data_Select_Single Fail " << query1.error() << std::endl;
            }
            // std::cout << "success local\n";
        } 
        else {
            std::vector<std::string> records;
            if (request_remote_execution_result(sql_, site_,&records)) {
                auto record = records.begin();
                record ++;
                for(; record != records.end(); ++record) {
                    results->push_back(*record);
                }
                std::cout << "success remote\n";
            }
            else {
                std::cout << "Data_Select_Single REMOTE" <<site_ << " FAIL!" << std::endl;
            }
        } 
    }
    catch(const mysqlpp::Exception& er){
        std::cout<<"Data_Select_Single 执行错误"<<er.what()<<std::endl;
    }

    return;
}

//Select操作结束后，再从最开始的根节点中，打印取出来的树
void Executor::Data_Select_Print(int root_index){
    // std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(root_index)+"_";
    // std::string sel_sql = "SELECT * FROM" + table_name;
    // mysqlpp::Query query1 = mysql_connection->query(sel_sql);
    // if (mysqlpp::StoreQueryResult res = query1.store())
    //     {
    //         std::cout << "We have:" << std::endl;
    //         mysqlpp::StoreQueryResult::const_iterator it;
    //         for (it = res.begin(); it != res.end(); ++it)
    //         {
    //             mysqlpp::Row row = *it;
    //             std::cout<<row.size();
    //             for (int jk=0;jk<row.size();jk++){
    //                 std::cout << '\t' << row[jk] ;
    //             }
    //             std::cout<<std::endl;
    //         }
    //     }
    // else
    //     {
    //         std::cout << "Failed to get item list: " << query1.error() << std::endl;
    //         return ;
    //     }
    
    // //删除这个表
    // std::string del_sql = "DROP TABLE" + table_name;
    // mysqlpp::Query query_del = mysql_connection->query(del_sql);
    // if (query_del.exec()) {
    //         std::cout  <<"DROP Table "<<table_name<< " Success!" << std::endl;
    //         }
    //         else {
    //             std::cout << "DROP Table "<<table_name<< "  FAIL! " << std::endl;
    //             return false;
    //         }
    // return true;
}

bool Executor:: request_remote_execution_result(std::string sql_,std::string site_,std::vector<std::string>* records){
    std::string aim_site = site_;
    std::string ip = meta_data_->site_map[aim_site].first;
    std::string port = "8800";
    std::string sql = sql_;
    brpc::Channel channel;
    initial_channel(channel, ip + ":" + port);
    whiteBear::DDBService_Stub stub(&channel);
    whiteBear::ResultsSQLRequest request;
    whiteBear::ResultsSQLResponse response;
    set_ResultsSQLRequest(request, sql);
    brpc::Controller controller;
    stub.ResultsSQL(&controller, &request, &response, nullptr);
//    std::cout << "finished send request and get response;\n";
    // temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(root_index), std::forward_as_tuple());
    // std::vector<std::string>* records = &(temp_table_data.find(root_index)->second);
    bool success;
    std::string errors;
    get_ResultsSQLResponse(response, success, errors, records);
    return success;
}

bool Executor:: remote_execution_insert_delete(std::string sql_,std::string site_){
    std::string aim_site = site_;
    std::string ip = meta_data_->site_map[aim_site].first;
    std::cout << "site ip: " << ip << std::endl;
    std::string port = "8800";
    std::string sql = sql_;
    // std::cout << "execute sql: " << sql << std::endl;
    brpc::Channel channel;
    initial_channel(channel, ip + ":" + port);
    whiteBear::DDBService_Stub stub(&channel);
    whiteBear::NonResultsSQLRequest request;
    whiteBear::NonResultsSQLResponse response;
    set_NonResultsSQLRequest(request, sql);
    brpc::Controller controller;
    stub.NonResultsSQL(&controller, &request, &response, nullptr);
//    std::cout << "finished send request and get response;\n";
    // temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(root_index), std::forward_as_tuple());
    // std::vector<std::string>* records = &(temp_table_data.find(root_index)->second);
    bool success;
    std::string errors;
    get_NonResultsSQLResponse(response, success, errors);
    return success;
}


void Executor::Data_Insert_Delete_Thread(std::string sql_,std::string site_,bool* result) {
    std::cout << site_ << std::endl;
    if (sql_.size()==0){//每个站点都有sql_，空SQL代表当前站点不需要执行。
        *result = true;
        return;
    }
    // 保证每条语句不为空
    if (site_ == meta_data_->local_site_name) {//判断是不是当前站点
        mysqlpp::Connection* conn = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
        // mysqlpp::Query query2 = mysql_connection->query(sql_);
        mysqlpp::Query query2 = conn->query(sql_);
        // std::cout << sql_.substr(0, 100) << std::endl;
        if (query2.exec()) {
            std::cout << meta_data_->local_site_name << " success!" << std::endl;
        }
        else {
            std::cout << "Failed to get item list: " << query2.error() << std::endl;
            *result = false;
            return;
        }
    } 
    else {
        if (remote_execution_insert_delete(sql_, site_)) {
            std::cout << site_ << " success!" << std::endl;
        } else {
            *result = false;
            return;
        }
    }
}

bool Executor:: Data_Insert_Delete(std::vector<std::string> sql_vec, std::vector<std::string> site_vec){ //给每个站点都开一个线程，
    // int n = sql_vec.size();
    // for(int i = 0; i < n; ++i)
    //     std::cout << site_vec[i] << std::endl;

    time_t start_time = time(NULL);
    bool results[site_vec.size()];
    std::thread load_threads[site_vec.size()];
    int i;
    for(i = 0; i < site_vec.size(); i++){
        /* 开启一个分片并在对应site存储的线程，通过传promise类给线程，让线程把结果给存到results里面，实现结果返回 */
        load_threads[i] = std::thread(&Executor::Data_Insert_Delete_Thread, this, sql_vec[i],site_vec[i], &results[i]);
        // Data_Insert_Delete_Thread(sql_vec[i], site_vec[i], &results[i]);
    }

    //等所有线程执行完后，才继续
    for(i = 0; i < site_vec.size(); i++){
        load_threads[i].join();
    }

    bool success= true;
    for(i = 0; i < site_vec.size(); i++) {
        if (!results[i]) {
            success = false;
            break;
        }
    }
    time_t end_time = time(NULL);
    double time_spend = difftime(end_time, start_time);
    /* 构造输出语句 */
    std::string time_output = std::to_string(time_spend);
    time_output.append(" seconds used.\n");
    std::string output_sentence = "";
    if (success){time_output.append("success");}
    // else{time_output.append("failure");}
    output_sentence.append(time_output);
    std::cout<<output_sentence<<std::endl;

    return success;
}


void Executor::create_db_remote(std::string db_name) {
    // std::ifstream file;
    std::string command_prefix = "sshpass -p \"ddbms500\" scp -o StrictHostKeyChecking=no " + meta_data_->db_name_file + " user-linux@";
    for(auto site_name_ip : meta_data_->site_map) {
        std::string site_name = site_name_ip.first;
        std::string site_ip = site_name_ip.second.first;
        if(site_name.compare(meta_data_->local_site_name) == 0) continue;
        std::string command = command_prefix + site_ip + ":" + meta_data_->file_path_prefix;
        system(command.c_str());
    }
}
