//
// Created by sxy on 22-11-14.
//
#include "Executor.h"
#include "NetworkRequest.h"

void Executor::exec_sql_local(std::string sql) {
    sql = sql_;
    mysqlpp::Query query = mysql_connection->query(sql);
    mysqlpp::StoreQueryResult result = query.store();

    int record_num = result.num_rows();
//    std::cout << record_num << std::endl;
//    std::cout << "name\n";
//    for(int i = 0; i < record_num; ++i) {
//        std::cout << result[i]["name"] << std::endl;
//    }

    temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(0), std::forward_as_tuple());
    std::vector<std::string>* records = &(temp_table_data.find(0)->second);
//    records->clear();
    for(int i = 0; i < record_num; ++i) {
//        std::cout<<i << " " << records->size() << " ";
        records->emplace_back(result[i]["name"]);
//        std::cout << records->size() << std::endl;
    }
//    std::cout << records->size() << std::endl;
}

// 执行本地站点上的执行计划
void Executor::exec_query_tree(int root_index) {
    exec_sql_local("");
    request_remote_execution_result(-1);
    std::vector<std::string>* total_records = &(temp_table_data.find(0)->second);
    std::vector<std::string>* sub_records = &(temp_table_data.find(1)->second);
    for(auto record = sub_records->begin(); record != sub_records->end(); ++record)
        total_records->emplace_back(*record);
//    QueryTree* query_tree = &(meta_data_->query_tree);
//    QueryNode* root = &(query_tree->nodes[root_index]);
//    // 如果根节点不是本地站点执行,那么就发送一个给其他站点的request请求
//    if(root->get_site_name() != meta_data_->local_site_name) {
//        request_remote_execution_result(root_index);
//    }
//    // 如果是projection, 并且只有一个孩子节点, 并且孩子节点是leaf, 那么直接执行sql语句
//    if(root->get_type() == NodeType::PROJECTION && root->get_sons()->size() == 1) {
//        int child_index = *(root->get_sons()->begin());
//        QueryNode* child = &query_tree->nodes[child_index];
//        if(child->is_leaf() && child->get_type() == NodeType::SELECTION) {
//            Table* table = &(meta_data_->tables[meta_data_->table_index[child->get_table_name()]]);
//            std::vector<Attribute>* table_attrs = &(table->attributes_);
//
//            std::string sql = "";
//            sql += "select ";
//            std::vector<std::string>* attrs = root->get_attrs();
//            for(auto attr = attrs->begin(); attr != attrs->end(); ++attr) {
//                sql += *attr;
//                if(attrs->back().compare(*attr) != 0) sql += ", ";
//                else sql += " ";
//            }
//            sql += "from ";
//            sql += child->get_table_name();
//            sql += "where ";
//            // 生成where表达式
//            std::vector<Predicate>* predicates = child->get_select_predicates();
//            for(auto predicate = predicates->begin(); predicate != predicates->end(); ++predicate) {
//                sql += predicate->attribute_name_ +
//                        OperationTypeStr[static_cast<int>(predicate->operation_type_)];
//                Attribute* attr_in_meta_data = nullptr;
//                for(auto iter = table_attrs->begin(); iter != table_attrs->end(); ++iter) {
//                    if(iter->attr_name_.compare(predicate->attribute_name_) == 0) {
//                        attr_in_meta_data = &*iter;
//                        break;
//                    }
//                }
//                if(attr_in_meta_data->type_ == AttributeType::CHAR || attr_in_meta_data->type_ == AttributeType::TEXT) {
//                    sql += "'" + predicate->right_value_ + "' ";
//                }
//                else {
//                    sql += predicate->right_value_ + " ";
//                }
//                if(&*predicate != &predicates->back()) {
//                    sql += "and ";
//                }
//                else {
//                    sql += ";";
//                }
//            }
//            exec_sql_local(sql);
//            return;
//        }
//    }
//    // 如果是projection, 并且是叶子节点,可以直接执行sql语句
//    if(root->is_leaf() && root->get_type() == NodeType::PROJECTION) {
//        std::string sql = "";
//        sql += "select ";
//        std::vector<std::string> *attrs = root->get_attrs();
//        for (auto attr = attrs->begin(); attr != attrs->end(); ++attr) {
//            sql += *attr;
//            if (attrs->back().compare(*attr) != 0) sql += ", ";
//            else sql += " ";
//        }
//        sql += "from ";
//        sql += root->get_table_name();
//        sql += ";";
//        exec_sql_local(sql);
//        return;
//    }
//

//    // 非叶子节点目前只有union和join
//    if(root->get_type() == NodeType::UNION) {
//        // 把所有的data都union起来
//        std::vector<std::string>* records = &(temp_table_data.find(root_index)->second);
//        std::vector<int>* sons = root->get_sons();
//        for(auto son = sons->begin(); son != sons->end(); ++son) {
//            exec_query_tree(*son);
//            std::vector<std::string>* child_records = &(temp_table_data.find(*son)->second);
//            for(auto record = child_records->begin(); record != child_records->end(); ++record) {
//                records->emplace_back(*record);
//            }
//        }
//    }
//    else {
//        // join两张表
//        std::vector<int>* sons = root->get_sons();
//        int left_child_index = *(sons->begin());
//        int right_child_index = sons->back();
//        QueryNode* left_child = &(query_tree->nodes[left_child_index]);
//        QueryNode* right_child = &(query_tree->nodes[right_child_index]);
//
//        // 找到需要join的两张表各自的属性下标
//        Predicate* predicate = root->get_join_predicate();
//        std::string left_attr_name = predicate->attribute_name_;
//        std::string right_attr_name = predicate->right_attribute_name_;
//        Table* left_table = meta_data_->temp_tables[meta_data_->temp_table_index()]
//    }
}

// 把query_tree的子树传送给其他站点, 然后获取其他站点的返回结果(临时表)
void Executor::request_remote_execution_result(int root_index) {
//    QueryNode* root = &(meta_data_->query_tree.nodes[root_index]);
//    std::string aim_site = root->get_site_name();
//    std::string ip = meta_data_->site_map[aim_site].first;
//    std::string port = meta_data_->site_map[aim_site].second;
    std::string ip = "192.168.212.150";
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
