//
// Created by sxy on 22-11-14.
//
#include "Executor.h"

void Executor::exec_sql_local(std::string sql) {
    sql = "select * from Publisher where nation = \"PRC\";";
    mysqlpp::Query query = mysql_connection->query(sql);
    mysqlpp::StoreQueryResult result = query.store();

    int record_num = result.num_rows();
    std::cout << "id    name    nation\n";
    for(int i = 0; i < record_num; ++i) {
        std::cout << result[i]["id"] << " " << result[i]["name"] << " " << result[i]["nation"] << std::endl;
    }
}

// 执行本地站点上的执行计划
void Executor::exec_query_tree(int root_index) {
    QueryTree* query_tree = &(meta_data_->query_tree);
    QueryNode* root = &(query_tree->nodes[root_index]);
    // 如果根节点不是本地站点执行,那么就发送一个给其他站点的request请求
    if(root->get_site_name() != meta_data_->local_site_name) {
        request_remote_execution_result(root_index);
    }
    // 如果是projection, 并且只有一个孩子节点, 并且孩子节点是leaf, 那么直接执行sql语句
    if(root->get_type() == NodeType::PROJECTION && root->get_sons()->size() == 1) {
        int child_index = *(root->get_sons()->begin());
        QueryNode* child = &query_tree->nodes[child_index];
        if(child->is_leaf() && child->get_type() == NodeType::SELECTION) {
            Table* table = &(meta_data_->tables[meta_data_->table_index[child->get_table_name()]]);
            std::vector<Attribute>* table_attrs = &(table->attributes_);

            std::string sql = "";
            sql += "select ";
            std::vector<std::string>* attrs = root->get_attrs();
            for(auto attr = attrs->begin(); attr != attrs->end(); ++attr) {
                sql += *attr;
                if(attrs->back().compare(*attr) != 0) sql += ", ";
                else sql += " ";
            }
            sql += "from ";
            sql += child->get_table_name();
            sql += "where ";
            // 生成where表达式
            std::vector<Predicate>* predicates = child->get_select_predicates();
            for(auto predicate = predicates->begin(); predicate != predicates->end(); ++predicate) {
                sql += predicate->attribute_name_ +
                        OperationTypeStr[static_cast<int>(predicate->operation_type_)];
                Attribute* attr_in_meta_data = nullptr;
                for(auto iter = table_attrs->begin(); iter != table_attrs->end(); ++iter) {
                    if(iter->attr_name_.compare(predicate->attribute_name_) == 0) {
                        attr_in_meta_data = &*iter;
                        break;
                    }
                }
                if(attr_in_meta_data->type_ == AttributeType::CHAR || attr_in_meta_data->type_ == AttributeType::TEXT) {
                    sql += "'" + predicate->right_value_ + "' ";
                }
                else {
                    sql += predicate->right_value_ + " ";
                }
                if(&*predicate != &predicates->back()) {
                    sql += "and ";
                }
                else {
                    sql += ";";
                }
            }
            exec_sql_local(sql);
            return;
        }
    }
    // 如果是projection, 并且是叶子节点,可以直接执行sql语句
    if(root->is_leaf() && root->get_type() == NodeType::PROJECTION) {
        std::string sql = "";
        sql += "select ";
        std::vector<std::string> *attrs = root->get_attrs();
        for (auto attr = attrs->begin(); attr != attrs->end(); ++attr) {
            sql += *attr;
            if (attrs->back().compare(*attr) != 0) sql += ", ";
            else sql += " ";
        }
        sql += "from ";
        sql += root->get_table_name();
        sql += ";";
        exec_sql_local(sql);
        return;
    }

    // 非叶子节点目前只有union和join
    if(root->get_type() == NodeType::UNION) {
        // 把节点
    }
    else {

    }
}

// 把query_tree的子树传送给其他站点, 然后获取其他站点的返回结果(临时表)
void Executor::request_remote_execution_result(int root_index) {
    QueryNode* root = &(meta_data_->query_tree.nodes[root_index]);
    std::string aim_site = root->get_site_name();
    std::string ip = meta_data_->site_map[aim_site].first;
    std::string port = meta_data_->site_map[aim_site].second;


}