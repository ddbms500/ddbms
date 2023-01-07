//
// Created by sxy on 22-11-14.
//
#include "Executor.h"
#include "NetworkRequest.h"
#include <string>
#include <thread>
#include <algorithm>

void Executor::exec_sql_local(std::string sql) {//先实现非select，
//    sql = sql_;
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
    //逻辑是：去metadata里面用index找到这个点在的位置，然后从这个点开始DFS，如果是自己站点要做的，就做。如果不是，就多开一个线程request
//    exec_sql_local("");
//    request_remote_execution_result(-1);
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
    if(node->node_type_==NodeType::SELECTION){
        std::string select_sql = "SELECT * FROM "+node->table_name_ ;
        std::string predicate_sql = " where ";

        for(int i=0;i<node->select_predicates_.size();i++){
            Predicate tem_p = node->select_predicates_[i];
            //右侧条件
            std::string tem_pre_sql = tem_p.table_name_+"."+tem_p.attribute_name_ + 
                OperationTypeStr[static_cast<int>(tem_p.operation_type_)] + tem_p.right_value_;
            //如果还有左侧条件的话
            if(tem_p.left_operation_type_ != OperationType::DEFAULT){
                std::string left_pre_sql = tem_p.table_name_+"."+tem_p.attribute_name_ + 
                OperationTypeStr[static_cast<int>(tem_p.left_operation_type_)] + tem_p.left_value_;
                tem_pre_sql = tem_pre_sql + " and ";
                tem_pre_sql += left_pre_sql;
            }
            if (i!=0){
                predicate_sql += " and ";
            }
            predicate_sql += tem_pre_sql;
        }
        select_sql += predicate_sql;
        return select_sql;
    }

    //PROJECT,只会针对单表。project也有可能出现在叶子节点，这个时候表名称就是正常；否则就用孩子节点的编号。
    else if(node->node_type_==NodeType::PROJECTION){
        std::string proj_sql = "SELECT ";
        for(int i=0;i<node->attr_name_.size();i++){
            std::string tem_attr = node->attr_name_[i];
            proj_sql += tem_attr;
            if (i!=node->attr_name_.size()-1){
                proj_sql = proj_sql + ",";
            }
        }
        proj_sql += " FROM ";
        // std::string table_pre = "";
        // if(tree_id!=-1){//根据sons的编号加入前缀,这里只会有一个孩子节点
        //     int son_node_id = node->sons_[0];
        //     table_pre = table_pre+ "tree_"+to_string(tree_id)+"node_"+to_string(son_node_id)+"_";
        // }
        // proj_sql += table_pre;
        // proj_sql += node->table_name_;
        if(tree_id!=-1){//不是根节点
            int son_node_id = node->sons_[0];
            std::string table_pre = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id)+"_";
            proj_sql += table_pre;
        }
        else{
            proj_sql += node->table_name_;
        }
        return proj_sql;
    }

    //JOIN 只会针对两个孩子节点的两个表
    else if(node->node_type_==NodeType::JOIN){
        int son_node_id1 = node->sons_[0];
        std::string table_pre1 = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id1)+"_";
        // std::string table1 = table_pre1 + node->join_predicate_.table_name_;
        std::string table1 = table_pre1 ;

        int son_node_id2 = node->sons_[1];
        std::string table_pre2 = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id2)+"_";
        // std::string table2 = table_pre2 + node->join_predicate_.right_table_name_;
        std::string table2 = table_pre2;

        std::string join_sql = "SELECT * FROM "+ table1 +","+table2; 
        std::string predicate_sql = " where ";
        predicate_sql += table1+"."+node->join_predicate_.attribute_name_;
        predicate_sql = predicate_sql + " = ";
        predicate_sql += table2+"."+node->join_predicate_.right_attribute_name_;
        join_sql += predicate_sql;
        return join_sql;
    }
    //UNION，所有孩子节点的表
    else{
        std::string union_sql;
        for(int i =0;i<node->sons_.size();i++){
            int son_node_id = node->sons_[i];
            std::string table_pre = "tree_"+std::to_string(tree_id)+"node_"+std::to_string(son_node_id)+"_";
            if (i==0){
                union_sql = "SELECT * FROM "+table_pre;
            }
            else{
                union_sql = union_sql + " UNION ";
                union_sql = union_sql + "SELECT * FROM "+table_pre;
            }
        }
        return union_sql;
    }
}



bool Executor::Data_Select_Thread_Remote(int root_index, std::string site_name_,std::promise<bool> &resultObj ){
    //第一部分，发送子树（节点编号），在对应站点获得子树
    whiteBear::ResultsSQLRequest request1;
    whiteBear::ResultsSQLResponse response1;
    std::string ip = meta_data_->site_map[site_name_].first;
    std::string port = meta_data_->site_map[site_name_].second;
    brpc::Channel channel;
    initial_channel(channel, ip + ":" + port);
    whiteBear::DDBService_Stub stub(&channel);
    set_QTRequest(request1, root_index);
    brpc::Controller controller1;
    stub.RootIndexService(&controller1, &request1, &response1, nullptr);


    //第二部分，再次发送节点编号，以及sql语句，获得结果，存储到temp_table中,然后在当前站点创建临时表
    //在本地直接建表，是不需要属性名的，但是现在我也可以知道属性名吧
    whiteBear::ResultsSQLRequest request2;
    whiteBear::ResultsSQLResponse response2;
    std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+ std::to_string(root_index)+"_";
    std::string sql = "SELECT * FROM ";
    sql += table_name;

    set_ResultsSQLRequest(request2, sql);
    brpc::Controller controller2;
    stub.ResultsSQL(&controller2, &request2, &response2, nullptr);

    // if(temp_table_data.find(root_index) == temp_table_data.end())
    //     temp_table_data.emplace(std::piecewise_construct, std::forward_as_tuple(root_index), std::forward_as_tuple());
    // std::vector<std::string>* records = &(temp_table_data.find(root_index)->second);
    std::vector<std::string>* records;
    bool success2;
    std::string errors2;
    get_ResultsSQLResponse(response2, success2, errors2, records);

    auto record = records->begin();

    std::string create_sql = "CREATE TABLE ";
    //获取属性名
    create_sql += table_name +"(" ;
    create_sql += *record ;
    create_sql = create_sql+ ")";
    record = record+1;

    mysqlpp::Query query_create = mysql_connection->query(create_sql);
    if (query_create.exec()) {
        std::cout << " Create Table "<<table_name<< "REMORT success!" << std::endl;
    }
    else {
        std::cout <<" Create Table "<<table_name<< "REMORT FAIL! " << std::endl;
        resultObj.set_value(false);
        return;
    }

    std::string insert_sql = "INSERT INTO ";
    insert_sql += table_name + "VALUES";

    for(record; record != records->end(); ++record) {
        insert_sql += "(" + *record +")";
        if (record != records->end()-1){
            insert_sql = insert_sql+",";
        }
    }
    mysqlpp::Query query_insert = mysql_connection->query(insert_sql);
    if (query_insert.exec()) {
        std::cout << " INSERT Table "<<table_name<< "REMORT success!" << std::endl;
        resultObj.set_value(true);
        return;
    }
    else {
        std::cout <<" INSERT Table "<<table_name<< "REMORT FAIL! " << std::endl;
        resultObj.set_value(false);
        return;
    }
}



//递归遍历孩子节点就可以了
bool Executor::Data_Select_Thread(int root_index,std::promise<bool> &resultObj) {
    // QueryTree tree = get_tree(root_index);
    // QueryTree sub_tree = ;
    // QueryNode = child_node;
    // QueryNode root_node = tree.nodes[root_index];
    // std::vector<int> childs_id = root_node.sons_;
    QueryNode cur_node = qt->nodes[root_index];

    //判断是否是叶子节点
    //叶子结点只会是select
    if(cur_node.sons_.size()==0){
        //拼接语句，从mysql中取出结果然后存到临时表中，所有的临时表，都只有节点的名字
        //create table tree_1_node_5 select * from 
        if(cur_node.site_ == meta_data_->local_site_name){
            std::string create_sql = "create table ";
            std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
            std::string exec_sql = get_sql(&cur_node,-1);
            std::string final_sql = create_sql+table_name +" " + exec_sql;
            mysqlpp::Query query = mysql_connection->query(final_sql);
            try{
            if (query.exec()) {
                std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf success!" << std::endl;
                resultObj.set_value(true);
                return true; 
            }
            else {
                std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf FAIL! " << std::endl;
                resultObj.set_value(false);
                return false;
            }
            }
            catch(mysqlpp::Exception& er){
                std::cout << "Error: "<<" Create Table "<<table_name<< " leaf FAIL! "  << er.what() << std::endl;
            }
        }
        //基本不可能在叶子节点，还是在非本站点执行
        else{
            std::promise<bool> tem_result;
            std::future<bool> tem_bool;
            tem_bool = tem_result.get_future();
            Data_Select_Thread_Remote(cur_node.node_id,cur_node.site_,tem_result);
            bool success = tem_bool.get();

            if(success){
                std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf success!" << std::endl;
                resultObj.set_value(true);
                return true;
            }
            else {
                std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf FAIL! " << std::endl;
                resultObj.set_value(false);
                return false;
            }
        }
    }
    
    //非叶子结点
    else{
        std::promise<bool> resultObjs[MAXTHREAD]; 
        std::thread select_threads[MAXTHREAD];
        std::future<bool> select_thread_bool[MAXTHREAD];
        int q = 0; // 计数
        for(q=0;q<cur_nodes.sons_.size();q++){
            int cur_son_id = cur_nodes.sons_[q];
            printf("child %d on processing ...\n", cur_nodes.sons_[q]);
            select_thread_bool[q] = resultObjs[q].get_future();
            std::string cur_son_site = qt->nodes[cur_son_id].site_
            if(cur_son_site == meta_data_->local_site_name){
                // res_sub_tree = Data_Select_Execute(sub_tree);
                select_threads[q] = std::thread(Data_Select_Thread,cur_son_id, std::ref(resultObjs[q]));
            }
            else{
                // res_sub_tree = RPC_Data_Select_Execute(sub_tree, site);
                // res_sub_tree = Data_Select_Execute(sub_tree); // 本地调试用
                select_threads[q] = std::thread(Data_Select_Thread_Remote, cur_son_id, cur_son_site, std::ref(resultObjs[q]));
                // select_threads[q] = std::thread(Data_Select_Execute_Thread, sub_tree, std::ref(resultObjs[q])); // 本地调试用
            }
        }

        for(q=0;q<cur_nodes.sons_.size();q++){
            bool success = select_thread_bool[q].get();
            if (success){
                resultObj.set_value(true);
                std::cout  <<"Executing son "<< std::to_string(q) << "Success" << std::endl;
            }
            else{
                resultObj.set_value(false);
                std::cout  <<"Executing son "<< std::to_string(q) << "FAIL!" << std::endl;
                // return;
            }
        }
        //等待所有线程完成
        for(q=0;q<cur_nodes.sons_.size(); q++){
            select_threads[q].join();
        }

        //根据根节点的sql语句整合
        std::string create_sql = "create table ";
        std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
        std::string exec_sql = get_sql(&cur_node,qt->tree_id);
        std::string final_sql = create_sql+table_name +" " + exec_sql;
        mysqlpp::Query query = mysql_connection->query(final_sql);
        if (query.exec()) {
            std::cout <<" Create Table"<<table_name<< " Success!" << std::endl;
            resultObj.set_value(true);
        }
        else {
            std::cout  <<"Create Table "<<table_name<< " FAIL! " << std::endl;
            resultObj.set_value(false);
            return false;
        }

        //删除所有孩子节点的临时表
        std::string delete_sql = "DROP TABLE ";
        for(q=0;q<cur_nodes.sons_.size(); q++){
            int cur_son_id = cur_nodes.sons_[q];
            std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_son_id)+"_";
            std::string delete_sql_son = delete_sql+table_name;
            mysqlpp::Query query_d = mysql_connection->query(delete_sql_son);
            if (query_d.exec()) {
            std::cout  <<"DROP Table "<<table_name<< " Success!" << std::endl;
            resultObj.set_value(true);
            }
            else {
                std::cout << "DROP Table "<<table_name<< "  FAIL! " << std::endl;
                resultObj.set_value(false);
                return false;
            }
        }
    }
    return true;
}

bool Executor::Data_Select(int root_index){//只是去除了所有的resultObj
    QueryNode cur_node = qt->nodes[root_index];
    //判断是否是叶子节点
    //叶子结点只会是select
    if(cur_node.sons_.size()==0){
        //拼接语句，从mysql中取出结果然后存到临时表中，所有的临时表，都只有节点的名字
        //create table tree_1_node_5 select * from 
        if(cur_node.site_ == meta_data_->local_site_name){
            std::string create_sql = "create table ";
            std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
            std::string exec_sql = get_sql(&cur_node,-1);
            std::string final_sql = create_sql+table_name +" " + exec_sql;
            mysqlpp::Query query = mysql_connection->query(final_sql);
            if (query.exec()) {
                std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf success!" << std::endl;
                return true; 
            }
            else {
                std::cout << meta_data_->local_site_name <<" Create Table "<<table_name<< " leaf FAIL! " << std::endl;
                return false;
            }
        }
        //基本不可能在叶子节点，还是在非本站点执行
        else{
            std::promise<bool> tem_result;
            std::future<bool> tem_bool;
            tem_bool = tem_result.get_future();
            Data_Select_Thread_Remote(cur_node.node_id,cur_node.site_,tem_result);
            bool success = tem_bool.get();

            if(success){
                std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf success!" << std::endl;
                return true;
            }
            else {
                std::cout << meta_data_->local_site_name <<" on "<<table_name<< " leaf FAIL! " << std::endl;
                return false;
            }
        }
    }
    
    //非叶子结点
    else{
        std::promise<bool> resultObjs[MAXTHREAD]; 
        std::thread select_threads[MAXTHREAD];
        std::future<bool> select_thread_bool[MAXTHREAD];
        int q = 0; // 计数
        for(q=0;q<cur_nodes.sons_.size();q++){
            int cur_son_id = cur_nodes.sons_[q];
            printf("child %d on processing ...\n", cur_nodes.sons_[q]);
            select_thread_bool[q] = resultObjs[q].get_future();
            std::string cur_son_site = qt->nodes[cur_son_id].site_
            if(cur_son_site == meta_data_->local_site_name){
                // res_sub_tree = Data_Select_Execute(sub_tree);
                select_threads[q] = std::thread(Data_Select_Thread,cur_son_id, std::ref(resultObjs[q]));
            }
            else{
                // res_sub_tree = RPC_Data_Select_Execute(sub_tree, site);
                // res_sub_tree = Data_Select_Execute(sub_tree); // 本地调试用
                select_threads[q] = std::thread(Data_Select_Thread_Remote, cur_son_id, cur_son_site, std::ref(resultObjs[q]));
                // select_threads[q] = std::thread(Data_Select_Execute_Thread, sub_tree, std::ref(resultObjs[q])); // 本地调试用
            }
        }

        for(q=0;q<cur_nodes.sons_.size();q++){
            bool success = select_thread_bool[q].get();
            if (success){
                std::cout  <<"Executing son "<< std::to_string(q) << "Success" << std::endl;
            }
            else{
                std::cout  <<"Executing son "<< std::to_string(q) << "FAIL!" << std::endl;
                // return;
            }
        }
        //等待所有线程完成
        for(q=0;q<cur_nodes.sons_.size(); q++){
            select_threads[q].join();
        }

        //根据根节点的sql语句整合
        std::string create_sql = "create table ";
        std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_node.node_id)+"_";
        std::string exec_sql = get_sql(&cur_node,qt->tree_id);
        std::string final_sql = create_sql+table_name +" " + exec_sql;
        mysqlpp::Query query = mysql_connection->query(final_sql);
        if (query.exec()) {
            std::cout <<" Create Table"<<table_name<< " Success!" << std::endl;
        }
        else {
            std::cout  <<"Create Table "<<table_name<< " FAIL! " << std::endl;
            return false;
        }

        //删除所有孩子节点的临时表
        std::string delete_sql = "DROP TABLE ";
        for(q=0;q<cur_nodes.sons_.size(); q++){
            int cur_son_id = cur_nodes.sons_[q];
            std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(cur_son_id)+"_";
            std::string delete_sql_son = delete_sql+table_name;
            mysqlpp::Query query_d = mysql_connection->query(delete_sql_son);
            if (query_d.exec()) {
            std::cout  <<"DROP Table "<<table_name<< " Success!" << std::endl;
            }
            else {
                std::cout << "DROP Table "<<table_name<< "  FAIL! " << std::endl;
                return false;
            }
        }
    }
    return true;
}



std::vector<std::string> Executor::Data_Select_Single(std::vector<std::string> sql_vec, std::vector<std::string> site_vec){
    std::vector<std::string> results[site_vec.size()];
    std::thread load_threads[site_vec.size()];
    int i;
    for(i = 0; i < site_vec.size(); i++){
        /* 开启一个分片并在对应site存储的线程，通过传promise类给线程，让线程把结果给存到results里面，实现结果返回 */
        load_threads[i] = std::thread(Data_Select_Single_Thread, sql_vec[i],site_vec[i], &results[i]);
    }

    //等所有线程执行完后，才继续
    for(i = 0; i < site_vec.size(); i++){
        load_threads[i].join();
    }

    std::sort(results[0].begin(), results[0].end());
    std::sort(results[1].begin(), results[1].end());

    std::vector<std::string> v_intersection1;

    std::set_intersection(results[0].begin(), results[0].end(),
        results[1].begin(), results[1].end(),
        std::back_inserter(v_intersection1));

    if(site_vec.size()>=3){
        std::sort(results[2].begin(), results[2].end());
        std::sort(v_intersection1.begin(),v_intersection1.end());
        std::vector<std::string> v_intersection2;
        std::set_intersection(results[2].begin(), results[2].end(),
            v_intersection1.begin(), v_intersection1.end(),
            std::back_inserter(v_intersection2));
    }
    else{
        return v_intersection1;
    }

    if(site_vec.size()>=4){
        std::sort(results[3].begin(), results[3].end());
        std::sort(v_intersection2.begin(),v_intersection2.end());
        std::vector<std::string> v_intersection3;
        std::set_intersection(results[3].begin(), results[3].end(),
            v_intersection2.begin(), v_intersection2.end(),
            std::back_inserter(v_intersection3));
        return v_intersection3;
    }
    else{
        return v_intersection2;
    }

}

void Executor::Data_Select_Single_Thread(std::string sql_, std::string site_,std::vector<std::string>* results){
    //当前站点

    try{
    if (site_ == meta_data_->local_site_name) {
        mysqlpp::Query query1 = mysql_connection->query(sql_);
        //只会取出来一个属性
        if (mysqlpp::StoreQueryResult res = query1.store())
            {
                mysqlpp::StoreQueryResult::const_iterator it;
                mysqlpp::FieldTypes::value_type ft = res.field_type(0);
                for (it = res.begin(); it != res.end(); ++it)
                {
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
    } 
    else {
        std::vector<std::string>* records;
        if (request_remote_execution_result(sql_, site_,records)) {
            auto record = records.begin()+1;
            for(record; record != records.end(); ++record) {
                results->push_back(*record);
            }
        }
            std::cout << "Data_Select_Single REMOTE" <<site_ << " success!" << std::endl;
        else {
            std::cout << "Data_Select_Single REMOTE" <<site_ << " FAIL!" << std::endl;
        }
        } 
    }
    
    catch(const mysqlpp::Exception& er){
                std::cout<<"Data_Select_Single 执行错误"<<er.what()<<std::endl;
            }
    return ;
}

//Select操作结束后，再从最开始的根节点中，打印取出来的树
void Executor::Data_Select_Print(int root_index){
    std::string table_name = "tree_"+ std::to_string(qt->tree_id)+ "node_"+std::to_string(root_index)+"_";
    std::string sel_sql = "SELECT * FROM" + table_name;
    mysqlpp::Query query1 = mysql_connection->query(sel_sql);
    if (mysqlpp::StoreQueryResult res = query1.store())
        {
            std::cout << "We have:" << std::endl;
            mysqlpp::StoreQueryResult::const_iterator it;
            for (it = res.begin(); it != res.end(); ++it)
            {
                mysqlpp::Row row = *it;
                std::cout<<row.size();
                for (int jk=0;jk<row.size();jk++){
                    std::cout << '\t' << row[jk] ;
                }
                std::cout<<std::endl;
            }
        }
    else
        {
            std::cout << "Failed to get item list: " << query1.error() << std::endl;
            return ;
        }
    
    //删除这个表
    std::string del_sql = "DROP TABLE" + table_name;
    mysqlpp::Query query_del = mysql_connection->query(del_sql);
    if (query_del.exec()) {
            std::cout  <<"DROP Table "<<table_name<< " Success!" << std::endl;
            }
            else {
                std::cout << "DROP Table "<<table_name<< "  FAIL! " << std::endl;
                return false;
            }
    return true;
}




bool Executor:: request_remote_execution_result(std::string sql_,std::string site_,std::vector<std::string>* records){
    std::string aim_site = site_;
    std::string ip = meta_data_->site_map[aim_site].first;
    std::string port = meta_data_->site_map[aim_site].second;
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


void Executor::Data_Insert_Delete_Thread(std::string sql_,std::string site_,bool* result) {
    // if (sql_.size()==0){//每个站点都有sql_，空SQL代表当前站点不需要执行。
    //     *result = true;
    //     return;
    // }
    //保证每条语句不为空
    if (site_ == meta_data_->local_site_name) {//判断是不是当前站点
        mysqlpp::Query query2 = mysql_connection->query(sql_);
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
        //这里可以一直是-1，因为本身不会往temp_table_data里面放
        std::vector<std::string>* records;
        if (request_remote_execution_result(sql_, site_,records)) {
            std::cout << site_ << " success!" << std::endl;
        } else {
            *result = false;
            return;
        }
    }
}

bool Executor:: Data_Insert_Delete(std::vector<std::string> sql_vec, std::vector<std::string> site_vec){ //给每个站点都开一个线程，
    time_t start_time = time(NULL);
    bool results[site_vec.size()];
    std::thread load_threads[site_vec.size()];
    int i;
    for(i = 0; i < site_vec.size(); i++){
        /* 开启一个分片并在对应site存储的线程，通过传promise类给线程，让线程把结果给存到results里面，实现结果返回 */
        load_threads[i] = std::thread(Data_Insert_Delete_Thread, sql_vec[i],site_vec[i], &results[i]);
    }

    //等所有线程执行完后，才继续
    for(i = 0; i < site_vec.size(); i++){
        load_threads[i].join();
    }

    bool success= true;
    for(i = 0; i < site_vec.size(); i++) {
        if (!results[i]) {
            success = false;
        }
    }
    time_t end_time = time(NULL);
    double time_spend = difftime(end_time, start_time);
    /* 构造输出语句 */
    std::string time_output = std::to_string(time_spend);
    time_output.append(" seconds used.\n");
    std::string output_sentence = "";
    if (success){time_output.append("success");}
    else{time_output.append("failure");}
    output_sentence.append(time_output);
    std::cout<<output_sentence<<std::endl;

    return success;
}
