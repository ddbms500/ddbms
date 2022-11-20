//
// Created by sxy on 11/20/22.
//
#include "NetworkRequest.h"

DEFINE_string(protocol,"baidu_std","Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type,"","Connection type,Available values:single,pooled,short");
DEFINE_string(load_balancer,"","The algorithm for load balancing");
DEFINE_int32(timeout_ms,100,"RPC timeout in milliseconds");
DEFINE_int32(max_retry,3,"Max retries(not including the first RPC)");
DEFINE_int32(interval_ms,1000,"Milliseconds between consecutive requests");

bool initial_channel(brpc::Channel& channel, std::string server_addr) {
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if(channel.Init(server_addr.c_str(),FLAGS_load_balancer.c_str(),&options)!=0){
        LOG(ERROR)<<"Fail to initialize channel";
        return false;
    }
    return true;
}

void set_QueryTreeRequest(whiteBear::QueryTreeRequest&request,const QueryTree& qt){
    request.set_n(qt.n);
    request.set_root_index(qt.root_index);
    for(int i=0;i<qt.n;i++){
        const QueryNode* node = &(qt.nodes[i]);
        whiteBear::QueryNode*query_node=request.add_nodes();//新增加一个QueryNode
        query_node->set_node_type_(whiteBear::QueryNode_NodeType(static_cast<int>(node->node_type_)));
        query_node->set_site_(node->site_);
        query_node->set_table_name_(node->table_name_);
        for(int j=0;j<node->attr_name_.size();j++){
            query_node->add_attr_name_(node->attr_name_[j]);
        }
        for(int j=0;j<node->select_predicates_.size();j++){
            whiteBear::Predicate*predicate=query_node->add_select_predicates_();
            predicate->set_attribute_name_(node->select_predicates_[j].attribute_name_);
            predicate->set_table_name_(node->select_predicates_[j].table_name_);
            predicate->set_operation_type_(whiteBear::Predicate_OperationType(static_cast<int>(node->select_predicates_[j].operation_type_)));
            predicate->set_left_operation_type_(whiteBear::Predicate_OperationType(static_cast<int>(node->select_predicates_[j].left_operation_type_)));
            predicate->set_right_value_(node->select_predicates_[j].right_value_);
            predicate->set_left_value_(node->select_predicates_[j].left_value_);
            predicate->set_right_attribute_name_(node->select_predicates_[j].right_attribute_name_);
            predicate->set_right_table_name_(node->select_predicates_[j].right_table_name_);
        }

        whiteBear::Predicate*predicate=query_node->add_join_predicate_();
        predicate->set_attribute_name_(node->join_predicate_.attribute_name_);
        predicate->set_table_name_(node->join_predicate_.table_name_);
        predicate->set_operation_type_(whiteBear::Predicate_OperationType(static_cast<int>(node->join_predicate_.operation_type_)));
        predicate->set_left_operation_type_(whiteBear::Predicate_OperationType(node->join_predicate_.left_operation_type_));
        predicate->set_right_value_(node->join_predicate_.right_value_);
        predicate->set_left_value_(node->join_predicate_.left_value_);
        predicate->set_right_attribute_name_(node->join_predicate_.right_attribute_name_);
        predicate->set_right_table_name_(node->join_predicate_.right_table_name_);

        query_node->set_parent_(node->parent_);
        for(int j=0;j<node->sons_.size();j++){
            query_node->add_sons_(node->sons_[j]);
        }
        query_node->set_is_parent_(node->is_parent_);
        query_node->set_is_leaf_(node->is_leaf_);
    }
}

void set_LoadTableRequest(whiteBear::LoadTableRequest&request){
    request.set_table_name("testforTable");
    request.set_meta_nums(2);

    whiteBear::Column*column1 = request.add_columns();//新增一个列
    column1->set_attr_meta("col1_INT");//设置列名
    column1->set_attr_type(whiteBear::Column::INT);//设置为int型
    for(int i=0;i<7;i++){//添加非类属性成员，使用add_*方法
        column1->add_attr_values_int(i+1);
    }
    whiteBear::Column*column2 = request.add_columns();//新增一个列
    column2->set_attr_meta("col2_STRING");
    column2->set_attr_type(whiteBear::Column::STRING);//设置为字符串型
    for(int i=0;i<7;i++){
        if((i+1)%2==0){
            column2->add_attr_values_string("是偶数");
        }else{
            column2->add_attr_values_string("是奇数");
        }
    }

}
void set_QueryTempTableRequest(whiteBear::QueryTempTableRequest&request, std::string table_name){
    request.set_table_name(table_name);
}
void set_DeleteTempTableRequest(whiteBear::DeleteTempTableRequest&request,std::string table_name){
    request.set_table_name(table_name);
}
void set_NonResultsSQLRequest(whiteBear::NonResultsSQLRequest&request,std::string sql){
    request.set_sql(sql);
}
void set_ResultsSQLRequest(whiteBear::ResultsSQLRequest&request, std::string sql){
    request.set_sql(sql);
}


void QueryTreeResponse(whiteBear::LoadTableResponse&response,bool&success,std::string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

void get_LoadTableResponse(whiteBear::LoadTableResponse&response,bool&success,std::string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

//需要修改，传入空容器，在函数内将查询到的表填入容器
void get_QueryTempTableResponse(whiteBear::QueryTempTableResponse&response,bool&success,std::string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

void get_DeleteTempTableResponse(whiteBear::DeleteTempTableResponse&response,bool&success,std::string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

void get_NonResultsSQLResponse(whiteBear::NonResultsSQLResponse&response,bool&success,std::string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}
//需要修改，传入空容器，在函数内部填充容器
void get_ResultsSQLResponse(whiteBear::ResultsSQLResponse&response,bool&success,std::string&errors,std::vector<std::string>*results){
    success=response.success();
    if(!success){
        errors=response.errors();
    }

    int row_nums=0;//获取列的数量
    switch(response.columns(0).attr_type()){
        case whiteBear::Column::BOOL:
            row_nums=response.columns(0).attr_values_bool_size();
            break;
        case whiteBear::Column::INT:
            row_nums=response.columns(0).attr_values_int_size();
            break;
        case whiteBear::Column::STRING:
            row_nums=response.columns(0).attr_values_string_size();
            break;
        case whiteBear::Column::FLOAT:
            row_nums=response.columns(0).attr_values_float_size();
            break;
        case whiteBear::Column::DOUBLE:
            row_nums=response.columns(0).attr_values_double_size();
            break;
    }
    for(int i=0;i<row_nums;i++){
        std::string str = "";
        for(int j=0;j<response.columns_size();j++){
            switch(response.columns(j).attr_type()){
                case whiteBear::Column::BOOL:
                    if(j==0){
                        str+=std::to_string(response.columns(j).attr_values_bool(i));
                    }else{
                        str+=("_"+std::to_string(response.columns(j).attr_values_bool(i)));
                    }
                    break;
                case whiteBear::Column::INT:
                    if(j==0){
                        str+=std::to_string(response.columns(j).attr_values_int(i));
                    }else{
                        str+=("_"+std::to_string(response.columns(j).attr_values_int(i)));
                    }
                    break;
                case whiteBear::Column::STRING:
                    if(j==0){
                        str+=response.columns(j).attr_values_string(i);
                    }else{
                        str+=("_"+response.columns(j).attr_values_string(i));
                    }
                    break;
                case whiteBear::Column::FLOAT:
                    if(j==0){
                        str+=std::to_string(response.columns(j).attr_values_float(i));
                    }else{
                        str+=("_"+std::to_string(response.columns(j).attr_values_float(i)));
                    }
                    break;
                case whiteBear::Column::DOUBLE:
                    if(j==0){
                        str+=std::to_string(response.columns(j).attr_values_double(i));
                    }else{
                        str+=("_"+std::to_string(response.columns(j).attr_values_double(i)));
                    }
                    break;
            }
        }//一行的一个属性
        results->push_back(str);
    }//一行结束
}