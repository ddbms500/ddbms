#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <iostream>
#include <string.h>
#include "ddbms.pb.h" //该文件会在make后自动生成

#include <unordered_map>

using namespace std;

DEFINE_string(protocol,"baidu_std","Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type,"","Connection type,Available values:single,pooled,short");
DEFINE_string(server,"0.0.0.0:8000","IP Address of server");
DEFINE_string(load_balancer,"","The algorithm for load balancing");
DEFINE_int32(timeout_ms,100,"RPC timeout in milliseconds");
DEFINE_int32(max_retry,3,"Max retries(not including the first RPC)");
DEFINE_int32(interval_ms,1000,"Milliseconds between consecutive requests");


//线程安全的，但client连接两个server(A->B和A->C需要两个channel)
bool initial_channel(brpc::Channel&channel,string server_address){//server_address格式如"0.0.0.0:8000"
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if(channel.Init(server_address.c_str(),FLAGS_load_balancer.c_str(),&options)!=0){
        LOG(ERROR)<<"Fail to initialize channel";
        return false;
    }
    return true;

}

//需要修改，传入表
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
void set_QueryTempTableRequest(whiteBear::QueryTempTableRequest&request,string table_name){
    request.set_table_name(table_name);
}
void set_DeleteTempTableRequest(whiteBear::DeleteTempTableRequest&request,string table_name){
    request.set_table_name(table_name);
}
void set_NonResultsSQLRequest(whiteBear::NonResultsSQLRequest&request,string sql){
    request.set_sql(sql);
}
void set_ResultsSQLRequest(whiteBear::ResultsSQLRequest&request,string sql){
    request.set_sql(sql);
}

void get_LoadTableResponse(whiteBear::LoadTableResponse&response,bool&success,string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

//需要修改，传入空容器，在函数内将查询到的表填入容器
void get_QueryTempTableResponse(whiteBear::QueryTempTableResponse&response,bool&success,string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

void get_DeleteTempTableResponse(whiteBear::DeleteTempTableResponse&response,bool&success,string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}

void get_NonResultsSQLResponse(whiteBear::NonResultsSQLResponse&response,bool&success,string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}
//需要修改，传入空容器，在函数内部填充容器
void get_ResultsSQLResponse(whiteBear::ResultsSQLResponse&response,bool&success,string&errors){
    success=response.success();
    if(!success){
        errors=response.errors();
    }
}
int main()
{
    brpc::Channel channel;
    initial_channel(channel,"0.0.0.0:8000");
    whiteBear::DDBService_Stub stub(&channel);
    for(int i=0;i<5;i++){
        whiteBear::LoadTableRequest request;
        whiteBear::LoadTableResponse response;
        set_LoadTableRequest(request);
        brpc::Controller cntl;
        stub.LoadTable(&cntl,&request,&response,NULL);
        if(cntl.Failed()){
            LOG(WARNING)<<"第"<<i+1<<"次通信失败"<<endl;
            continue;
        }
        bool success;
        string errors;
        get_LoadTableResponse(response,success,errors);
        if(!success){
            LOG(WARNING)<<"server返回错误信息"<<errors<<endl;
        }else{
            LOG(INFO)<<"LoadTable返回成功"<<endl;
        }
    }
    return 0;
}

