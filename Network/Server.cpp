//
// Created by sxy on 11/20/22.
//
#include <iostream>
#include <stdio.h>
/*--------------BRPC需要------------------*/
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include "Network/ddbms.pb.h"
#include "Defs/QueryTree.h"
#include "MetaData/MetaData.h"
#include <mysql++/mysql++.h>
#include <string.h>
#include "Network/Executor.cpp"

DEFINE_bool(echo_attachment, true, "Echo attachment as well");
DEFINE_int32(port, 8800, "TCP Port of this server");
DEFINE_string(listen_addr, "", "Server listen address, may be IPV4/IPV6/UDS."
                               " If this is set, the flag port will be ignored");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
                                 "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
                              "(waiting for client to close connection before server stops)");

MetaData* meta_data_ = new MetaData();

namespace whiteBear{
/*bool connect2DB(const whiteBear::ConnectDB_Request* request,whiteBear::ConnectDB_Response* response){
  try{
  mysqlpp::Connection conn(request->dbname().c_str(),"127.0.0.1",
  request->username().c_str(),request->password().c_str());
  }catch(const mysqlpp::Exception& er){
  response->set_whetherconnectdb(false);
  response->set_message(er.what());
  return false;
  }
  response->set_whetherconnectdb(true);
  response->set_message("Successfully connect to MySql\n");
  return true;
  }*/
    class DDBServiceImpl : public DDBService{
    public:
        DDBServiceImpl(){};
        virtual ~DDBServiceImpl(){};
        //下面这个函数现在是传树，后面变成传root_index直接去meta_data里面找就行
        virtual void QueryTreeService(google::protobuf::RpcController* cntl_base,
                               const QueryTreeRequest* request,
                               QueryTreeResponse* response,
                               google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);

            QueryTree*qt=new QueryTree();
            qt->n=request->n();
            qt->root_index=request->root_index();
            for(int i=0;i<request->n();i++){//填充QueryNode nodes[]
                ::QueryNode*qn=new ::QueryNode();
                qn->node_type_ = NodeType(static_cast<int>(request->nodes(i).node_type_()));
                qn->site_=request->nodes(i).site_();
                qn->table_name_=request->nodes(i).table_name_();
                for(int j=0;j<request->nodes(i).attr_name__size();j++){
                    qn->attr_name_.push_back(request->nodes(i).attr_name_(j));
                }
                for(int j=0;j<request->nodes(i).select_predicates__size();j++){
                    ::Predicate predicate;
                    predicate.attribute_name_=request->nodes(i).select_predicates_(j).attribute_name_();
                    predicate.table_name_=request->nodes(i).select_predicates_(j).table_name_();
                    predicate.operation_type_=::OperationType(request->nodes(i).select_predicates_(j).operation_type_());
                    predicate.left_operation_type_=::OperationType(request->nodes(i).select_predicates_(j).left_operation_type_());
                    predicate.right_value_=request->nodes(i).select_predicates_(j).right_value_();
                    predicate.left_value_=request->nodes(i).select_predicates_(j).left_value_();
                    predicate.right_attribute_name_=request->nodes(i).select_predicates_(j).right_attribute_name_();
                    predicate.right_table_name_=request->nodes(i).select_predicates_(j).right_table_name_();
                    qn->select_predicates_.push_back(predicate);
                }
                ::Predicate predicate;
                predicate.attribute_name_=request->nodes(i).join_predicate_(0).attribute_name_();
                predicate.table_name_=request->nodes(i).join_predicate_(0).table_name_();
                predicate.operation_type_=::OperationType(request->nodes(i).join_predicate_(0).operation_type_());
                predicate.left_operation_type_=::OperationType(request->nodes(i).join_predicate_(0).left_operation_type_());
                predicate.right_value_=request->nodes(i).join_predicate_(0).right_value_();
                predicate.left_value_=request->nodes(i).join_predicate_(0).left_value_();
                predicate.right_attribute_name_=request->nodes(i).join_predicate_(0).right_attribute_name_();
                predicate.right_table_name_=request->nodes(i).join_predicate_(0).right_table_name_();
                qn->join_predicate_=predicate;

                qn->parent_=request->nodes(i).parent_();

                for(int j=0;j<request->nodes(i).sons__size();j++){
                    qn->sons_.push_back(request->nodes(i).sons_(j));
                }
                qn->is_parent_=request->nodes(i).is_parent_();
                qn->is_leaf_=request->nodes(i).is_leaf_();
                qt->nodes[i]=*qn;
            }
        }//QueryTree服务结束

        //获得root_index对应的子树的结果
        virtual void RootIndexService(google::protobuf::RpcController* cntl_base,
                               const QueryTreeRequest* request,
                               QueryTreeResponse* response,
                               google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);
            //begin
                //执行接受到的树（节点编号，创建临时表）从上面解析出，root_index
                int root_index = request->root_index;
                try{
                    bool success = Data_Select(root_index);
                    response->set_success(success);
                }
                catch(const mysqlpp::Exception& er){
                std::cout<<"sql执行错误:"<<er.what()<<std::endl;
                response->set_success(false);
                response->set_errors(er.what());
            }
        }




        virtual void LoadTable(google::protobuf::RpcController* cntl_base,
                               const LoadTableRequest* request,
                               LoadTableResponse* response,
                               google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);

            //将接收到的表载入到mysql++的部分
            int row_nums=0;
            switch(request->columns(0).attr_type()){
                case whiteBear::Column::BOOL:
                    row_nums=request->columns(0).attr_values_bool_size();
                    break;
                case whiteBear::Column::INT:
                    row_nums=request->columns(0).attr_values_int_size();
                    break;
                case whiteBear::Column::STRING:
                    row_nums=request->columns(0).attr_values_string_size();
                    break;
                case whiteBear::Column::FLOAT:
                    row_nums=request->columns(0).attr_values_float_size();
                    break;
                case whiteBear::Column::DOUBLE:
                    row_nums=request->columns(0).attr_values_double_size();
                    break;
            }
            LOG(INFO)<<"接受到来自 "<<cntl->remote_side()<<" 的LoadTableRequest"<<std::endl;
            LOG(INFO)<<"表名："<<request->table_name()<<" ,列的数量："<<request->meta_nums()<<" ,行的数量："<<row_nums<<std::endl;
            for(int i=-1;i<row_nums;i++){
                if(i==-1){
                    for(int j=0;j<request->columns_size();j++){
                        LOG(INFO)<<request->columns(j).attr_meta()<<"   ";
                    }
                    LOG(INFO)<<std::endl;
                    continue;
                }
                for(int j=0;j<request->columns_size();j++){
                    switch(request->columns(j).attr_type()){
                        case whiteBear::Column::BOOL:
                            LOG(INFO)<<request->columns(j).attr_values_bool(i);
                            break;
                        case whiteBear::Column::INT:
                            LOG(INFO)<<request->columns(j).attr_values_int(i);
                            break;
                        case whiteBear::Column::STRING:
                            LOG(INFO)<<request->columns(j).attr_values_string(i);
                            break;
                        case whiteBear::Column::FLOAT:
                            LOG(INFO)<<request->columns(j).attr_values_float(i);
                            break;
                        case whiteBear::Column::DOUBLE:
                            LOG(INFO)<<request->columns(j).attr_values_double(i);
                            break;
                    }//属性分类处理结束
                }//输出一行的一列
                LOG(INFO)<<std::endl;
            }//输出所有的行
            response->set_success(true);
        }//LoadTable服务定义结束

        virtual void QueryTempTable(google::protobuf::RpcController* cntl_base,
                                    const QueryTempTableRequest* request,
                                    QueryTempTableResponse* response,
                                    google::protobuf::Closure* done){
            //BRPC server需要
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);

            //查询临时表并放入response的部分
            response->set_meta_nums(2);
            whiteBear::Column*column1 = response->add_columns();//新增一个列
            column1->set_attr_meta("col1_INT");//设置列名
            column1->set_attr_type(whiteBear::Column::INT);//设置为int型
            for(int i=0;i<7;i++){//添加非类属性成员，使用add_*方法
                column1->add_attr_values_int(i+1);
            }
            whiteBear::Column*column2 = response->add_columns();
            column2->set_attr_meta("col2_STRING");
            column2->set_attr_type(whiteBear::Column::STRING);//设置为字符串型
            for(int i=0;i<7;i++){
                if((i+1)%2==0){
                    column2->add_attr_values_string("是偶数");
                }else{
                    column2->add_attr_values_string("是奇数");
                }
            }
            response->set_success(true);
        }//QueryTempTable服务定义结束

        virtual void DeleteTempTable(google::protobuf::RpcController* cntl_base,
                                     const DeleteTempTableRequest* request,
                                     DeleteTempTableResponse* response,
                                     google::protobuf::Closure* done){
            //BRPC server需要
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);

            //删除临时表并设置response的部分
            response->set_success(true);
        }//DeleteTempTable服务结束

        virtual void NonResultsSQL(google::protobuf::RpcController* cntl_base,
                                   const NonResultsSQLRequest* request,
                                   NonResultsSQLResponse* response,
                                   google::protobuf::Closure* done){
            //BRPC server需要
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);

            //删除临时表并设置response的部分i
            std::string sql=request->sql();
            response->set_success(true);
        }//NonResultsSQL服务定义结束

//        virtual void ResultsSQL(google::protobuf::RpcController* cntl_base,
//                                const ResultsSQLRequest* request,
//                                ResultsSQLResponse* response,
//                                google::protobuf::Closure* done){
//            //BRPC server需要
//            brpc::ClosureGuard done_guard(done);
//            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);
//
//            std::string sql = "select Publisher.name from Publisher;";
//            //查询临时表并放入response的部分
//            response->set_table_name("TABLE_NAME");
//            response->set_meta_nums(2);
//            whiteBear::Column*column1 = response->add_columns();//新增一个列
//            column1->set_attr_meta("col1_INT");//设置列名
//            column1->set_attr_type(whiteBear::Column::INT);//设置为int型
//            for(int i=0;i<7;i++){//添加非类属性成员，使用add_*方法
//                column1->add_attr_values_int(i+1);
//            }
//            whiteBear::Column*column2 = response->add_columns();
//            column2->set_attr_meta("col2_STRING");
//            column2->set_attr_type(whiteBear::Column::STRING);//设置为字符串型
//            for(int i=0;i<7;i++){
//                if((i+1)%2==0){
//                    column2->add_attr_values_string("是偶数");
//                }else{
//                    column2->add_attr_values_string("是奇数");
//                }
//            }
//            response->set_success(true);
//        }

        //执行对应的sql语句，一列一列村在response中
        virtual void ResultsSQL(google::protobuf::RpcController* cntl_base,
                                const ResultsSQLRequest* request,
                                ResultsSQLResponse* response,
                                google::protobuf::Closure* done){
            //BRPC server需要
            brpc::ClosureGuard done_guard(done);
            brpc::Controller*cntl=static_cast<brpc::Controller*>(cntl_base);
            LOG(INFO)<<"received ResultsSQL request from"<<cntl->remote_side();
            try{
                mysqlpp::Connection conn("ddb", "127.0.0.1", "root", "123456");
                mysqlpp::Query query = conn.query();
                query << request->sql().c_str();
                mysqlpp::StoreQueryResult res = query.store();
                int c_nums=res.field_names()->size();//得到列的数量
                response->set_table_name("Temporary NULL");
                response->set_meta_nums(c_nums);
                for(int i=0;i<c_nums;i++){//得出各列
                    whiteBear::Column*column = response->add_columns();
                    column->set_attr_meta(res.field_name(i));//获得列名
                    mysqlpp::StoreQueryResult::const_iterator it;
                    for(it = res.begin();it!=res.end();++it){
                        mysqlpp::Row row = * it;
                        mysqlpp::FieldTypes::value_type ft = res.field_type(i);
                        if(strstr(ft.sql_name(),"BOOL")!=NULL){
                            column->set_attr_type(whiteBear::Column::BOOL);
                            column->add_attr_values_bool(row[i]);
                        }else if(strstr(ft.sql_name(),"INT")!=NULL){
                            column->set_attr_type(whiteBear::Column::INT);
                            column->add_attr_values_int(row[i]);
                        }else if(strstr(ft.sql_name(),"DOUBLE")!=NULL){
                            column->set_attr_type(whiteBear::Column::DOUBLE);
                            column->add_attr_values_double(row[i]);
                        }else if(strstr(ft.sql_name(),"FLOAT")!=NULL){
                            column->set_attr_type(whiteBear::Column::FLOAT);
                            column->add_attr_values_float(row[i]);
                        }else{
                            column->set_attr_type(whiteBear::Column::STRING);
                            column->add_attr_values_string(row[i]);
                        }
                    }
                }
                response->set_success(true);
            }catch(const mysqlpp::Exception& er){
                std::cout<<"sql执行错误:"<<er.what()<<std::endl;
                response->set_success(false);
                response->set_errors(er.what());
            }
        }

    };//DDBServiceImpl类结束
}//whiteBear命名空间结束

int main(int argc,char *argv[])
{
    brpc::Server server;
    whiteBear::DDBServiceImpl ddb_service_impl;
    if(server.AddService(&ddb_service_impl,brpc::SERVER_DOESNT_OWN_SERVICE)!=0){
        LOG(ERROR)<<"Fail to add service ConnectService"<<std::endl;
        return -1;
    }
    butil::EndPoint point;
    if(!FLAGS_listen_addr.empty()){
        LOG(ERROR)<<"Invalid listen address:"<<FLAGS_listen_addr;
        return -1;
    }else{
        point=butil::EndPoint(butil::IP_ANY,8800);
    }
    /*开始服务*/
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(point, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    //一直等到输入ctrl+c,等到后 Stop() 并 Join() 服务器
    server.RunUntilAskedToQuit();
    return 0;
}
