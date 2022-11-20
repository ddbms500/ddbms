//
// Created by sxy on 11/20/22.
//
#ifndef DDBMS_NETWORKREQUEST_H
#define DDBMS_NETWORKREQUEST_H

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <iostream>
#include <string.h>
#include "ddbms.pb.h" //该文件会在make后自动生成
#include "Defs/QueryTree.h"

#include <unordered_map>

bool initial_channel(brpc::Channel& channel, std::string server_addr);
void set_QueryTreeRequest(whiteBear::QueryTreeRequest&request,const QueryTree& qt);
void set_LoadTableRequest(whiteBear::LoadTableRequest&request);
void set_QueryTempTableRequest(whiteBear::QueryTempTableRequest&request, std::string table_name);
void set_DeleteTempTableRequest(whiteBear::DeleteTempTableRequest&request,std::string table_name);
void set_NonResultsSQLRequest(whiteBear::NonResultsSQLRequest&request,std::string sql);
void set_ResultsSQLRequest(whiteBear::ResultsSQLRequest&request, std::string sql);
void QueryTreeResponse(whiteBear::LoadTableResponse&response,bool&success, std::string&errors);
void get_LoadTableResponse(whiteBear::LoadTableResponse&response,bool&success,std::string&errors);
void get_QueryTempTableResponse(whiteBear::QueryTempTableResponse&response,bool&success,std::string&errors);
void get_DeleteTempTableResponse(whiteBear::DeleteTempTableResponse&response,bool&success,std::string&errors);
void get_NonResultsSQLResponse(whiteBear::NonResultsSQLResponse&response,bool&success,std::string&errors);
void get_ResultsSQLResponse(whiteBear::ResultsSQLResponse&response,bool&success,std::string&errors,std::vector<std::string>*results);

#endif //DDBMS_NETWORKREQUEST_H
