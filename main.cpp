//
// Created by sxy on 22-10-20.
//
#include <iostream>
#include "hsql/SQLParser.h"
#include "Defs/Query.h"
#include <boost/regex.hpp>
#include "MetaData/MetaData.h"
#include "Parser/Parser.h"
#include "Network/Executor.h"
#include "Utils/Exceptions.h"

MetaData* meta_data = new MetaData();
Optimizer* optimizer = new Optimizer();
Parser* parser = new Parser(optimizer, meta_data);
Executor* executor = new Executor(meta_data);

QueryType get_query_type(std::string str) {
//    boost::regex regex_exit(reg_exit);
//    boost::regex regex_define_site(reg_define_site);
//    boost::regex regex_create_db(reg_create_db);
//    boost::regex regex_drop_db(reg_drop_db);
//    boost::regex regex_use_db(reg_use_db);
//    boost::regex regex_create_table(reg_create_table);
//    boost::regex regex_drop_table(reg_drop_table);
//    boost::regex regex_create_fragment(reg_create_fragment);
//    boost::regex regex_allocate(reg_allocate);
//    boost::regex regex_show_databases(reg_show_databases);
//    boost::regex regex_show_tables(reg_show_tables);
//    boost::regex regex_show_sites(reg_show_sites);
//    boost::regex regex_show_fragments(reg_show_fragments);
//    boost::regex regex_load_data(reg_load_data);
//    boost::regex regex_insert(reg_insert);
//    boost::regex regex_delete(reg_delete);
//    boost::regex regex_select(reg_select);
////    boost::regex regex_test(reg_test);
//
//    if(boost::regex_match(str, regex_exit)) return QueryType::EXIT;
//    if(boost::regex_match(str, regex_define_site)) return QueryType::DEFINE_SITE;
//    if(boost::regex_match(str, regex_create_db)) return QueryType::CREATE_DB;
//    if(boost::regex_match(str, regex_drop_db)) return QueryType::DROP_DB;
//    if(boost::regex_match(str, regex_use_db)) return QueryType::USE_DB;
//    if(boost::regex_match(str, regex_create_table)) return QueryType::CREATE_TABLE;
//    if(boost::regex_match(str, regex_drop_table)) return QueryType::DROP_TABLE;
//    if(boost::regex_match(str, regex_create_fragment)) return QueryType::CREATE_FRAGMENT;
//    if(boost::regex_match(str, regex_allocate)) return QueryType::ALLOCATE;
//    if(boost::regex_match(str, regex_show_databases)) return QueryType::SHOW_DATABASES;
//    if(boost::regex_match(str, regex_show_tables)) return QueryType::SHOW_TABLES;
//    if(boost::regex_match(str, regex_show_sites)) return QueryType::SHOW_SITES;
//    if(boost::regex_match(str, regex_show_fragments)) return QueryType::SHOW_FRAGMENTS;
//    if(boost::regex_match(str, regex_load_data)) return QueryType::LOAD_DATA;
//    if(boost::regex_match(str, regex_insert)) return QueryType::INSERT;
//    if(boost::regex_match(str, regex_delete)) return QueryType::DELETE;
//    if(boost::regex_match(str, regex_select)) return QueryType::SELECT;
////    if(boost::regex_match(str, regex_test)) return QueryType::TEST_TYPE;
//    return QueryType::UNKNOWN_TYPE;
    return QueryType::SELECT;
}

QueryType parse_command(std::string command) {
    QueryType query_type = get_query_type(command);
    std::cout << query_type_str[static_cast<int>(query_type)] << std::endl;
    switch(query_type) {
        case QueryType::EXIT: {
            // TODO: 断掉连接
            return QueryType::EXIT;
        } break;
        case QueryType::DEFINE_SITE: {
//            std::string site_name = "";
//            std::string site_ip = "";
//            std::string site_port = "";
//            boost::regex regex(reg_define_site);
//            site_name = boost::regex_replace(command, regex, "$2");
//            site_ip = boost::regex_replace(command, regex, "$4");
//            site_port = boost::regex_replace(command, regex, "$6");
//
//            meta_data->add_site(site_name, site_ip, site_port);
        } break;
        case QueryType::CREATE_DB: {
//            std::string db_name = "";
//            boost::regex regex(reg_create_db);
//            db_name = boost::regex_replace(command, regex, "$2");
//
//            meta_data->add_db(db_name);
        } break;
        case QueryType::DROP_DB: {
//            std::string db_name = "";
//            boost::regex regex(reg_drop_db);
//            db_name = boost::regex_replace(command, regex, "$2");
//
//            meta_data->drop_db(db_name);
        } break;
        case QueryType::USE_DB: {
//            std::string db_name = "";
//            boost::regex regex(reg_use_db);
//            db_name = boost::regex_replace(command, regex, "$2");
//
//            meta_data->use_db(db_name);
        } break;
        case QueryType::CREATE_TABLE: {
//            std::string table_name = "";
//            hsql::SQLParserResult result;
//            hsql::SQLParser::parse(command, &result);
//
//            if(!result.isValid()) {
//                // TODO: invalid command exception
//                std::cout << "command invalid" << std::endl;
//            }
//
//            if(result.isValid() && result.size() > 0) {
//                const hsql::SQLStatement* statement = result.getStatement(0);
//                if(statement->isType(hsql::kStmtCreate)) {
//
//                }
//                else {
//                    // TODO: invalid command
//                }
//            }
        } break;
        case QueryType::DROP_TABLE: {

        } break;
        case QueryType::CREATE_FRAGMENT: {

        } break;
        case QueryType::ALLOCATE: {

        } break;
        case QueryType::SHOW_DATABASES: {
            meta_data->show_databases();
        } break;
        case QueryType::SHOW_TABLES: {
            meta_data->show_tables();
        } break;
        case QueryType::SHOW_SITES: {
            meta_data->show_sites();
        } break;
        case QueryType::SHOW_FRAGMENTS: {
            meta_data->show_fragments();
        } break;
        case QueryType::LOAD_DATA: {

        } break;
        case QueryType::INSERT: {
            // TODO:
            // 和delete一样
        } break;
        case QueryType::DELETE: {
            // TODO:
            // parser: parser阶段需要解析要在哪些站点执行哪些操作
            // network: 然后调用network的传输接口告诉每个站点需要执行的delete信息
            // executor: executor需要在本地执行对应的delete语句
        } break;
        case QueryType::SELECT: {
            // TODO:
            // parser模块生成一个查询计划,把查询计划写入到etcd中(为什么要写入etcd呢,直接发送给每个站点不可以么?)
            // 然后每个站点去执行查询计划,采用pull模型? (executor)
            // 临时表的元信息也要存在etcd中吧(临时表的元信息其实就是query_tree每个节点的元信息吧,etcd存的key_value中的key如何来确定呢)
            // 当前函数需要去request_temp_table(请求临时表也就是每个节点(执行算子)生成的数据)然后输出
            // request_temp_table传递什么参数取决于写入etcd的query_tree如何去表示key
            // executor不断去query子节点的数据
            hsql::SQLParserResult result;
            hsql::SQLParser::parse(command, &result);

            std::cout << "finish parse" << std::endl;

            if(!result.isValid()) {
//                std::cout << "command invalid\n";
                throw new InvalidCommandException("invalid select command \"" + command + "\";");
            }

            parser->query_tree_generation(&result);
            // query_tree用完之后要clear掉
//            parser->print_query_tree(parser->get_query_tree_root());
            executor->exec_query_tree(-1);
            executor->print_records(-1);
        } break;
        default:
        break;
    }
    return QueryType::EXIT;
}

int main(int argc, char **argv) {
    std::string command = "select Publisher.name, Publisher.nation, Book.title, Book.copies from Book, Publisher "
                          "where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies>1000;";
    meta_data->init();
//    executor->exec_sql_local("");
    while (true) {
        std::cout << "ddbms> ";
        getline(std::cin, command);
        if(parse_command(command) == QueryType::EXIT) break;
    }
//    try{
//        parse_command(command);
//    } catch (InvalidCommandException &e) {
//        std::cout << e.what() << std::endl;
//    }
    return 0;
}