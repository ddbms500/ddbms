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
#include <fstream>
#include <ctime>

MetaData* meta_data = new MetaData();
Optimizer* optimizer = new Optimizer();
Parser* parser = new Parser(optimizer, meta_data);
Executor* executor = new Executor(meta_data);

QueryType get_query_type(std::string str) {
    boost::regex regex_exit(reg_exit);
    boost::regex regex_define_site(reg_define_site);
    boost::regex regex_create_db(reg_create_db);
    boost::regex regex_drop_db(reg_drop_db);
    boost::regex regex_use_db(reg_use_db);
    boost::regex regex_create_table(reg_create_table);
    boost::regex regex_drop_table(reg_drop_table);
    boost::regex regex_create_fragment(reg_create_fragment);
    boost::regex regex_allocate(reg_allocate);
    boost::regex regex_show_databases(reg_show_databases);
    boost::regex regex_show_tables(reg_show_tables);
    boost::regex regex_show_sites(reg_show_sites);
    boost::regex regex_show_fragments(reg_show_fragments);
    boost::regex regex_load_data(reg_load_data);
    boost::regex regex_insert(reg_insert);
    boost::regex regex_delete(reg_delete);
    boost::regex regex_select(reg_select);
//    boost::regex regex_test(reg_test);

    if(boost::regex_match(str, regex_exit)) return QueryType::EXIT;
    if(boost::regex_match(str, regex_define_site)) return QueryType::DEFINE_SITE;
    if(boost::regex_match(str, regex_create_db)) return QueryType::CREATE_DB;
    if(boost::regex_match(str, regex_drop_db)) return QueryType::DROP_DB;
    if(boost::regex_match(str, regex_use_db)) return QueryType::USE_DB;
    if(boost::regex_match(str, regex_create_table)) return QueryType::CREATE_TABLE;
    if(boost::regex_match(str, regex_drop_table)) return QueryType::DROP_TABLE;
    if(boost::regex_match(str, regex_create_fragment)) return QueryType::CREATE_FRAGMENT;
    if(boost::regex_match(str, regex_allocate)) return QueryType::ALLOCATE;
    if(boost::regex_match(str, regex_show_databases)) return QueryType::SHOW_DATABASES;
    if(boost::regex_match(str, regex_show_tables)) return QueryType::SHOW_TABLES;
    if(boost::regex_match(str, regex_show_sites)) return QueryType::SHOW_SITES;
    if(boost::regex_match(str, regex_show_fragments)) return QueryType::SHOW_FRAGMENTS;
    if(boost::regex_match(str, regex_load_data)) return QueryType::LOAD_DATA;
    if(boost::regex_match(str, regex_insert)) return QueryType::INSERT;
    if(boost::regex_match(str, regex_delete)) return QueryType::DELETE;
    if(boost::regex_match(str, regex_select)) return QueryType::SELECT;
//    if(boost::regex_match(str, regex_test)) return QueryType::TEST_TYPE;
//    return QueryType::UNKNOWN_TYPE;
    if(str.compare("exit") == 0) return QueryType::EXIT;
    return QueryType::SELECT;
}

void process_insert() {
    // std::cout << "begin process_insert" << std::endl;
    Table* table = parser->get_insert_stmt()->table_;
    InsertStmt* insert_stmt = parser->get_insert_stmt();
    std::vector<std::string>* record = &(insert_stmt->values_);

    // std::cout << "begin scan fragments" << static_cast<int>(table->table_fragment_type_)<<std::endl;

    // ??????????????????????????????????????????????????????????????????????????????????????????????????????
    if(table->table_fragment_type_ == FragmentType::HORIZONTAL) {
        // ??????fragment?????????predicate??????????????????table?????????????????????????????????????????????????????????
        std::vector<Fragment>* fragments = &(table->fragments_);
        std::cout << fragments->size() << std::endl;
        int insert_fragment_index = -1;
        for(int i = 0; i < fragments->size(); ++i) {
            if(fragments->at(i).is_in_fragment(record)) {
                insert_fragment_index = i;
                break;
            }
        }
        if(insert_fragment_index == -1) {
            throw new InvalidInsertValueException("the insert value does not belong to any fragment.");
        }
        else {
            std::string insert_sql = "insert into ";
            insert_sql += insert_stmt->table_name_ + " values (";
            for(int i = 0; i < record->size() - 1; ++i) {
                if(table->attributes_[i].type_ == AttributeType::CHAR) {
                    insert_sql += "'" + record->at(i) + "', ";    
                }
                else {
                    insert_sql += record->at(i) + ", ";
                }
            }
            if(table->attributes_[record->size() - 1].type_ == AttributeType::CHAR) {
                insert_sql += "'" + record->at(record->size() - 1) + "');";    
            }
            else {
                insert_sql += record->at(record->size() - 1) + ");";
            }
            std::vector<std::string> sql_vec;
            sql_vec.emplace_back(insert_sql);
            std::vector<std::string> site_vec;
            site_vec.emplace_back(fragments->at(insert_fragment_index).site_name_);
            for(int i = 0; i < sql_vec.size(); ++i) {
                std::cout << site_vec[i] << " : " << sql_vec[i] << std::endl;
            }
            executor->Data_Insert_Delete(sql_vec, site_vec);
        }
    }
    // ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????record
    else {
        std::vector<Fragment>* fragments = &(table->fragments_);
        std::vector<std::string> sql_vec;
        std::vector<std::string> site_vec;

        for(int i = 0; i < fragments->size(); ++i) {
            std::string sql = "insert into " + insert_stmt->table_name_ + " values (";
            Fragment* fragment = &(fragments->at(i));
            for(int j = 0; j < fragment->vertical_attr_index_.size() - 1; ++j) {
                if(table->attributes_[fragment->vertical_attr_index_[j]].type_ == AttributeType::CHAR) {
                    sql += "'" + record->at(fragment->vertical_attr_index_[j]) + "', ";
                }
                else {
                    sql += record->at(fragment->vertical_attr_index_[j]) + ", ";
                }
            }
            if(table->attributes_[fragment->vertical_attr_index_.back()].type_ == AttributeType::CHAR) {
                sql += "'" + record->at(fragment->vertical_attr_index_.back()) + "');";    
            }
            else{
                sql += record->at(fragment->vertical_attr_index_.back()) + ");";
            }
            sql_vec.emplace_back(sql);
            site_vec.emplace_back(fragment->site_name_);
        }

        for(int i = 0; i < sql_vec.size(); ++i) {
            std::cout << site_vec[i] << " : " << sql_vec[i] << std::endl;
        }
        executor->Data_Insert_Delete(sql_vec, site_vec);
    }
}

void process_delete(std::string command) {
    Table* table = parser->get_delete_stmt()->table_;
    std::vector<std::string> delete_record_key_values;
    std::vector<Fragment>* fragments = &(table->fragments_);
    DeleteStmt* delete_stmt = parser->get_delete_stmt();
    std::vector<Predicate>* delete_predicates = &(delete_stmt->predicates_);
    std::vector<std::string> sql_vec;
    std::vector<std::string> site_vec;
    // std::cout << "delete command: " << command << std::endl;

    // ???????????????????????????????????????????????????????????????????????????
    if(table->table_fragment_type_ == FragmentType::HORIZONTAL) {
        for(int i = 0; i < fragments->size(); ++i) {
            bool is_in_fragment = true;
            // ????????????delete?????????predicate??????????????????predicate?????????fragment??????predicate????????????
            for(int j = 0; j < delete_predicates->size(); ++j) {
                // std::cout << "delete predicates: " << delete_predicates->at(j).attribute_name_  << " "
                //  << OperationTypeStr[static_cast<int>(delete_predicates->at(j).operation_type_)] 
                //  << " " << delete_predicates->at(j).right_value_ << std::endl;
                if(!fragments->at(i).have_intersection(delete_predicates->at(j))) {
                    is_in_fragment = false;
                    break;
                }
            }

            // ????????????????????????????????????????????????
            if(is_in_fragment == false) continue;
            // ????????????????????????????????????????????????????????????
            // ??????command??????????????????????????????sql????????????????????????command?????????sql?????????
            // std::cout << "site" << i <<std::endl;
            sql_vec.emplace_back(command);
            site_vec.emplace_back(fragments->at(i).site_name_);
        }
    }
    // ??????????????????????????????????????????????????????????????????record????????????record?????????????????????delete_record_key_values???
    // ????????????delete_record_key_values????????????????????????record
    else {
        std::vector<std::string> select_sql_vec;
        std::vector<std::string> select_site_vec;
        for(int i = 0; i < fragments->size(); ++i) {
            // ??????select??????????????????????????????record???????????????
            std::string sql = "select " + table->attributes_[table->primary_key_index_].attr_name_ + " from " + table->table_name_ + " where ";
            bool need_select = false;

            for(int j = 0; j < delete_predicates->size(); ++j) {
                for(int k = 0; k < fragments->at(i).attributes_.size(); ++k)
                    if(fragments->at(i).attributes_[k].compare(delete_predicates->at(j).attribute_name_) == 0) {
                        // ?????????????????????????????????????????????select
                        // ??????need_select=false???????????????where_clause????????????????????????????????????????????????and
                        if(need_select == false) {
                            need_select = true;
                            // ???delete_predicates????????????????????????????????????????????????
                            sql += delete_predicates->at(j).attribute_name_ +
                                    OperationTypeStr[static_cast<int>(delete_predicates->at(j).operation_type_)] +
                                    delete_predicates->at(j).right_value_;
                        }
                        else {
                            sql += " and " + delete_predicates->at(j).attribute_name_ +
                                             OperationTypeStr[static_cast<int>(delete_predicates->at(j).operation_type_)] +
                                             delete_predicates->at(j).right_value_;
                        }
                    }
            }

            if(need_select == true) {
                sql += ";";
                select_sql_vec.emplace_back(sql);
                select_site_vec.emplace_back(fragments->at(i).site_name_);
            }
        }

        delete_record_key_values = executor->Data_Select_Single(select_sql_vec, select_site_vec);

        // ?????????????????????????????????????????????
        std::string primary_key_attr_name = table->attributes_[table->primary_key_index_].attr_name_;
        for(int i = 0; i < fragments->size(); ++i) {
            std::string sql = "delete from " + table->table_name_ + " where ";
            for(int j = 0; j < delete_record_key_values.size(); ++j) {
                if(j != 0) sql += " or ";
                sql += primary_key_attr_name + "=" + delete_record_key_values[j];
            }
            sql += ";";
            sql_vec.emplace_back(sql);
            site_vec.emplace_back(fragments->at(i).site_name_);
        }

    }

    for(int i = 0; i < sql_vec.size(); ++i) {
        std::cout << site_vec[i] << " : " << sql_vec[i] << std::endl;
    }
    executor->Data_Insert_Delete(sql_vec, site_vec);

}

QueryType parse_command(std::string command) {
    QueryType query_type = get_query_type(command);
//    std::cout << query_type_str[static_cast<int>(query_type)] << std::endl;
std::cout << query_type_str[static_cast<int>(query_type)] << std::endl;
    switch(query_type) {
        case QueryType::EXIT: {
            // TODO: ????????????
            return QueryType::EXIT;
        } break;
        case QueryType::DEFINE_SITE: {
            std::string site_name = "";
            std::string site_ip = "";
            std::string site_port = "";
            boost::regex regex(reg_define_site);
            site_name = boost::regex_replace(command, regex, "$2");
            site_ip = boost::regex_replace(command, regex, "$4");
            site_port = boost::regex_replace(command, regex, "$6");

            std::cout << "define site: \n";
            std::cout << "site name: "<< site_name << "\nsite ip: " << site_ip << "\nsite port: " << site_port << std::endl;
            meta_data->add_site(site_name, site_ip, site_port);
        } break;
        case QueryType::CREATE_DB: {
            std::string db_name = "";
            boost::regex regex(reg_create_db);
            db_name = boost::regex_replace(command, regex, "$2");

            // std::cout << "create database: \n";
            // std::cout << "database name: " << db_name << std::endl;
            meta_data->add_db(db_name);
            // executor->create_db_remote(db_name);
            // std::cout << "Query OK" << std::endl;
        } break;
        case QueryType::DROP_DB: {
            std::string db_name = "";
            boost::regex regex(reg_drop_db);
            db_name = boost::regex_replace(command, regex, "$2");

            std::cout << "drop database: \n";
            std::cout << "database name: " << db_name << std::endl;
            meta_data->drop_db(db_name);
        } break;
        case QueryType::USE_DB: {
            std::string db_name = "";
            boost::regex regex(reg_use_db);
            db_name = boost::regex_replace(command, regex, "$2");

            std::cout << "use database:\n";
            std::cout << "database name: " << db_name << std::endl;
            meta_data->use_db(db_name);
            std::cout << "Database Changed" << std::endl;
        } break;
        case QueryType::CREATE_TABLE: {
            std::string table_name = "";
            hsql::SQLParserResult result;
            hsql::SQLParser::parse(command, &result);

            if(!result.isValid()) {
                // TODO: invalid command exception
                std::cout << "command invalid" << std::endl;
            }

            if(result.isValid() && result.size() > 0) {
                const hsql::SQLStatement* statement = result.getStatement(0);
                if(statement->isType(hsql::kStmtCreate)) {
                    const auto* create = static_cast<const hsql::CreateStatement*>(statement);
                    table_name = create->tableName;
                }
                else {
                    throw InvalidCommandException("Invalid create statement.\n");
                }
            }
            // std::cout << "create table: " << table_name << std::endl;
            meta_data->add_table(table_name);
        } break;
        case QueryType::DROP_TABLE: {

        } break;
        case QueryType::CREATE_FRAGMENT: {
            // std::cout << "create fragment" << std::endl;
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
            // ????????????????????????????????????scp???????????????????????????mysql???insert??????????????????
            mysqlpp::Connection* conn  = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
            mysqlpp::Query query = conn->query();

            std::vector<std::string> sql_vec_delete;
            std::vector<std::string> site_vec_delete;
            for(auto iter : meta_data->delete_sql) {
                for(int i = 0; i < iter.second.size(); ++i) {
                    site_vec_delete.push_back(iter.first);
                    sql_vec_delete.push_back(iter.second[i]);
                }
            }
            executor->Data_Insert_Delete(sql_vec_delete, site_vec_delete);
            // std::cout << "finish delete\n";

            for(auto site : meta_data->site_map) {
                std::string site_name = site.first;
                std::string site_ip = site.second.first;
                std::string site_port = site.second.second;

                std::ifstream file;
                std::string sql;
                std::vector<std::string> sql_vec;
                std::vector<std::string> site_vec;
                file.open(meta_data->file_path_prefix + "data_load/" + site_name + "_data_loader.sql");
                while(getline(file, sql)) {
                    sql_vec.push_back(sql);
                    site_vec.push_back(site_name);
                }
                executor->Data_Insert_Delete(sql_vec, site_vec);
            }
        } break;
        case QueryType::INSERT: {
            // TODO:
            // ???delete??????
            std::cout << "insert command: " << command << std::endl;
            hsql::SQLParserResult result;
            hsql::SQLParser::parse(command, &result);

            if(!result.isValid()) {
                throw new InvalidCommandException("invalid insert command \"" + command + "\".");
            }

            parser->parse_insert(&result);
            process_insert();
            parser->free_insert_stmt();

        } break;
        case QueryType::DELETE: {
            // TODO:
            // parser: parser??????????????????????????????????????????????????????
            // network: ????????????network????????????????????????????????????????????????delete??????
            // executor: executor??????????????????????????????delete??????
            hsql::SQLParserResult result;
            hsql::SQLParser::parse(command, &result);

            if(!result.isValid()) {
                throw new InvalidCommandException("invalid delete command \"" + command + "\".");
            }

            parser->parse_delete(&result);
            process_delete(command);
            parser->free_delete_stmt();
        } break;
        case QueryType::SELECT: {
            // TODO:
            // parser??????????????????????????????,????????????????????????etcd???(??????????????????etcd???,????????????????????????????????????????)
            // ???????????????????????????????????????,??????pull??????? (executor)
            // ?????????????????????????????????etcd??????(?????????????????????????????????query_tree???????????????????????????,etcd??????key_value??????key??????????????????)
            // ?????????????????????request_temp_table(????????????????????????????????????(????????????)???????????????)????????????
            // request_temp_table?????????????????????????????????etcd???query_tree???????????????key
            // executor?????????query??????????????????

            mysqlpp::Connection* conn  = new mysqlpp::Connection("ddb", "127.0.0.1", "root", "123456");
            mysqlpp::Query query = conn->query();

            hsql::SQLParserResult result;
            hsql::SQLParser::parse(command, &result);

            std::cout << "finish parse" << std::endl;

            if(!result.isValid()) {
//                std::cout << "command invalid\n";
                throw new InvalidCommandException("invalid select command \"" + command + "\";");
            }
            parser->query_tree_generation(&result);
//             query_tree???????????????clear???
        //    parser->print_query_tree(parser->get_query_tree_root());
	
	    clock_t begin_time =clock();
            executor->exec_query_tree(-1);
	    clock_t end_time = clock();
	    std::cout << "Time used: " << (double)(end_time - begin_time) / CLOCKS_PER_SEC << " sec" << std::endl;

            parser->free_query_tree();

        } break;
        default:
        break;
    }
    return QueryType::SELECT;
}

int main(int argc, char **argv) {
    std::string command = "";
    meta_data->init();
//    executor->exec_sql_local("");
    while (true) {
        std::cout << "ddbms> ";
        getline(std::cin, command);
        // executor->sql_ = command;
        if(parse_command(command) == QueryType::EXIT) break;
    }
//    try{
//        parse_command(command);
//    } catch (InvalidCommandException &e) {
//        std::cout << e.what() << std::endl;
//    }
    return 0;
}
