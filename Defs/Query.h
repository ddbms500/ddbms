//
// Created by sxy on 22-10-20.
//

#ifndef DDBMS_QUERY_H
#define DDBMS_QUERY_H
#include <iostream>

enum class QueryType {
    DEFINE_SITE,
    CREATE_DB,
    DROP_DB,
    USE_DB,
    CREATE_TABLE,
    DROP_TABLE,
    CREATE_FRAGMENT,
    ALLOCATE,
    SHOW_DATABASES,
    SHOW_TABLES,
    SHOW_SITES,
    SHOW_FRAGMENTS,
    LOAD_DATA,
    INSERT,
    DELETE,
    SELECT,
    EXIT,
//    TEST_TYPE,
    UNKNOWN_TYPE
};

// used for debug & test
static std::string query_type_str[20] = {"DEFINE_SITE",
                                  "CREATE_DB",
                                  "DROP_DB",
                                  "USE_DB",
                                  "CREATE_TABLE",
                                  "DROP_TABLE",
                                  "CREATE_FRAGMENT",
                                  "ALLOCATE",
                                  "SHOW_DATABASES",
                                  "SHOW_TABLES",
                                  "SHOW_SITES",
                                  "SHOW_FRAGMENTS",
                                  "LOAD_DATA",
                                  "INSERT",
                                  "DELETE",
                                  "SELECT",
                                  "EXIT",
//                                  "TEST_TYPE",
                                  "UNKNOWN_TYPE"};

// define site "site name" xx.xx.xx.xx:xxxx (ip + port);
static const std::string reg_define_site = "^(define\\s+site\\s+)([A-Za-z0-9]+)(\\s+)([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3})(:)([0-9]+)(\\s*;)$";
// create database "database name";
static const std::string reg_create_db = "^(create\\s+database\\s+)([A-Za-z0-9]+)\\s*;$";
// drop database "database name";
static const std::string reg_drop_db = "^(drop\\s+database\\s+)([A-Za-z0-9]+)\\s*;$";
// create table "table name" ("col name" "col type", ...);
// 在这里不考虑sql语法错误的问题,只单纯做命令prefix的判别以及简单的sql语法判别
static const std::string reg_create_table = "^create\\s+table\\s+[A-Za-z0-9]+\\s*\\([^;]+\\)\\s*;$";
// drop table "table name";
static const std::string reg_drop_table = "^drop\\s+table\\s+[A-Za-z0-9]+\\s*;$";
// use "database name";
static const std::string reg_use_db = "^(use\\s+)([A-Za-z0-9]+)\\s*;$";
// create horizontal|vertical fragment "fragment name" on "table name" "fragment conditions";
static const std::string reg_create_fragment = "^create\\s+(horizontal|vertical)\\s+fragment\\s+[A-Za-z0-9.]+\\s+on\\s+[A-Za-z0-9]+\\s+[^;]+\\s*;$";
// allocate "fragmentation name" to "site name";
static const std::string reg_allocate = "^allocate\\s+[A-Za-z0-9.]+\\s+to\\s+[A-Za-z0-9]+\\s*;$";
// show database;
static const std::string reg_show_databases = "^show\\s+database\\s*;$";
// show tables;
static const std::string reg_show_tables = "^show\\s+tables\\s*;$";
// show sites;
static const std::string reg_show_sites = "^show\\s+sites\\s*;$";
// show fragments;
static const std::string reg_show_fragments = "^show\\s+fragments\\s*;$";
// load data "file name"
// TODO: 文件里面内容是什么,是load data到某个表?还是文件里包括表的元数据?格式需要一并调整
static const std::string reg_load_data = "^load\\s+data\\s+[A-Za-z0-9.,]+\\s*;$";
// insert into "table name" values()
// TODO: 理论上来说分号也是允许的,这边values()括号里的regex该怎么写比较合理呢?
static const std::string reg_insert = "^insert\\s+into\\s*[^;]+\\s*;$";
// delete from "table name" where "conditions"
static const std::string reg_delete = "^delete\\s+from\\s+[A-Za-z0-9]+\\s+where\\s+[^;]+\\s*;$";
// select xxx from xxx where xxx
static const std::string reg_select = "^select\\s+((([A-Za-z0-9.]+\\s*,\\s*)*[A-Za-z0-9.]+)|\\*)\\s+from\\s+([A-Za-z0-9]+\\s*,\\s*)*[A-Za-z0-9]+((\\s+[^;]+)|(\\s*))\\s*;$";
// exit;
static const std::string reg_exit = "^exit\\s*;$";

//static const std::string reg_test = "test";



#endif //DDBMS_QUERY_H
