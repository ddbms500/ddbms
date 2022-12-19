#ifndef METADATA_H
#define METADATA_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <Defs/Attribute.h>
#include <Defs/Fragment.h>
#include <Defs/Table.h>
#include "Defs/QueryTree.h"

class MetaData {
public:
    bool add_site(std::string name, std::string ip, std::string port);
    bool add_db(std::string db_name);
    bool drop_db(std::string db_name);
    bool use_db(std::string db_name);
    bool add_table(std::string table_name, std::vector<Attribute> attributes);
    bool drop_table(std::string table_name);
    bool add_fragment(Fragment fragment);
    bool allocate(std::string fragment_name, std::string site_name);
    // TODO: 这边参数可能需要改咩
    Table* get_table(std::string table_name);
    // TODO: 这边怎么传参比较合理呢
    void show_sites();
    void show_databases();
    void show_tables();
    void show_fragments();

    // 用于测试, 把元数据写死, 后续删掉
    void init();

    // used for insert
    // 如果是水平分片,那么只需要返回一个站点的信息,如果是竖直分片,那么需要返回每个站点要插入的记录包含的属性值
    // TODO: 这边属性的参数用什么数据类型来传递比较好
    std::unordered_map<std::string, std::string> get_insert_location();

    // 根据table名称获得Table对象的下标
    std::unordered_map<std::string, int> table_index;
    std::vector<Table> tables;
    // site_name, <site_ip, site_port>
    // 根据site name获得site的ip和port
    std::unordered_map<std::string, std::pair<std::string, std::string>> site_map;
    // 记录当前机器的site_name
    std::string local_site_name;
    QueryTree query_tree;
};

#endif