#ifndef METADATA_H
#define METADATA_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <Defs/Attribute.h>
#include <Defs/Fragment.h>
#include <Defs/Table.h>
#include "Defs/QueryTree.h"

//王乙霖增加的头文件
#include <cstring>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

class MetaData {
public:
    bool add_site(std::string name, std::string ip, std::string port);//设置site_map,在etcd中人工设置目录结构：/site_name
    bool add_db(std::string db_name);//在etcd和本地记录db_name
    bool drop_db(std::string db_name);//在etcd和本地记录删除db_name下的所有key
    bool use_db(std::string db_name);//设置默认数据库名称，之后所有的sql语句默认会在该数据库进行操作
    bool add_table(std::string table_name, std::vector<Attribute> attributes);//在etcd中人工设置目录结构：/dbs/db_name/table_name/site_name/attribute_name/...
    bool drop_table(std::string table_name);//在etcd中删除/dbs/db_name/table_name及其子目录的所有key
    bool add_fragment(std::string table_name,Fragment fragment);//在etcd中人工设置目录结构：/dbs/db_name/table_name/site_name/fragment_name/...

    Table get_table(std::string table_name);//获取默认数据库下table_name的所有attribute和fragment

    void show_sites();//在函数里面用std::string存储了site_name
    void show_databases();//在函数里面用std::string存储了db_name
    void show_tables();
    void show_fragments();

    // 用于测试, 把元数据写死, 后续删掉
    void init();

    // // used for insert
    // // 如果是水平分片,那么只需要返回一个站点的信息,如果是竖直分片,那么需要返回每个站点要插入的记录包含的属性值
    // // TODO: 这边属性的参数用什么数据类型来传递比较好
    // std::unordered_map<std::string, std::string> get_insert_location();

    // 根据table名称获得Table对象的下标
    std::unordered_map<std::string, int> table_index;
    std::vector<Table> tables;
    // site_name, <site_ip, site_port>
    // 根据site name获得site的ip和port
    std::unordered_map<std::string, std::pair<std::string, std::string>> site_map;
    // 记录当前机器的site_name
    std::string local_site_name;

    QueryTree query_tree;

    /*修改了参数的成员函数*/
    bool allocate(std::string fragment_name, std::string site_name,std::string table_name);//指定fragment在哪个site_name里，修改了参数，增设table_name
    /*新增成员变量*/
    etcd::Client etcd;//会在构造函数中隐式初始化
    std::string default_db_name;//默认的数据库名称，会在use_db(db_name)中设置
    std::string current_site_name;//当前的节点名称，和Fragment对应site_name_一致，会在构造函数中显式初始化
    /*新增成员函数*/
    MetaData(std::string site_name)
            :etcd("http://127.0.0.1:2379")
            ,current_site_name(site_name)
    {
    }

    bool write_query_tree_to_etcd(QueryTree query_tree);//把query_tree写到etcd中
    QueryTree read_query_tree_from_etcd(int root_id);//把以root_id为根节点的子树从etcd中读出来并返回(在同一时间etcd只会存在一颗query_tree)
    bool delete_query_tree_from_etcd(int root_id);

    int get_location_of_str(std::string str,int num){//获取字符串中第num个'/'的位置
        int location = -1;
        while(num!=0){
            if(str.find("/",location+1)==std::string::npos){
                return -1;
            }
            location=str.find("/",location+1);
            num--;
        }
        return location;
    }
};

#endif