//
// Created by sxy on 22-11-5.
//

#ifndef DDBMS_QUERYTREE_H
#define DDBMS_QUERYTREE_H

#include <iostream>
#include <vector>
#include "Predicate.h"
#define NODE_SIZE 1003

enum class NodeType {PROJECTION, SELECTION, JOIN, UNION};
static std::string node_type_str[5] = {"PROJECTION", "SELECTION", "JOIN", "UNION"};

class QueryNode {
public:
    QueryNode(){}
    bool is_parent(void) { return is_parent_; }
    bool is_leaf(void) { return is_leaf_; }
    void set_node_as_parent() { is_parent_ = true; }
    void set_node_as_leaf() { is_leaf_ = true; }
    bool add_son(int node) { sons_.emplace_back(node); }
    std::string get_table_name() { return table_name_; }
    void set_table_name(const std::string& table_name) { table_name_ = table_name; }
    std::string get_site_name() { return site_; }
    void set_site_name(const std::string& site) { site_ = site; }
    void add_attr(const std::string& attr) { attr_name_.emplace_back(attr); }
    void init_attrs(std::vector<std::string>* attrs) { attr_name_ = *attrs; }
    std::vector<std::string>* get_attrs() { return &attr_name_; }
    std::vector<Predicate>* get_select_predicates() { return &select_predicates_; }
    void init_select_predicates(std::vector<Predicate>* predicates) { select_predicates_ = *predicates; }
    Predicate* get_join_predicate() { return &join_predicate_; }
    void set_join_predicate(Predicate* predicate) { join_predicate_ = *predicate; }
    int get_parent() { return parent_; }
    void set_parent(int parent) { parent_ = parent; }
    std::vector<int>* get_sons() { return &sons_; }
    void set_type(NodeType type) { node_type_ = type; }
    NodeType get_type() { return node_type_; }

    void free_node() {
        site_ = "";
        table_name_ = "";
        fragment_id_ = 0;
        fragment_name_ = "";
        attr_name_.clear();
        select_predicates_.clear();
        sons_.clear();
        is_parent_ = false;
        is_leaf_ = false;
        node_id = 0;
    }

    // 理论上来说,把每个节点生成的数据当成一个临时表,那么其实每个节点都最多包含两个表在做join
    NodeType node_type_;
    std::string site_; // 在哪个站点执行操作
    std::string table_name_; // 对于单点select需要记录select table name
    int fragment_id_; // 用于叶子节点，当前节点对应的fragment在表中的index
    std::string fragment_name_; // 用于打印信息

    std::vector<std::string> attr_name_; // 投影算子需要的attr_name, eg. customer_id, orders_customer_id等
    std::vector<Predicate> select_predicates_; // select算子需要的谓词条件, eg. book_copies > 5000
    Predicate join_predicate_; // join算子需要的谓词条件 eg. book_publisher_id = publisher_id
    int parent_; // (parent node) -> parent = -1, 存储父亲节点的编号
    std::vector<int> sons_; // (leaf node) ->sons.size() = 0
    bool is_parent_ = false;
    bool is_leaf_ = false;
    int node_id ; //节点在整棵树的nodes中的下表
    /**
     * projection: node_type_(节点类型), site_(站点名称), table_name_, attr_name_,
     */

    void print_node() {
        std::cout << "node " << node_id << ": " << table_name_ <<" ";
        std::cout << node_type_str[static_cast<int>(node_type_)] <<" ";
        std::cout << fragment_id_ <<" "<< fragment_name_ << " ";
        if(node_type_ == NodeType::PROJECTION) {
            for(int i = 0; i < attr_name_.size(); ++i)
                std::cout << attr_name_[i] << " ";
            std::cout << std::endl;
        }
        if(node_type_ == NodeType::SELECTION) {
            for(int i = 0; i < select_predicates_.size(); ++i) {
                select_predicates_[i].print_predicate();
                std::cout << " and ";
            }
            std::cout << "\n";
        }
        std::cout << std::endl;
    }
};

class QueryTree {
public:
    int tree_id;// >=0，每个执行树的唯一ID
    int n = 0;
    int root_index = -1;
    QueryNode nodes[NODE_SIZE];
};

#endif //DDBMS_QUERYTREE_H
