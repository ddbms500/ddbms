//
// Created by sxy on 22-11-5.
//

#ifndef DDBMS_QUERYTREE_H
#define DDBMS_QUERYTREE_H

#include <iostream>
#include <vector>
#include "Predicate.h"

const enum class NodeType {PROJECTION, SELECTION, JOIN, UNION};

class QueryNode {
public:
    bool is_parent(void) { return is_parent_; }
    bool is_leaf(void) { return is_leaf_; }
    bool add_son(QueryNode* node) { sons_.push_back(node); }

private:
    NodeType node_type_;
    std::string site_;
    Predicate predicate_;
    QueryNode* parent_; // (parent node) -> parent = nullptr
    std::vector<QueryNode*> sons_; // (leaf node) ->sons.size() = 0
    bool is_parent_;
    bool is_leaf_;
};

#endif //DDBMS_QUERYTREE_H
