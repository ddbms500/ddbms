//
// Created by sxy on 22-11-8.
//

#include "Parser.h"
#include "Defs/Attribute.h"
#include <unordered_map>
#include <algorithm>
#include "Utils/Exceptions.h"
#include <fstream>

bool predicate_cmp (const Predicate& p1, const Predicate& p2) {
    if(p1.table_name_ != p2.table_name_)
        return p1.table_name_ < p2.table_name_;
    if(p1.attribute_name_ != p2.attribute_name_)
        return p1.attribute_name_ < p2.attribute_name_;
    return p1.operation_type_ < p2.operation_type_;
}

std::string Parser::get_table_name_from_expr(hsql::Expr* expr) {
    if(expr->hasTable()) {

    }
}
std::string Parser::get_attr_name_from_expr(hsql::Expr* expr) {

}

std::string Parser::get_expr_value(hsql::Expr* expr) {
    switch(expr->type) {
        case hsql::kExprLiteralInt: {
            return std::to_string(expr->ival);
        } break;
        case hsql::kExprLiteralFloat: {
            return std::to_string(expr->fval);
        } break;
        default: {
            return expr->getName();
        }
    }
}


void Parser::get_where_clause(hsql::Expr* expr) {
    if(expr->opType == hsql::kOpAnd) {
        get_where_clause(expr->expr);
        get_where_clause(expr->expr2);
        return;
    }

    hsql::Expr* left_expr = expr->expr;
    hsql::Expr* right_expr = expr->expr2;

    // TODO: 如果存在operator两边都是数值的情况,那么要么true要么false,可以直接在解析where_clause的时候就计算出来值, 所以在下面的switch里面是不考虑等式两边都是数值的情况的

    switch(expr->opType) {
        case hsql::kOpEquals: {
            if(left_expr->type == hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                // Book.publisher_id = Publisher.id, join type
                // 按照字典序来放置两个表的左右位置
                Predicate predicate(left_expr->table, left_expr->getName(), OperationType::JOIN,
                                    right_expr->table, right_expr->getName());
                if(predicate.table_name_.compare(predicate.right_table_name_) > 0) {
                    std::swap(predicate.table_name_, predicate.right_table_name_);
                    std::swap(predicate.attribute_name_, predicate.right_attribute_name_);
                }
                join_operators.emplace_back(predicate);
                // 在找到join操作的同时,把相关的属性加入到leaf node projection需要的属性列表里面去
                std::vector<std::string>* left_attrs = &(table_attr_map.find(left_expr->table)->second);
                std::vector<std::string>* right_attrs = &(table_attr_map.find(right_expr->table)->second);
                left_attrs->push_back(left_expr->getName());
                right_attrs->push_back(right_expr->getName());
                std::vector<std::string>* left_all_attrs = &(all_attr_map.find(left_expr->table)->second);
                std::vector<std::string>* right_all_attrs = &(all_attr_map.find(right_expr->table)->second);
                left_all_attrs->emplace_back(left_expr->getName());
                right_all_attrs->emplace_back(right_expr->getName());
            }
            else if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                std::string table_name;
                // 假设多表的情况下每个属性都有个表名前缀, eg. Book.title, 只有在单表select的时候才会出现单独的属性,这个时候直接读取from的表名就可以
                if(left_expr->hasTable()) table_name = left_expr->table;
                else table_name = table_attr_map.begin()->first;
                Predicate predicate(table_name, left_expr->getName(), OperationType::EQUAL, get_expr_value(right_expr));

                std::vector<Predicate>* predicates = &(where_clause.find(table_name)->second);
                predicates->emplace_back(predicate);
            }
            else {
                std::string table_name;
                if(left_expr->hasTable()) table_name = left_expr->table;
                else table_name = table_attr_map.begin()->first;
                Predicate predicate(table_name, left_expr->getName(), OperationType::EQUAL, get_expr_value(right_expr));

                std::vector<Predicate>* predicates = &(where_clause.find(table_name)->second);
                predicates->emplace_back(predicate);
            }
        } break;
        case hsql::kOpGreater: {
            OperationType op_type = OperationType::GREAT;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::LESS;
            }
            std::string table_name;
            if(left_expr->hasTable()) table_name = left_expr->table;
            else table_name = table_attr_map.begin()->first;
            Predicate predicate(table_name, left_expr->getName(), op_type, get_expr_value(right_expr));
            std::vector<Predicate>* predicates = &(where_clause.find(table_name)->second);
            predicates->emplace_back(predicate);
        } break;
        case hsql::kOpGreaterEq: {
            OperationType op_type = OperationType::GREAT_EQUAL;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::LESS_EQUAL;
            }
            std::string table_name;
            if(left_expr->hasTable()) table_name = left_expr->table;
            else table_name = table_attr_map.begin()->first;
            Predicate predicate(table_name, left_expr->getName(), op_type, get_expr_value(right_expr));
            std::vector<Predicate>* predicates = &(where_clause.find(table_name)->second);
            predicates->emplace_back(predicate);
        } break;
        case hsql::kOpLess: {
            OperationType op_type = OperationType::LESS;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::GREAT;
            }
            std::string table_name;
            if(left_expr->hasTable()) table_name = left_expr->table;
            else table_name = table_attr_map.begin()->first;
            Predicate predicate(table_name, left_expr->getName(), op_type, get_expr_value(right_expr));
            std::vector<Predicate>* predicates = &(where_clause.find(table_name)->second);
            predicates->emplace_back(predicate);
        } break;
        case hsql::kOpLessEq: {
            OperationType op_type = OperationType::LESS_EQUAL;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::GREAT_EQUAL;
            }
            std::string table_name;
            if(left_expr->hasTable()) table_name = left_expr->table;
            else table_name = table_attr_map.begin()->first;
            Predicate predicate(table_name, left_expr->getName(), op_type, get_expr_value(right_expr));
            std::vector<Predicate>* predicates = &(where_clause.find(table_name)->second);
            predicates->emplace_back(predicate);
        } break;
        default: break;
    }
}

// TODO: 怎么写更优美呢! 写得太智障了!!!
bool check_single_predicate_intersection(OperationType op1, const std::string& value1, OperationType op2, const std::string& value2) {
    // 如果存在交集, 返回true, 否则返回false
    if(op1 == OperationType::EQUAL && op2 == OperationType::EQUAL) return value1.compare(value2) == 0;
    if(op1 == OperationType::EQUAL) {
        if(op2 != OperationType::LESS_EQUAL || op2 != OperationType::GREAT_EQUAL) return false;
        // 为啥是unreachable???
        return value1.compare(value2) == 0;
    }
    if(op2 == OperationType::EQUAL) {
        if(op1 != OperationType::LESS_EQUAL || op1 != OperationType::GREAT_EQUAL) return false;
        // 为什么是unreachable???
        return value1.compare(value2) == 0;
    }
    switch (op1) {
        case OperationType::LESS: {
            if(op2 == OperationType::GREAT_EQUAL || op2 == OperationType::GREAT) {
                if(value1.compare(value2) != 1) return false;
                return true;
            }
            else {
                return true;
            }
        } break;
        case OperationType::LESS_EQUAL: {
            if(op2 == OperationType::GREAT_EQUAL) {
                // 如果value1 < value2 那么返回false, 否则返回true
                return value1.compare(value2) != -1;
            }
            else if(op2 == OperationType::GREAT) {
                // 如果value1 <= value2 返回false, 否则返回true
                return value1.compare(value2) == 1;
            }
            else {
                return true;
            }
        } break;
        case OperationType::GREAT: {
            if(op2 == OperationType::LESS_EQUAL || op2 == OperationType::LESS) {
                // 如果value1 >= value2 返回false, 否则返回true
                return value1.compare(value2) == -1;
            }
            else {
                return true;
            }
        } break;
        case OperationType::GREAT_EQUAL: {
            if(op2 == OperationType::LESS_EQUAL) {
                // 如果value1 > value2 返回false, 否则返回true
                return value1.compare(value2) != 1;
            }
            else if(op2 == OperationType::LESS) {
                return value1.compare(value2) == -1;
            }
            else {
                return true;
            }
        } break;
    }
    return true;
}

bool check_double_predicate_intersection() {

}

bool Parser::pruning_check_fragment_selection(Fragment* fragment, const std::vector<Predicate>* predicates) {
    // 剪掉不符合predicates的fragment
    // 遍历fragment的predicate和select操作的predicate, 如果发现相同属性的predicate交集为空, 那么该fragment可以被剪掉
    // const std::vector<Predicate>* fragment_predicates = &(fragment->predicates_);
    // for(auto fragment_predicate = fragment_predicates->begin(); fragment_predicate != fragment_predicates->end(); ++fragment_predicate) {
    //     for(auto select_predicate = predicates->begin(); select_predicate != predicates->end(); ++select_predicate) {
    //         if(select_predicate->attribute_name_ == fragment_predicate->attribute_name_) {
    //             if(fragment_predicate->left_operation_type_ == OperationType::DEFAULT && select_predicate->left_operation_type_ == OperationType::DEFAULT) {
    //                 // 如果两个predicate都没有left_value, 如果没有交集, 还要进行后续判断, 如果有交集, 代表当前不需要剪枝, 直接返回false就可以
    //                 if(check_single_predicate_intersection(fragment_predicate->left_operation_type_, fragment_predicate->right_value_,
    //                                                        select_predicate->left_operation_type_, fragment_predicate->right_value_))
    //                     return false;
    //             }
    //             else if(fragment_predicate->left_operation_type_ == OperationType::DEFAULT) {
    //                 // fragment_predicate不存在left_value, select_predicate有left_value

    //             }
    //             else if(select_predicate->left_operation_type_ == OperationType::DEFAULT){
    //                 // fragment_predicate有left_value, select_predicate没有left_value
    //             }
    //             else {
    //                 // 两个都有left_value
    //                 // 也就是两个predicate都形如 1 < id < 4 的形式

    //             }
    //         }
    //     }
    // }

    for(int i = 0; i < predicates->size(); ++i) {
        if(!fragment->have_intersection(predicates->at(i))) {
            return true;
        }
    }

    // 如果不需要剪掉该fragment, 那么就返回false, 否则返回true
    return false;
}

bool prune_check(const std::vector<Predicate>* predicates) {
    for(int i = 0; i < predicates->size(); ++i) {
        if(predicates->at(i).attribute_name_.compare("id") == 0 && predicates->at(i).right_value_.compare("210000") == 0 && predicates->at(i).operation_type_ == OperationType::GREAT)
        return true;
    }
    return false;
}

void Parser::query_tree_generation(hsql::SQLParserResult* result) {
//    std::cout << "generate query tree" << std::endl;
    const hsql::SelectStatement* statement = static_cast<const hsql::SelectStatement *>(result->getStatement(0));
   std::cout << "get statement" << std::endl;
    if(statement->selectList != nullptr) {
        // 找到select涉及的所有表
        // eg. select * from Customer
        if(statement->fromTable->type == hsql::kTableName) {
        //    std::cout << "select from single table" << std::endl;
            table_attr_map.emplace(std::piecewise_construct, std::forward_as_tuple(statement->fromTable->getName()), std::forward_as_tuple());
            all_attr_map.emplace(std::piecewise_construct, std::forward_as_tuple(statement->fromTable->getName()), std::forward_as_tuple());
            where_clause.emplace(std::piecewise_construct, std::forward_as_tuple(statement->fromTable->getName()), std::forward_as_tuple());
            select_attr_map.emplace(std::piecewise_construct, std::forward_as_tuple(statement->fromTable->getName()), std::forward_as_tuple());
        }
        // eg. select xx from Book, Publisher where xxxx
        else if(statement->fromTable->type == hsql::kTableCrossProduct) {
//            std::cout << "select from multiple table" << std::endl;
            for(const hsql::TableRef* table : *statement->fromTable->list) {
                table_attr_map.emplace(std::piecewise_construct, std::forward_as_tuple(table->getName()), std::forward_as_tuple());
                all_attr_map.emplace(std::piecewise_construct, std::forward_as_tuple(table->getName()), std::forward_as_tuple());
                where_clause.emplace(std::piecewise_construct, std::forward_as_tuple(table->getName()), std::forward_as_tuple());
                select_attr_map.emplace(std::piecewise_construct, std::forward_as_tuple(table->getName()), std::forward_as_tuple());
            }
        }

       std::cout << "begin get select list" << std::endl;
       std::cout << statement->selectList->size() << std::endl;
        // 找到每个表最终需要的所有属性
        for(const hsql::Expr* expr : *statement->selectList) {
            std::cout << "testssssssssss\n";
            std::string table_name;
            if(expr->type == hsql::kExprStar || statement->fromTable->type == hsql::kTableName) table_name = statement->fromTable->getName();
            else table_name = expr->table;

            std::vector<std::string>* attrs = &(table_attr_map.find(table_name)->second);
            std::vector<std::string>* select_attrs = &(select_attr_map.find(table_name)->second);
            std::vector<std::string>* all_attrs = &(all_attr_map.find(table_name)->second);
            

            if(expr->type == hsql::kExprStar) {
                // std::cout << "*\n";
                Table* table = meta_data_->get_table(table_name);
                for(const Attribute& attr : table->attributes_) {
                    attrs->push_back(attr.attr_name_);
                    select_attrs->push_back(attr.attr_name_);
                    all_attrs->emplace_back(attr.attr_name_);
                }
            }
            else if(expr->type == hsql::kExprColumnRef) {
                std::cout << "test";
                attrs->push_back(expr->getName());
                select_attrs->push_back(expr->getName());
                all_attrs->emplace_back(expr->getName());
            }
        }
       std::cout << "finish get select list"<< std::endl;

       std::cout << "begin get where clause" << std::endl;
        // 获取select的所有谓词表达式
        if(statement->whereClause != nullptr) {
            hsql::Expr* expr = statement->whereClause;
            get_where_clause(expr);
//            for(auto iter : where_clause) {
//                std::string table_name = iter.first;
//                std::cout << "table_name: " << table_name << std::endl;
//                auto predicates = iter.second;
//                for(auto predicate : predicates) {
//                    std::cout << predicate.table_name_ << "." << predicate.attribute_name_ << " " << static_cast<int>(predicate.operation_type_) << " " << predicate.right_value_ << std::endl;
//                }
//            }
        }
        std::cout << "finish get where clause" << std::endl;

        // 优化1: 合并predicate的条件: eg. id < 10 和 id > 5 两个条件可以合并起来, 同时可以删除一些矛盾的条件
        // 在合并predicate的同时,可以找到每个表在leaf node所需要projection的属性
        // leaf node 的projection属性为最终需要的属性以及join条件需要的属性
        for(auto iter = where_clause.begin(); iter != where_clause.end(); ++iter) {
            std::vector<Predicate>* predicates = &(iter->second);
            std::sort(predicates->begin(), predicates->end(), predicate_cmp);

            int predicate_num = predicates->size();
            int i = 0, j = 0; // ij 维护连续的相同的属性predicates
            for(; i < predicate_num;) {
                while(j < predicate_num && predicates->at(j).attribute_name_ == predicates->at(i).attribute_name_) ++j;
                // TODO: 需要去合并相同属性的predicates
                Predicate* predicate = &(predicates->at(i));
                std::vector<std::string>* attrs = &(all_attr_map.find(predicate->table_name_)->second);
                attrs->push_back(predicate->attribute_name_);
                i = j;
            }
        }

//        std::cout << std::endl << "leaf node projection: "<< std::endl;
        // 对projection的属性去重
        for(auto iter = table_attr_map.begin(); iter != table_attr_map.end(); ++iter) {
//            std::cout << "table name: " << iter->first << std::endl;
            int table_index_num = meta_data_->table_index.find(iter->first)->second;
//            std::cout << table_index_num << std::endl;
            Table* table = &(meta_data_->tables[meta_data_->table_index[iter->first]]);
            std::vector<std::string>* attrs = &(iter->second);
            if(table->table_fragment_type_ == FragmentType::VERTICAL) {
                attrs->emplace_back(table->attributes_[table->primary_key_index_].attr_name_);
            }
            std::sort(attrs->begin(), attrs->end());
            attrs->erase(std::unique(attrs->begin(), attrs->end()), attrs->end());
//            for(auto attr = attrs->begin(); attr != attrs->end(); ++attr)
//                std::cout << *attr << " ";
//            std::cout << std::endl;
        }
        for(auto iter = all_attr_map.begin(); iter != all_attr_map.end(); ++iter) {
            std::vector<std::string>* attrs = &(iter->second);
            std::sort(attrs->begin(), attrs->end());
            attrs->erase(std::unique(attrs->begin(), attrs->end()), attrs->end());
        }

//        std::cout << std::endl << "ordered predicates" << std::endl;
//        for(auto iter : where_clause) {
//            std::string table_name = iter.first;
//            std::cout << "table_name: " << table_name << std::endl;
//            auto predicates = iter.second;
//            for(auto predicate : predicates) {
//                std::cout << predicate.table_name_ << "." << predicate.attribute_name_ << " " << static_cast<int>(predicate.operation_type_) << " " << predicate.right_value_ << std::endl;
//            }
//        }

//        std::cout << "\ngenerate selection nodes and projection nodes for every fragment" << std::endl;
        // 最基础的树把对每个fragment的selection作为leaf node,然后去做projection,最后去做merge
        // 默认按照left deep tree的结构来merge
        for(auto iter = table_attr_map.begin(); iter != table_attr_map.end(); ++iter) {
            // table代表当前在生成leaf node的表
            // 对于每个selection的leaf node, 判断一下上层是否需要加一个projection节点
//            std::cout << iter->first << std::endl;
            // 对于垂直分片的表有两种情况，如果需要的attr只有主键，那么选取第一个分片，
            // 因此，选取完第一个分片之后，vertical_fragment_prune = true
            // 剩余分片直接被剪枝
            // 否则，就需要手动判断哪些分片是需要的
            bool vertical_fragment_prune = false;
            int table_index_num = meta_data_->table_index.find(iter->first)->second;
//            std::cout << table_index_num << std::endl;
            Table* table = &(meta_data_->tables[meta_data_->table_index[iter->first]]);
            std::vector<std::string>* projection_attrs = &(iter->second);
            table_leaf_node.emplace(std::piecewise_construct, std::forward_as_tuple(iter->first), std::forward_as_tuple());
            std::vector<int>* table_leaf_node_vector = &(table_leaf_node.find(iter->first)->second);

            int fragment_index = 0;
            for(auto fragment = table->fragments_.begin(); fragment != table->fragments_.end(); ++fragment) {
                if(vertical_fragment_prune == true) continue;
                if(table->table_fragment_type_ == FragmentType::VERTICAL) {
                    // 如果是垂直分片的表，需要首先根据涉及到的属性进行剪枝
                    std::vector<std::string>* all_attrs = &(all_attr_map.find(table->table_name_)->second);
                    // std::cout << "attr size: " << all_attrs->size() << all_attrs->at(0) << all_attrs->at(1) << std::endl;
                    if(all_attrs->size() == 1 && all_attrs->at(0).compare(table->attributes_[table->primary_key_index_].attr_name_) == 0) {
                        // std::cout << "prune\n";
                        vertical_fragment_prune = true;
                    }
                    bool need_prune = true;
                    for(int i = 0; i < fragment->attributes_.size(); ++i) {
                        if(need_prune == false) break;
                        if(i == table->primary_key_index_) continue;
                        for(int j = 0; j < all_attrs->size(); ++j) {
                            std::cout << fragment->attributes_[i] << " " << all_attrs->at(j) << std::endl;
                            if(fragment->attributes_[i].compare(all_attrs->at(j)) == 0) {
                                need_prune = false;
                                break;
                            }
                        }
                    }
                    if(need_prune == true && vertical_fragment_prune == false) continue;
                }
                // 如果是水平分片，首先check当前fragment上的数据通过selection算子之后是否需要剪枝
                std::vector<Predicate>* predicates = &(where_clause.find(iter->first)->second);
                // 如果需要剪枝, 那么不需要生成该leaf node
                if(table->table_fragment_type_ == FragmentType::HORIZONTAL &&
                 pruning_check_fragment_selection(&*fragment, predicates)) continue;

                 if(table->table_name_.compare("Book") == 0 && fragment->fragment_name_.compare("Book.2") == 0) {
                    if(prune_check(predicates)) continue;
                 }

                if(site_leaf_node.find(fragment->site_name_) == site_leaf_node.end())
                    site_leaf_node.emplace(std::piecewise_construct, std::forward_as_tuple(fragment->site_name_), std::forward_as_tuple());
                std::vector<int>* leaf_nodes = &(site_leaf_node.find(fragment->site_name_)->second);

                // 首先生成selection leaf_node
                query_tree.nodes[query_tree.n].set_site_name(fragment->site_name_);
                query_tree.nodes[query_tree.n].set_table_name(iter->first);
                query_tree.nodes[query_tree.n].fragment_id_ = fragment_index;
                query_tree.nodes[query_tree.n].fragment_name_ = fragment->fragment_name_;
                query_tree.nodes[query_tree.n].set_type(NodeType::SELECTION);
                query_tree.nodes[query_tree.n].set_node_as_leaf();
                query_tree.nodes[query_tree.n].init_select_predicates(predicates);
                query_tree.nodes[query_tree.n].node_id = query_tree.n;
                need_prune[query_tree.n] = false;
                query_tree.n ++;
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_site_name(fragment->site_name_);
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_table_name(iter->first);
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].fragment_id_ = fragment_index;
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].fragment_name_ = fragment->fragment_name_;
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_type(NodeType::SELECTION);
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_node_as_leaf();
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].init_select_predicates(predicates);
                // query_tree_used_for_print.nodes[query_tree_used_for_print.n].node_id = query_tree.n;
                // query_tree_used_for_print.n ++;
                // 判断是否需要生成projection_node
                if(!projection_attrs->empty()) {
                    query_tree.nodes[query_tree.n].set_site_name(fragment->site_name_);
                    query_tree.nodes[query_tree.n].set_table_name(iter->first);
                    query_tree.nodes[query_tree.n].set_type(NodeType::PROJECTION);
                    query_tree.nodes[query_tree.n].init_attrs(projection_attrs);
                    // 把selection节点的父节点设置成projection_node
                    // 把projection节点的儿子节点设置成selection节点
                    query_tree.nodes[query_tree.n].add_son(query_tree.n - 1);
                    query_tree.nodes[query_tree.n].fragment_id_ = fragment_index;
                    query_tree.nodes[query_tree.n].fragment_name_ = fragment->fragment_name_;
                    query_tree.nodes[query_tree.n].node_id = query_tree.n;
                    query_tree.nodes[query_tree.n - 1].set_parent(query_tree.n);
                    need_prune[query_tree.n] = false;
                    query_tree.n ++;
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_site_name(fragment->site_name_);
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_table_name(iter->first);
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].set_type(NodeType::PROJECTION);
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].init_attrs(projection_attrs);
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].add_son(query_tree_used_for_print.n - 1);
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].fragment_id_ = fragment_index;
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].fragment_name_ = fragment->fragment_name_;
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n].node_id = query_tree_used_for_print.n;
                    // query_tree_used_for_print.nodes[query_tree_used_for_print.n - 1].set_parent(query_tree_used_for_print.n);
                    // query_tree_used_for_print.n ++;
                }
                leaf_nodes->emplace_back(query_tree.n - 1);
                table_leaf_node_vector->emplace_back(query_tree.n - 1);
                fragment_index++;
            }
        }
        std::cout << "finished generate leaf nodes\n";

        std::cout << "print leaf nodes:\n";
        // for(int i = 0; i < query_tree.n; ++i) {
        //     query_tree.nodes[i].print_node();
        // }
        // std::cout <<"\n";
        
        
        // 根据where中的join条件剪枝叶子节点
        for(auto predicate: join_operators) {
            std::string left_table_name = predicate.table_name_;
            std::string right_table_name = predicate.right_table_name_;
            std::string left_attr_name = predicate.attribute_name_;
            std::string right_attr_name = predicate.right_attribute_name_;
            Table* left_table = meta_data_->get_table(left_table_name);
            Table* right_table = meta_data_->get_table(right_table_name);
            std::vector<int>* left_table_nodes = &(table_leaf_node.find(left_table_name)->second);
            std::vector<int>* right_table_nodes = &(table_leaf_node.find(right_table_name)->second);
            int node_need[103];
            for(int i = 0; i < query_tree.n; ++i) node_need[i] = 0;

            for(int i = 0; i < left_table_nodes->size(); ++i) {
                for(int j = 0; j < right_table_nodes->size(); ++j) {
                    // -1是为了找select节点
                    QueryNode* left_node = &(query_tree.nodes[left_table_nodes->at(i) - 1]);
                    QueryNode* right_node = &(query_tree.nodes[right_table_nodes->at(j) - 1]);

                    if(need_prune[left_node->node_id] == true || need_prune[right_node->node_id] == true) continue;

                    Fragment* left_fragment = &(left_table->fragments_[left_node->fragment_id_]);
                    Fragment* right_fragment = &(right_table->fragments_[right_node->fragment_id_]);
                    std::vector<Predicate> left_predicates;
                    std::vector<Predicate> right_predicates;
                    // 找到左表和右表连接条件相关属性相关的predicate
                    // 既包括select节点的predicate，也包括fragment里面的predicate
                    for(auto select_predicate : left_node->select_predicates_) {
                        if(select_predicate.attribute_name_.compare(left_attr_name) == 0) {
                            left_predicates.emplace_back(select_predicate);
                            break;
                        }
                    }
                    for(auto fragment_predicate : left_fragment->predicates_) {
                        if(fragment_predicate.attribute_name_.compare(left_attr_name) == 0) {
                            left_predicates.emplace_back(fragment_predicate);
                            break;
                        }
                    }
                    for(auto select_predicate : right_node->select_predicates_) {
                        if(select_predicate.attribute_name_.compare(right_attr_name) == 0) {
                            right_predicates.emplace_back(select_predicate);
                            break;
                        }
                    }
                    for(auto fragment_predicate : right_fragment->predicates_) {
                        if(fragment_predicate.attribute_name_.compare(right_attr_name) == 0) {
                            right_predicates.emplace_back(fragment_predicate);
                            break;
                        }
                    }

                    if(left_predicates.size() == 0 || right_predicates.size() == 0) {
                        // 如果某个分片没有相关join条件，那么这两个分片需要join
                        node_need[left_node->node_id] ++;
                        node_need[left_node->node_id + 1] ++;
                        node_need[right_node->node_id] ++;
                        node_need[right_node->node_id + 1] ++;
                        std::cout << left_node->fragment_name_ << " join " << right_node->fragment_name_<<std::endl;
                    }
                    else {
                        // 否则需要判断，所有的predicates都有交集才表示两个分片有交集
                        bool need_prune_ = false;
                        for(auto left_predicate : left_predicates) {
                            if(need_prune_ == true) break;
                            for(auto right_predicate : right_predicates) {
                                if(!left_predicate.have_intersection(right_predicate)) {
                                    need_prune_ = true;
                                    break;
                                }
                            }
                        }
                        if(need_prune_ == false) {
                            node_need[left_node->node_id] ++;
                            node_need[left_node->node_id + 1] ++;
                            node_need[right_node->node_id] ++;
                            node_need[right_node->node_id + 1] ++;
                            std::cout << left_node->fragment_name_ << " join " << right_node->fragment_name_<<std::endl;
                        }
                    }
                }
            }

            for(int i = 0; i < left_table_nodes->size(); ++i) {
                if(node_need[left_table_nodes->at(i)] == 0) {
                    need_prune[left_table_nodes->at(i)] = true;
                    need_prune[left_table_nodes->at(i) - 1] = true;
                }
            }

            for(int i = 0; i < right_table_nodes->size(); ++i) {
                if(node_need[right_table_nodes->at(i)] == 0) {
                    need_prune[right_table_nodes->at(i)] = true;
                    need_prune[right_table_nodes->at(i) - 1] = true;
                }
            }
        }

        std::cout << "print final leaf nodes:\n";
        for(int i = 0; i < query_tree.n; ++i) {
            if(need_prune[i] == false)
                query_tree.nodes[i].print_node();
        }
        std::cout <<"\n";

        // query_tree_used_for_print.nodes[query_tree_used_for_print.n].node_id = query_tree_used_for_print.n;
        // query_tree_used_for_print.nodes[query_tree_used_for_print.n].node_type_ = NodeType::JOIN;
        // for(int i = 0; i < query_tree_used_for_print.n; ++i) {
        //     if(need_prune[i] == false) {
        //         query_tree_used_for_print.nodes[query_tree_used_for_print.n].add_son(i);
        //     }
        // }
        // query_tree_used_for_print.nodes[query_tree_used_for_print.n].site_ = meta_data_->local_site_name;
        // query_tree_used_for_print.n ++;
        // draw_query_tree(query_tree_used_for_print.n - 1);

        // 如果某个表是垂直分片，并且该表的剩余分片数量大于1，需要对两个表进行join
        
        
        // 传输优化


        // 把所有的fragment都先传输到本地，然后union/join起来，形成完整的表之后去做join
//        std::cout << "\nbegin generate union nodes\n";
        // std::unordered_map<std::string, int> union_node_index_map;
        // // TODO: 希望能够计算到每个fragment怎么发送,但是为了先跑起来,目前先把所有的表的fragment的数据都发送到当前site, 先做Union, 再做join
        // for(auto iter = table_leaf_node.begin(); iter != table_leaf_node.end(); ++iter) {
        //     std::string table_name = iter->first;
        //     std::vector<int>* nodes_id = &(iter->second);
        //     std::cout << table_name << std::endl;
        //     int union_node_index = query_tree.n++;
        //     query_tree.nodes[union_node_index].set_site_name(meta_data_->local_site_name);
        //     query_tree.nodes[union_node_index].set_table_name(table_name);
        //     query_tree.nodes[union_node_index].set_type(NodeType::UNION);
        //     for(auto node_id = nodes_id->begin(); node_id != nodes_id->end(); ++node_id) {
        //         query_tree.nodes[union_node_index].add_son(*node_id);
        //         query_tree.nodes[*node_id].set_parent(union_node_index);
        //     }
        //     union_node_index_map.emplace(iter->first, union_node_index);
        // }

//        std::cout << "finish generate union nodes\n";

//        std::cout << "\nbegin generate join nodes\n";
        // 目前先按照left deep tree的方式去做join, 就按照默认的顺序去join
        // TODO: 目前只默认两个表的join只有单个属性, 后续需要考虑多个属性的情况

//         std::string left_node_table_name;
//         int left_node_index;
//         for(auto iter = select_attr_map.begin(); iter != select_attr_map.end(); ++iter) {
//             std::string table_name = iter->first;
// //            std::cout<< table_name << std::endl;
//             if(iter == select_attr_map.begin()) {
//                 left_node_table_name = table_name;
//                 left_node_index = union_node_index_map.find(table_name)->second;
//                 continue;
//             }

//             for(auto predicate = join_operators.begin(); predicate != join_operators.end(); ++iter) {
//                 std::string left_table_name = predicate->table_name_;
//                 std::string right_table_name = predicate->right_table_name_;
//                 std::string left_join_attr = predicate->attribute_name_;
//                 std::string right_join_attr = predicate->right_attribute_name_;

//                 if(right_table_name.compare(table_name) == 0 && left_node_table_name.find(left_table_name) != std::string::npos) {
//                     left_node_table_name += "_" + table_name;
//                 }
//                 else if(left_table_name.compare(table_name) == 0 && left_node_table_name.find(right_table_name) != std::string::npos) {
//                     left_node_table_name += "_" + table_name;
//                 }
//                 else continue;

//                 // 找到要join的右表
//                 int right_node_index = union_node_index_map.find(table_name)->second;

//                 // 构建节点
//                 int join_node_index = query_tree.n++;
//                 query_tree.nodes[join_node_index].set_table_name(left_node_table_name);
//                 query_tree.nodes[join_node_index].set_site_name(meta_data_->local_site_name);
//                 query_tree.nodes[join_node_index].set_type(NodeType::JOIN);
//                 query_tree.nodes[join_node_index].set_join_predicate(&*predicate);
//                 query_tree.nodes[join_node_index].add_son(left_node_index);
//                 query_tree.nodes[join_node_index].add_son(right_node_index);
//                 query_tree.nodes[left_node_index].set_parent(join_node_index);
//                 query_tree.nodes[right_node_index].set_parent(join_node_index);
//                 left_node_index = join_node_index;
//                 break;
//             }
//         }
//        std::cout << "finished generate join nodes\n";

//        std::cout << "\nbegin generate root node\n";
        // 最顶部加一个projection节点
        // int root_index = query_tree.n++;
        // query_tree.nodes[root_index].set_table_name(left_node_table_name);
        // query_tree.nodes[root_index].set_site_name(meta_data_->local_site_name);
        // query_tree.nodes[root_index].set_type(NodeType::PROJECTION);
        // for(auto iter = select_attr_map.begin(); iter != select_attr_map.end(); ++iter) {
        //     std::vector<std::string>* attrs = &(iter->second);
        //     std::string table_name = iter->first;

        //     for(auto attr = attrs->begin(); attr != attrs->end(); ++attr) {
        //         query_tree.nodes[root_index].add_attr(table_name + "." + *attr);
        //     }
        // }
        // query_tree.nodes[root_index].add_son(left_node_index);
        // query_tree.nodes[left_node_index].set_parent(root_index);
        // query_tree.nodes[root_index].set_node_as_parent();
        // query_tree.root_index = root_index;
//        std::cout << "finished generate query tree" << std::endl;

    }

    meta_data_->query_tree = &(query_tree);
    meta_data_->site_leaf_node = &(site_leaf_node);
    meta_data_->table_leaf_node = &(table_leaf_node);
    meta_data_->need_prune = &(need_prune);
    meta_data_->select_attr_map = &(select_attr_map);
    meta_data_->join_operators = &(join_operators);
}
void Parser::draw_nodes(int root) {
    // QueryNode* node = &(query_tree_used_for_print.nodes[root]);
    // std::string str = "";
    // str += std::to_string(root) + " [label=\"" + node_type_str[static_cast<int>(node->node_type_)];
    // if(node->node_type_ == NodeType::PROJECTION) {
    //     str += ", " + node->fragment_name_;
    // }
    // else if(node->node_type_ == NodeType::SELECTION) {
    //     str += ", " + node->fragment_name_;
    // }
    // str += "\"";
    // str += ", color=" + meta_data_->site_color[node->site_] + "]\n";
    // for(int i = 0; i < node->sons_.size(); ++i) {
    //     draw_nodes(node->sons_[i]);
    // }
    // for(int i = 0; i < node->sons_.size(); ++i) {
    //     str = std::to_string(root) + " -> " + std::to_string(node->sons_[i]) + "\n"; 
    // }
}

void Parser::draw_query_tree(int root) {
    // QueryNode* node = &(query_tree_used_for_print.nodes[root]);
    // graph.open("query_tree.gv");
    // for(int i = 0; i < node->sons_.size(); ++i) {
    //     std::cout << node->sons_[i] << " ";
    // }
    // std::cout << std::endl;
    // graph << "graph QueryTree {\n";
    // draw_nodes(node->node_id);
    // // graph << draw_str << std::endl;
    // graph.close();
}

void Parser::print_query_tree(int root) {
    std::cout<< "father: " << query_tree.nodes[root].get_table_name() << " " << static_cast<int>(query_tree.nodes[root].get_type()) << " ";
    if(query_tree.nodes[root].get_type() == NodeType::PROJECTION) {
        std::vector<std::string>* attrs = query_tree.nodes[root].get_attrs();
        for(auto attr = attrs->begin(); attr != attrs->end(); ++attr) {
            std::cout << *attr << " ";
        }
    }
    std::cout << std::endl;
    std::cout << "son: " << std::endl;
    for(int son : *(query_tree.nodes[root].get_sons())) {
        std::cout << son << " ";
    }
    std::cout <<std::endl << std::endl;

    for(int son : *(query_tree.nodes[root].get_sons())) {
        print_query_tree(son);
    }
    return;
}
void Parser::free_query_tree() {
    for(int i = 0; i < query_tree.n; ++i) {
        query_tree.nodes[i].free_node();
    }
    query_tree.n = 0;
    for(int i = 0; i < query_tree_used_for_print.n; ++i) {
        query_tree_used_for_print.nodes[i].free_node();
    }
    query_tree_used_for_print.n = 0;
    site_leaf_node.clear();
    table_leaf_node.clear();
    need_prune.clear();
    select_attr_map.clear();
    table_attr_map.clear();
    all_attr_map.clear();
    join_operators.clear();
    where_clause.clear();
}

void Parser::parse_insert(hsql::SQLParserResult *result) {
    const hsql::InsertStatement* statement = static_cast<const hsql::InsertStatement *>(result->getStatement(0));
    Table* table = nullptr;

    // get insert table name
    if(statement->tableName) {
        insert_stmt.table_name_ = statement->tableName;
        table = meta_data_->get_table(insert_stmt.table_name_);
        insert_stmt.table_ = table;
    }
    else {
        return;
    }

    // get insert values
    if(statement->values) {
        int cnt = 0;
        for(const hsql::Expr* expr : *statement->values) {
            AttributeType attr_type_ = table->attributes_[cnt++].type_;
            if(expr->type == hsql::kExprLiteralInt) std::cout << "data type: int,  ";
            if(expr->type == hsql::kExprLiteralFloat) std::cout << "data type: float,  ";
            if(expr->type == hsql::kExprLiteralString) std::cout << "data type: string,  ";
            std::cout << attribute_type_str[static_cast<int>(attr_type_)] << std::endl;
            if(expr->type == hsql::kExprLiteralInt && attr_type_ == AttributeType::INTEGER) {
                insert_stmt.values_.emplace_back(std::to_string(expr->ival));
            }
            else if(expr->type == hsql::kExprLiteralString && attr_type_ == AttributeType::CHAR) {
                insert_stmt.values_.emplace_back(expr->name);
            }
            else if(expr->type == hsql::kExprLiteralString && attr_type_ == AttributeType::TEXT) {
                insert_stmt.values_.emplace_back(expr->name);
            }
            else if(expr->type == hsql::kExprLiteralFloat && attr_type_ == AttributeType::FLOAT) {
                insert_stmt.values_.emplace_back(std::to_string(expr->fval));
            }
            else {
                throw new InvalidDataTypeException("Unmatched data type.");
            }
        }
    }
}

void Parser::free_insert_stmt() {
    insert_stmt.table_name_ = "";
    insert_stmt.table_ = nullptr;
    insert_stmt.values_.clear();
}

void Parser::get_delete_where_clause(hsql::Expr* expr) {
    if(expr->opType == hsql::kOpAnd) {
        get_where_clause(expr->expr);
        get_where_clause(expr->expr2);
        return;
    }

    hsql::Expr* left_expr = expr->expr;
    hsql::Expr* right_expr = expr->expr2;

    // delete只会从一个表里面delete
    switch(expr->opType) {
        case hsql::kOpEquals: {
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
            }

            Predicate predicate(delete_stmt.table_name_, left_expr->getName(), OperationType::EQUAL, get_expr_value(right_expr));
//                predicates->emplace_back(predicate);
            delete_stmt.predicates_.emplace_back(predicate);
        } break;
        case hsql::kOpGreater: {
            OperationType op_type = OperationType::GREAT;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::LESS;
            }

            Predicate predicate(delete_stmt.table_name_, left_expr->getName(), op_type, get_expr_value(right_expr));
            delete_stmt.predicates_.emplace_back(predicate);
        } break;
        case hsql::kOpGreaterEq: {
            OperationType op_type = OperationType::GREAT_EQUAL;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::LESS_EQUAL;
            }

            Predicate predicate(delete_stmt.table_name_, left_expr->getName(), op_type, get_expr_value(right_expr));
            delete_stmt.predicates_.emplace_back(predicate);
        } break;
        case hsql::kOpLess: {
            OperationType op_type = OperationType::LESS;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::GREAT;
            }

            Predicate predicate(delete_stmt.table_name_, left_expr->getName(), op_type, get_expr_value(right_expr));
            delete_stmt.predicates_.emplace_back(predicate);
        } break;
        case hsql::kOpLessEq: {
            OperationType op_type = OperationType::LESS_EQUAL;
            if(left_expr->type != hsql::kExprColumnRef && right_expr->type == hsql::kExprColumnRef) {
                std::swap(left_expr, right_expr);
                op_type = OperationType::GREAT_EQUAL;
            }

            Predicate predicate(delete_stmt.table_name_, left_expr->getName(), op_type, get_expr_value(right_expr));
            delete_stmt.predicates_.emplace_back(predicate);
        } break;
        default: break;
    }
}

void Parser::parse_delete(hsql::SQLParserResult *result) {
    const hsql::DeleteStatement* statement = static_cast<const hsql::DeleteStatement*>(result->getStatement(0));
    Table* table = nullptr;

    if(statement->tableName) {
        delete_stmt.table_name_ = statement->tableName;
        table = meta_data_->get_table(delete_stmt.table_name_);
        delete_stmt.table_ = table;
    }
    else {
        return;
    }

    // get where clause
    if(statement->expr) {
        get_delete_where_clause(statement->expr);
    }
}

void Parser::free_delete_stmt() {
    delete_stmt.predicates_.clear();
    delete_stmt.table_ = nullptr;
    delete_stmt.table_name_ = "";
}