//
// Created by sxy on 22-11-10.
//

#ifndef DDBMS_PARSER_H
#define DDBMS_PARSER_H

#include "Optimizer.h"
#include "MetaData/MetaData.h"
#include "hsql/SQLParser.h"
#include "Defs/QueryTree.h"
#include "Defs/Predicate.h"
#include "Defs/InsertStmt.h"
#include "Defs/DeleteStmt.h"

class Parser {
public:
    Parser(Optimizer* optimizer, MetaData* meta_data)
    : optimizer_(optimizer), meta_data_(meta_data) {}

    std::string get_table_name_from_expr(hsql::Expr* expr);
    std::string get_attr_name_from_expr(hsql::Expr* expr);

    void query_tree_generation(hsql::SQLParserResult* result);
    void print_query_tree(int root);
    int get_query_tree_root() { return query_tree.root_index; }
    void get_where_clause(hsql::Expr* expr);
    std::string get_expr_value(hsql::Expr* expr);
    bool pruning_check_fragment_selection(const Fragment* fragment, const std::vector<Predicate>* predicates);
    void parse_delete(hsql::SQLParserResult* result);
    void parse_insert(hsql::SQLParserResult* result);
    void get_delete_where_clause(hsql::Expr* expr);
    void free_query_tree();
    void free_insert_stmt();
    void free_delete_stmt();
    DeleteStmt* get_delete_stmt() { return &delete_stmt; }
    InsertStmt* get_insert_stmt() { return &insert_stmt; }

private:
    Optimizer* optimizer_;
    MetaData* meta_data_;

    // used for leaf node selection operator
    std::unordered_map<std::string, std::vector<std::string>> table_attr_map;
    // used for top select attr
    std::unordered_map<std::string, std::vector<std::string>> select_attr_map;
    std::unordered_map<std::string, std::vector<Predicate>> where_clause;
    std::vector<Predicate> join_operators;
    QueryTree query_tree;
    // used for delete
    DeleteStmt delete_stmt;
    // used for insert
    InsertStmt insert_stmt;
};

/**
 * 先生成一个global query tree
 * 然后用fragment来替代每张表
 * reduction: 根据选择算子,删除不需要的fragment
 * 先在站点本地join,然后再传送到某个站点union, 这样可以删除不需要的join操作,比如某个站点的join(ENO)两张表fragment的eno不重合
 */

/**
 * 无论是从where_clause解析获得的predicates还是meta_data里面存储的fragment的predicates,
 * 感觉fragment里的predicates排序没啥用啊, 先不排序了
 * 都需要按照属性来进行合并然后排序,这样无论在pruning还是在query_plan优化的时候都很方便
 * 希望都能够按照以下形式来进行存储:
 * 10 < id < 20
 * 10 <= id < 20
 * 10 < id <= 20
 * 10 <= id <= 20
 * id = 5
 * id < 10
 * id <= 10
 * id > 20
 * id >= 10
 * 上述规则的意思是,
 * 如果同时存在left_value和right_value, 那么希望能够按照从小到大的顺序来排列,
 * 如果只存在一个value, 那么都存在right_value,
 * 对于所有只存在right_value的情况, 按照operator的顺序来进行排序: =, <, <=, >, >=
 */

#endif //DDBMS_PARSER_H
