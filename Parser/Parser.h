//
// Created by sxy on 22-11-10.
//

#ifndef DDBMS_PARSER_H
#define DDBMS_PARSER_H

#include "Optimizer.h"
#include "hsql/SQLParser.h"
#include "Defs/QueryTree.h"

class Parser {
public:
    Parser(Optimizer* optimizer) : optimizer_(optimizer) {}

    QueryNode* query_tree_generation(hsql::SQLParserResult* result);

private:
    Optimizer* optimizer_;
};

#endif //DDBMS_PARSER_H
