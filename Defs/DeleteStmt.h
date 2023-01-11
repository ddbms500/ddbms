//
// Created by sxy on 22-12-7.
//

#ifndef DDBMS_DELETESTMT_H
#define DDBMS_DELETESTMT_H

#include "Predicate.h"
#include "Table.h"
#include <vector>

class DeleteStmt {
public:
    std::string table_name_;
    Table* table_;
    std::vector<Predicate> predicates_;
};

#endif //DDBMS_DELETESTMT_H
