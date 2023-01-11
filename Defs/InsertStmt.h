//
// Created by sxy on 22-12-7.
//

#ifndef DDBMS_INSERTSTMT_H
#define DDBMS_INSERTSTMT_H

#include <iostream>
#include <vector>
#include "Table.h"

class InsertStmt {
public:
    std::string table_name_;
    Table* table_;
    std::vector<std::string> values_;
    InsertStmt(){}
    ~InsertStmt(){}
};

#endif //DDBMS_INSERTSTMT_H
