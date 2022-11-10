//
// Created by sxy on 22-11-8.
//

#ifndef DDBMS_PREDICATE_H
#define DDBMS_PREDICATE_H
#include <iostream>

enum class OperationType {Equal, join};

class Predicate {
public:
    std::string attribute_name;
    std::string table_name;

    std::string right_value;
    std::string left_value;

    // used for join operation
    std::string right_attribute_name;
    std::string right_table_name;
};

#endif //DDBMS_PREDICATE_H
