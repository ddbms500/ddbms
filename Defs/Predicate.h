//
// Created by sxy on 22-11-8.
//

#ifndef DDBMS_PREDICATE_H
#define DDBMS_PREDICATE_H
#include <iostream>

enum class OperationType {JOIN, EQUAL, LESS, LESS_EQUAL, GREAT, GREAT_EQUAL, DEFAULT};
// less = 2 less_equal = 3
// great = 4 great_equal = 5
// less ^ 1 = less_equal
// great ^ 1 = great_equal
static const std::string OperationTypeStr[] = {"join", "=", "<", "<=", ">", ">=", ""};

class Predicate {
public:
    Predicate() {}
    // the most common predicate; eg. id < 10400
    Predicate(const std::string& attribute_name, OperationType operation_type, const std::string& right_value)
    : attribute_name_(attribute_name), operation_type_(operation_type), right_value_(right_value) {}

    // Book.title = "A"
    Predicate(const std::string& table_name, const std::string& attribute_name, OperationType operation_type, const std::string& right_value)
    : table_name_(table_name), attribute_name_(attribute_name), operation_type_(operation_type), right_value_(right_value) {}

    // join predicate; eg. Book.publisher_id = Publisher.id
    Predicate(const std::string& table_name, const std::string& attribute_name, OperationType operation_type,
              const std::string& right_table_name, const std::string& right_attribute_name)
              : table_name_(table_name), attribute_name_(attribute_name), operation_type_(operation_type),
              right_table_name_(right_table_name), right_attribute_name_(right_attribute_name){}

    std::string attribute_name_;
    std::string table_name_;

    OperationType operation_type_ = OperationType::DEFAULT;
    OperationType left_operation_type_ = OperationType::DEFAULT;
    // 通过检查left_operation_type是否是DEFAULT来判断是否存在left_value

    std::string right_value_;
    std::string left_value_;

    // used for join operation
    std::string right_attribute_name_;
    std::string right_table_name_;
};

#endif //DDBMS_PREDICATE_H
