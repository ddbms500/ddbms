//
// Created by sxy on 22-11-8.
//

#ifndef DDBMS_PREDICATE_H
#define DDBMS_PREDICATE_H
#include <iostream>
#include <assert.h>

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

    bool value_cmp(std::string left_value, OperationType op, std::string right_value) {
        // if left_value op right_value, return true
        // else return false
        switch(op) {
            case OperationType::EQUAL: {
                if(left_value.compare(right_value) == 0) return true;
                return false;
            } break;
            case OperationType::LESS: {
                if(left_value.compare(right_value) < 0) return true;
                return false;
            } break;
            case OperationType::LESS_EQUAL: {
                if(left_value.compare(right_value) <= 0) return true;
                return false;
            } break;
            case OperationType::GREAT: {
                if(left_value.compare(right_value) > 0) return true;
                return false;
            } break;
            case OperationType::GREAT_EQUAL: {
                if(left_value.compare(right_value) >= 0) return true;
                return false;
            } break;
            default: break;
        }
        return false;
    }

    // ?????????join???predicate????????????????????????
    bool is_satisfy_predicate(const std::string& value) {
        assert(operation_type_ != OperationType::JOIN);

        if(left_operation_type_ != OperationType::DEFAULT) {
            if(value_cmp(left_value_, left_operation_type_, value) &&
                    value_cmp(value, operation_type_, right_value_))
                return true;
            return false;
        }
        return value_cmp(value, operation_type_, right_value_);
    }

    // ??????????????????id < 5???????????????????????????
    bool single_expr_intersection_check(OperationType op1, const std::string& value1, OperationType op2, const std::string& value2) {
        // ????????????predicate???op??????????????????????????????????????????right_value???????????????predicate??????????????????
        if(op1 == OperationType::EQUAL) {
            switch(op2) {
                case OperationType::EQUAL: {
                    return value1.compare(value2) == 0;
                } break;
                case OperationType::LESS: {
                    if(value1.compare(value2) < 0) return true;
                    return false;
                } break;
                case OperationType::LESS_EQUAL: {
                    if(value1.compare(value2) <= 0) return true;
                    return false;
                } break;
                case OperationType::GREAT: {
                    if(value1.compare(value2) > 0) return true;
                    return false;
                } break;
                case OperationType::GREAT_EQUAL: {
                    if(value1.compare(value2) >= 0) return true;
                    return false;
                } break;
                default: break;
            }
        }
        // ????????????predicate???op?????????????????????op?????????????????????????????????right_value???????????????predicate???????????????
        else if(op2 == OperationType::EQUAL) {
            switch(op1) {
                case OperationType::LESS: {
                    if(value2.compare(value1) < 0) return true;
                    return false;
                } break;
                case OperationType::LESS_EQUAL: {
                    if(value2.compare(value1) <= 0) return true;
                    return false;
                } break;
                case OperationType::GREAT: {
                    if(value2.compare(value1) > 0) return true;
                    return false;
                } break;
                case OperationType::GREAT_EQUAL: {
                    if(value2.compare(value1) >= 0) return true;
                    return false;
                } break;
                default: break;
            }
        }
        // ???????????????????????????????????????
        else if(op1 == OperationType::LESS) {
            if(op2 == OperationType::GREAT_EQUAL || op2 == OperationType::GREAT) {
                if(value2.compare(value1) < 0) {
                    return true;
                }
                return false;
            }
            return true;
        }
        else if(op1 == OperationType::LESS_EQUAL) {
            if(op2 == OperationType::GREAT) {
                if(value2.compare(value1) < 0) return true;
                return false;
            }
            else if(op2 == OperationType::GREAT_EQUAL) {
                if(value2.compare(value1) <= 0) return true;
                return false;
            }
            return true;
        }
        else if(op1 == OperationType::GREAT) {
            if(op2 == OperationType::LESS || op2 == OperationType::LESS_EQUAL) {
                if(value2.compare(value1) > 0) return true;
                return false;
            }
            return true;
        }
        else if(op1 == OperationType::GREAT_EQUAL) {
            if(op2 == OperationType::LESS) {
                if(value2.compare(value1) > 0) return true;
                return false;
            }
            else if(op2 == OperationType::LESS_EQUAL) {
                if(value2.compare(value1) >= 0) return true;
                return false;
            }
            return true;
        }

        return true;
    }

    // ????????????predicate??????????????????????????????????????????true???????????????false
    bool have_intersection(const Predicate& predicate) {
        // ???????????????left_value
        if(this->left_operation_type_ == OperationType::DEFAULT && predicate.left_operation_type_ == OperationType::DEFAULT) {
            return single_expr_intersection_check(this->operation_type_, this->right_value_,
                                                  predicate.operation_type_, predicate.right_value_);
        }
        // ????????????predicate???left_value?????????predicate??????left_value
        else if(this->left_operation_type_ != OperationType::DEFAULT && predicate.left_operation_type_ == OperationType::DEFAULT) {
            // ??????????????????predicate?????????????????????predicate?????????????????????????????????????????????????????????predicate?????????predicate?????????
            if(single_expr_intersection_check(this->left_operation_type_, this->left_value_,
                                              predicate.operation_type_, predicate.right_value_) &&
                    single_expr_intersection_check(this->operation_type_, this->right_value_,
                                                   predicate.operation_type_, predicate.right_value_))
                return true;
            return false;
        }
        // ????????????predicate???left_value?????????predicate??????left_value
        else if(this->left_operation_type_ == OperationType::DEFAULT && predicate.left_operation_type_ != OperationType::DEFAULT) {
            // ??????????????????predicate?????????????????????predicate?????????????????????????????????????????????????????????predicate?????????predicate?????????
            if(single_expr_intersection_check(this->operation_type_, this->right_value_,
                                              predicate.left_operation_type_, predicate.left_value_) &&
                    single_expr_intersection_check(this->operation_type_, this->right_value_,
                                                   predicate.operation_type_, predicate.right_value_))
                return true;
            return false;
        }

        // ????????????left_value
        // ????????????????????????where_clause????????????????????????????????????predicate???????????????????????????????????????????????????????????????????????????
        // ??????????????????predicate??????????????????????????? 1 <(=) id <(=) 3????????????????????????
        OperationType op_left = predicate.left_operation_type_;
        OperationType op_right = predicate.operation_type_;
        std::string value_left = predicate.left_value_;
        std::string value_right = predicate.right_value_;

        if(this->left_operation_type_ == OperationType::LESS || this->left_operation_type_ == OperationType::LESS_EQUAL) {
            std::swap(this->left_operation_type_, this->operation_type_);
            std::swap(this->left_value_, this->right_value_);
        }
        if(op_left == OperationType::LESS || op_right == OperationType::LESS_EQUAL) {
            std::swap(op_left, op_right);
            std::swap(value_left, value_right);
        }

        // ??????predicate????????????????????????????????????????????????????????????eg. this_predicate: [a,b]; predicate: (c, d], ????????????[a???d]???????????????(c???b]?????????
        if(single_expr_intersection_check(this->left_operation_type_, this->left_value_, op_right, value_right) &&
                single_expr_intersection_check(this->operation_type_, this->right_value_, op_left, value_left))
            return true;
        return false;
    }

    std::string attribute_name_;
    std::string table_name_;

    OperationType operation_type_ = OperationType::DEFAULT;
    OperationType left_operation_type_ = OperationType::DEFAULT;
    // ????????????left_operation_type?????????DEFAULT?????????????????????left_value

    std::string right_value_;
    std::string left_value_;

    // used for join operation
    std::string right_attribute_name_;
    std::string right_table_name_;

    void print_predicate() {
        if(operation_type_ == OperationType::JOIN) {
            std::cout << "JOIN: ";
            // ???????????????join????????????????????????
            std::cout << table_name_<<"."<<attribute_name_ << " = "<<right_table_name_<<"."<<right_attribute_name_;
        }
        else{
            std::cout << table_name_<<"."<<attribute_name_ << OperationTypeStr[static_cast<int>(operation_type_)] <<" " << right_value_;
        }
    }
};


#endif //DDBMS_PREDICATE_H
