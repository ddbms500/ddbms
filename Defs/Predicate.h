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

    // 只有非join的predicate才能调用这个函数
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

    // 判断两个形如id < 5的表达式是否有交集
    bool single_expr_intersection_check(OperationType op1, const std::string& value1, OperationType op2, const std::string& value2) {
        // 如果当前predicate的op是等于，那么只需要判断当前的right_value是否在参数predicate的范围内即可
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
        // 如果当前predicate的op不是等于，并且op是等于，那么就判断当前right_value是否在参数predicate范围内即可
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
        // 两个操作符都不是等于的情况
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

    // 判断两个predicate是否有交集，如果有交集，返回true，否则返回false
    bool have_intersection(const Predicate& predicate) {
        // 两个都没有left_value
        if(this->left_operation_type_ == OperationType::DEFAULT && predicate.left_operation_type_ == OperationType::DEFAULT) {
            return single_expr_intersection_check(this->operation_type_, this->right_value_,
                                                  predicate.operation_type_, predicate.right_value_);
        }
        // 只有当前predicate有left_value，参数predicate没有left_value
        else if(this->left_operation_type_ != OperationType::DEFAULT && predicate.left_operation_type_ == OperationType::DEFAULT) {
            // 当且仅当参数predicate的表达式和当前predicate的两个表达式都有交集的时候，才代表当前predicate和参数predicate有交集
            if(single_expr_intersection_check(this->left_operation_type_, this->left_value_,
                                              predicate.operation_type_, predicate.right_value_) &&
                    single_expr_intersection_check(this->operation_type_, this->right_value_,
                                                   predicate.operation_type_, predicate.right_value_))
                return true;
            return false;
        }
        // 只有参数predicate有left_value，当前predicate没有left_value
        else if(this->left_operation_type_ == OperationType::DEFAULT && predicate.left_operation_type_ != OperationType::DEFAULT) {
            // 当且仅当当前predicate的表达式和参数predicate的两个表达式都有交集的时候，才代表当前predicate和参数predicate有交集
            if(single_expr_intersection_check(this->operation_type_, this->right_value_,
                                              predicate.left_operation_type_, predicate.left_value_) &&
                    single_expr_intersection_check(this->operation_type_, this->right_value_,
                                                   predicate.operation_type_, predicate.right_value_))
                return true;
            return false;
        }

        // 两个都有left_value
        // 因为测试数据中的where_clause是合取范式，所以如果两个predicate都是双表达式，就代表两个都不存在只等于一个值的情况
        // 因此，把两个predicate的双表达式都转化成 1 <(=) id <(=) 3的形式，方便比较
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

        // 两个predicate的表达式区间，相反位置的表达式都有交集，eg. this_predicate: [a,b]; predicate: (c, d], 那么要求[a和d]有交集并且(c和b]有交集
        if(single_expr_intersection_check(this->left_operation_type_, this->left_value_, op_right, value_right) &&
                single_expr_intersection_check(this->operation_type_, this->right_value_, op_left, value_left))
            return true;
        return false;
    }

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
