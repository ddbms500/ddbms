#ifndef DDBMS_ATTRIBUTE_H
#define DDBMS_ATTRIBUTE_H


enum class AttributeType {INTEGER, CHAR, TEXT};

class Attribute {
public:
    Attribute(const std::string& attr_name, AttributeType type, int data_length, bool is_primary_key = false)
    : attr_name_(attr_name), type_(type), data_length_(data_length), is_primary_key_(is_primary_key) {}

    std::string attr_name_;
    AttributeType type_;
    int data_length_;
    bool is_primary_key_ = false;
};

#endif