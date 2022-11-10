#ifndef DDBMS_ATTRIBUTE_H
#define DDBMS_ATTRIBUTE_H


enum class AttributeType {INTEGER, CHAR, VARCHAR, TEXT};

class Attribute {
public:
    std::string name;
    AttributeType type;
    bool is_primary_key;
};

#endif