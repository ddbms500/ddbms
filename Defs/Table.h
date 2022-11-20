#ifndef DDBMS_TABLE_H
#define DDBMS_TABLE_H
#include "Attribute.h"
#include "Fragment.h"

class Table {
public:
    std::string table_name_;
    std::vector<Attribute> attributes_;
    std::vector<Fragment> fragments_;
};

#endif