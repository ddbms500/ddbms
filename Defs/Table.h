#ifndef DDBMS_TABLE_H
#define DDBMS_TABLE_H
#include "Attribute.h"
#include "Fragment.h"

class Table {
public:
    Attribute get_attr(std::string attr_name) {
        for(int i = 0; i < attributes_.size(); ++i) {
            if(attributes_[i].attr_name_.compare(attr_name) == 0)
                return attributes_[i];
        }
        return attributes_[0];
    }
    std::string table_name_;
    std::vector<Attribute> attributes_;
    std::vector<Fragment> fragments_;
    FragmentType table_fragment_type_;
    // 如果primary_key_index = -1，代表该表没有主键，否则代表主键对应的属性在attributes的vector中的index
    int primary_key_index_ = 0;
};

#endif