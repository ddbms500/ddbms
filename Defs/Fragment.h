#ifndef DDBMS_FRAGMENT_H
#define DDBMS_FRAGMENT_H

#include "Predicate.h"

enum class FragmentType {HORIZONTAL, VERTICAL};

class Fragment {
public:
    Fragment() {}

    // horizontal fragment
    Fragment(const std::string& fragment_name, FragmentType fragment_type, const std::string& site_name)
    : fragment_name_(fragment_name), fragment_type_(fragment_type), site_name_(site_name) {}

    std::string fragment_name_;
    FragmentType fragment_type_;
    std::string site_name_;
    // predicates 按照属性名称和类型做好排序
    std::vector<Predicate> predicates_;
    int record_count_;
    std::vector<std::string> attributes_;
};

#endif