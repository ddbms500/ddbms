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

    bool is_in_fragment(std::vector<std::string>* record) {
        // used for horizontal fragment
        for(int i = 0; i < predicates_.size(); ++i) {
            std::cout << "predicate " << i << "predicate attr index: " << predicate_attr_index_[i] << std::endl;
            std::cout << record->at(predicate_attr_index_[i]);
            // 如果record的该value在对应predicate范围内，继续判断下一个predicate，否则返回不属于
            if(!predicates_[i].is_satisfy_predicate(record->at(predicate_attr_index_[i])))
                return false;
        }
        return true;
    }

    // 判断单个predicate是否和当前分片有交集，如果有交集，返回true，否则返回false
    bool have_intersection(const Predicate& predicate) {
        for(auto fragment_predicate = this->predicates_.begin(); fragment_predicate != this->predicates_.end(); ++fragment_predicate) {
            if(predicate.attribute_name_.compare(fragment_predicate->attribute_name_) == 0) {
                return fragment_predicate->have_intersection(predicate);
            }
        }

        // 分片的predicates中不包含要查找的predicate的属性名称，因此每个分片中允许含有符合当前predicate的record，因此返回true
        return true;
    }

    void init(std::vector<Attribute>* attrs) {
        if(fragment_type_ == FragmentType::HORIZONTAL) {
            for(auto predicate: predicates_) {
                for(int i = 0; i < attrs->size(); ++i) {
                    if(predicate.attribute_name_.compare(attrs->at(i).attr_name_) == 0) {
                        predicate_attr_index_.emplace_back(i);
                        break;
                    }
                }
            }
        }
        else {
            for(auto attr : attributes_) {
                for(int i = 0; i < attrs->size(); ++i) {
                    if(attr.compare(attrs->at(i).attr_name_) == 0) {
                        vertical_attr_index_.emplace_back(i);
                        break;
                    }
                }
            }
        }
    }

    bool is_attr_in_fragment(std::string attr_name) {
        for(auto attr : attributes_) {
            if(attr.compare(attr_name) == 0) 
                return true;
        }
        return false;
    }

    std::string fragment_name_;
    FragmentType fragment_type_;
    std::string site_name_;
    // predicates 按照属性名称和类型做好排序
    std::vector<Predicate> predicates_;
    // 记录predicates中每个attr对应的属性在表的meta_data里的index，这样当插入一个record的时候，可以快速判断该record是否属于当前fragment
    std::vector<int> predicate_attr_index_;
    int record_count_;
    std::vector<std::string> attributes_;
    // 记录竖直分片情况下每个attr对应的属性在表的meta_data里的index，这样可以快速构建需要向每个分片插入的record
    std::vector<int> vertical_attr_index_;
};

#endif