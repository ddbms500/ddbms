#include "MetaData.h"

bool MetaData::add_site(std::string name, std::string ip, std::string port) {

    return true;
}

bool MetaData::add_db(std::string db_name) {

    return true;
}

bool MetaData::use_db(std::string db_name) {

    return true;
}

bool MetaData::add_table(std::string table_name, std::vector<Attribute> attributes) {

    return true;
}

bool MetaData::add_fragment(Fragment fragment) {

    return true;
}

bool MetaData::allocate(std::string fragment_name, std::string site_name) {

    return true;
}

Table MetaData::get_table(std::string table_name) {
    Table table = Table();

    return table;
}

void MetaData::show_sites() {

}

void MetaData::show_databases() {

}

void MetaData::show_tables() {

}

void MetaData::show_fragments() {

}

std::unordered_map<std::string, std::string> MetaData::get_insert_location() {
    std::unordered_map<std::string, std::string> map{};

    return map;
}