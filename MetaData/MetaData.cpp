#include "MetaData.h"

void MetaData::init() {
    site_map.emplace("site1", std::make_pair("192.168.3.1", "1200"));
    site_map.emplace("site2", std::make_pair("192.168.3.2", "1200"));
    site_map.emplace("site3", std::make_pair("192.168.3.3", "1200"));
    site_map.emplace("site4", std::make_pair("192.168.3.4", "1200"));

    Table publisher;
    publisher.table_name_ = "Publisher";
    publisher.table_fragment_type_ = FragmentType::HORIZONTAL;
    table_index.emplace(publisher.table_name_, 0);
    Attribute publisher_id("id", AttributeType::INTEGER, sizeof(int), true);
    publisher.attributes_.emplace_back(publisher_id);
    Attribute publisher_name("name", AttributeType::CHAR, 100);
    publisher.attributes_.emplace_back(publisher_name);
    Attribute publisher_nation("nation", AttributeType::CHAR, 3);
    publisher.attributes_.emplace_back(publisher_nation);

    Fragment publisher_fragment_1("Publisher.1", FragmentType::HORIZONTAL, "site1");
    Predicate publisher_1_predicate_1("id", OperationType::LESS, "10400");
    Predicate publisher_1_predicate_2("nation", OperationType::EQUAL, "PRC");
    publisher_fragment_1.predicates_.emplace_back(publisher_1_predicate_1);
    publisher_fragment_1.predicates_.emplace_back(publisher_1_predicate_2);
    publisher.fragments_.emplace_back(publisher_fragment_1);

    Fragment publisher_fragment_2("Publisher.2", FragmentType::HORIZONTAL, "site2");
    Predicate publisher_2_predicate_1("id", OperationType::LESS, "10400");
    Predicate publisher_2_predicate_2("nation", OperationType::EQUAL, "USA");
    publisher_fragment_2.predicates_.emplace_back(publisher_2_predicate_1);
    publisher_fragment_2.predicates_.emplace_back(publisher_2_predicate_2);
    publisher.fragments_.emplace_back(publisher_fragment_2);

    Fragment publisher_fragment_3("Publisher.3", FragmentType::HORIZONTAL, "site3");
    Predicate publisher_3_predicate_1("id", OperationType::GREAT_EQUAL, "10400");
    Predicate publisher_3_predicate_2("nation", OperationType::EQUAL, "PRC");
    publisher_fragment_3.predicates_.push_back(publisher_3_predicate_1);
    publisher_fragment_3.predicates_.push_back(publisher_3_predicate_2);
    publisher.fragments_.emplace_back(publisher_fragment_3);

    Fragment publisher_fragment_4("Publisher.4", FragmentType::HORIZONTAL, "site4");
    Predicate publisher_4_predicate_1("id", OperationType::GREAT_EQUAL, "10400");
    Predicate publisher_4_predicate_2("nation", OperationType::EQUAL, "USA");
    publisher_fragment_4.predicates_.push_back(publisher_4_predicate_1);
    publisher_fragment_4.predicates_.push_back(publisher_4_predicate_2);
    publisher.fragments_.emplace_back(publisher_fragment_4);

    tables.emplace_back(publisher);

    Table book;
    book.table_name_ = "Book";
    book.table_fragment_type_ = FragmentType::HORIZONTAL;
    table_index.emplace(book.table_name_, 1);
    Attribute book_id("id", AttributeType::INTEGER, sizeof(int), true);
    book.attributes_.emplace_back(book_id);
    Attribute book_title("title", AttributeType::CHAR, 100);
    book.attributes_.push_back(book_title);
    Attribute book_authors("authors", AttributeType::CHAR, 200);
    book.attributes_.push_back(book_authors);
    Attribute book_publisher_id("publisher_id", AttributeType::INTEGER, sizeof(int));
    book.attributes_.push_back(book_publisher_id);
    Attribute book_copies("copies", AttributeType::INTEGER, sizeof(int));
    book.attributes_.push_back(book_copies);

    Fragment book_fragment_1("Book.1", FragmentType::HORIZONTAL, "site1");
    Predicate book_1_predicate_1("id", OperationType::LESS, "205000");
    book_fragment_1.predicates_.push_back(book_1_predicate_1);
    book.fragments_.emplace_back(book_fragment_1);

    Fragment  book_fragment_2("Book.2", FragmentType::HORIZONTAL, "site2");
    Predicate book_2_predicate_1("id", OperationType::GREAT_EQUAL, "205000");
    book_2_predicate_1.left_operation_type_ = OperationType::GREAT;
    book_2_predicate_1.left_value_ = "210000";
    book_fragment_2.predicates_.push_back(book_2_predicate_1);
    book.fragments_.emplace_back(book_fragment_2);

    Fragment book_fragment_3("Book.3", FragmentType::HORIZONTAL, "site3");
    Predicate book_3_predicate_1("id", OperationType::GREAT_EQUAL, "210000");
    book_fragment_3.predicates_.push_back(book_3_predicate_1);
    book.fragments_.emplace_back(book_fragment_3);

    tables.emplace_back(book);

    Table customer;
    customer.table_name_ = "Customer";
    customer.table_fragment_type_ = FragmentType::VERTICAL;
    table_index.emplace(customer.table_name_, 2);
    Attribute customer_id("id", AttributeType::INTEGER, sizeof(int), true);
    customer.attributes_.push_back(customer_id);
    Attribute customer_name("name", AttributeType::CHAR, 25);
    customer.attributes_.push_back(customer_name);
    Attribute customer_rank("rank", AttributeType::INTEGER, sizeof(int));
    customer.attributes_.push_back(customer_rank);

    Fragment customer_fragment_1("Customer.1", FragmentType::VERTICAL, "site1");
    customer_fragment_1.attributes_.push_back(customer_id.attr_name_);
    customer_fragment_1.attributes_.push_back(customer_name.attr_name_);
    customer.fragments_.emplace_back(customer_fragment_1);

    Fragment customer_fragment_2("Customer.2", FragmentType::VERTICAL, "site2");
    customer_fragment_2.attributes_.push_back(customer_id.attr_name_);
    customer_fragment_2.attributes_.push_back(customer_rank.attr_name_);
    customer.fragments_.emplace_back(customer_fragment_2);

    tables.emplace_back(customer);

    Table orders;
    orders.table_name_ = "Orders";
    orders.table_fragment_type_ = FragmentType::HORIZONTAL;
    table_index.emplace(orders.table_name_, 3);
    Attribute orders_customer_id("customer_id", AttributeType::INTEGER, sizeof(int));
    orders.attributes_.push_back(orders_customer_id);
    Attribute orders_book_id("book_id", AttributeType::INTEGER, sizeof(int));
    orders.attributes_.push_back(orders_book_id);
    Attribute orders_quantity("quantity", AttributeType::INTEGER, sizeof(int));
    orders.attributes_.push_back(orders_quantity);

    Fragment orders_fragment_1("Orders.1", FragmentType::HORIZONTAL, "site1");
    Predicate orders_1_predicate_1("customer_id", OperationType::LESS, "307000");
    Predicate orders_1_predicate_2("book_id", OperationType::LESS, "215000");
    orders_fragment_1.predicates_.push_back(orders_1_predicate_1);
    orders_fragment_1.predicates_.push_back(orders_1_predicate_2);
    orders.fragments_.emplace_back(orders_fragment_1);

    Fragment orders_fragment_2("Orders.2", FragmentType::HORIZONTAL, "site2");
    Predicate orders_2_predicate_1("customer_id", OperationType::LESS, "307000");
    Predicate orders_2_predicate_2("book_id", OperationType::GREAT_EQUAL, "215000");
    orders_fragment_2.predicates_.push_back(orders_2_predicate_1);
    orders_fragment_2.predicates_.push_back(orders_2_predicate_2);
    orders.fragments_.emplace_back(orders_fragment_2);

    Fragment orders_fragment_3("Orders.3", FragmentType::HORIZONTAL, "site3");
    Predicate orders_3_predicate_1("customer_id", OperationType::GREAT_EQUAL, "307000");
    Predicate orders_3_predicate_2("book_id", OperationType::LESS, "215000");
    orders_fragment_3.predicates_.push_back(orders_3_predicate_1);
    orders_fragment_3.predicates_.push_back(orders_3_predicate_2);
    orders.fragments_.emplace_back(orders_fragment_3);

    Fragment orders_fragment_4("Orders.4", FragmentType::HORIZONTAL, "site4");
    Predicate orders_4_predicate_1("customer_id", OperationType::GREAT_EQUAL, "307000");
    Predicate orders_4_predicate_2("book_id", OperationType::GREAT_EQUAL, "215000");
    orders_fragment_4.predicates_.push_back(orders_4_predicate_1);
    orders_fragment_4.predicates_.push_back(orders_4_predicate_2);
    orders.fragments_.emplace_back(orders_fragment_4);

    tables.emplace_back(orders);
}

bool MetaData::add_site(std::string name, std::string ip, std::string port) {

    return true;
}

bool MetaData::add_db(std::string db_name) {

    return true;
}

bool MetaData::drop_db(std::string db_name) {

    return true;
}

bool MetaData::use_db(std::string db_name) {

    return true;
}

bool MetaData::add_table(std::string table_name_, std::vector<Attribute> attributes_) {

    return true;
}

bool MetaData::drop_table(std::string table_name_) {

}

bool MetaData::add_fragment(Fragment fragment) {

    return true;
}

bool MetaData::allocate(std::string fragment_name, std::string site_name) {

    return true;
}

Table* MetaData::get_table(std::string table_name) {
    if(table_index.find(table_name) == table_index.end()) {
        return nullptr;
    }

    int index = table_index.find(table_name)->second;

    if(index >= tables.size()) return nullptr;

    return &(tables[index]);
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