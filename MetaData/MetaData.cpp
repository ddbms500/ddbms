#include "MetaData.h"


void MetaData::init() {
    site_map.emplace("site1", std::make_pair("192.168.3.1", "1200"));
    site_map.emplace("site2", std::make_pair("192.168.3.2", "1200"));
    site_map.emplace("site3", std::make_pair("192.168.3.3", "1200"));
    site_map.emplace("site4", std::make_pair("192.168.3.4", "1200"));

    Table publisher;
    publisher.table_name_ = "Publisher";
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

//已检查
bool MetaData::add_site(std::string name, std::string ip, std::string port) {
    //实际情况是四个站点的ip和端口全都是固定的
    //ip固定为172.25.32.1(/2/3/4)
    //brpc端口固定为8800，etcd端口固定为2379、2380
    etcd.set("/sites","");
    pplx::task<etcd::Response> response_task = etcd.set("/sites/"+name,ip+":"+port);
    try{
        etcd::Response response = response_task.get();
        if(response.is_ok()){//为了使etcd保存的站点与本地一致，先设置etcd，成功的情况下设置本地
            site_map.emplace(name, std::make_pair(ip,port));
            return true;
        }else{
            std::cout<<"MetaData::add_site() failed,details: "<<response.error_message()<<std::endl;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }
    return false;
}

//已检查
bool MetaData::add_db(std::string db_name) {
    //对于像db_name这样只需要存储名称的，将db_name按照key存储，value设为长度为0的字符串
    pplx::task<etcd::Response> response_task = etcd.add("/dbs/"+db_name,"");
    try{
        etcd::Response response = response_task.get();
        if(response.is_ok()){
            return true;
        }else{
            std::cout<<"MetaData::add_db() failed,details: "<<response.error_message()<<std::endl;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }
    return false;
}

//已检查
bool MetaData::drop_db(std::string db_name) {
    pplx::task<etcd::Response> response_task = etcd.get("/dbs/"+db_name);
    try{
        etcd::Response response = response_task.get();
        if(!response.is_ok()){//如果db_name在etcd并没有存储，就会进入该分支
            std::cout<<"MetaData::drop_db() failed,may bacause of using nonexistent db_name,details:"
                     <<response.error_message()<<std::endl;
            return false;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }
    //删除etcd存储的以"/dbs/db_name/"为前缀的所有key
    etcd.rmdir("/dbs/"+dbname+"/",true).wait();
    //删除etcd存储的"/dbs/db_name"
    etcd.rmdir("/dbs/"+dbname,false).wait();
    return true;
}

//已检查
bool MetaData::use_db(std::string db_name) {
    //在调用了use_db(db_name)后所有默认的sql语句都是在db_name对应的数据库执行
    pplx::task<etcd::Response> response_task = etcd.get("/dbs/"+db_name);
    try{
        etcd::Response response = response_task.get();
        if(response.is_ok()){//etcd中存储了该db_name
            default_db_name=db_name;
            return true;
        }else{//如果db_name在etcd并没有存储，就会进入该分支
            std::cout<<"MetaData::use_db() failed,may bacause of using nonexistent db_name,details:"
                     <<response.error_message()<<std::endl;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }
    return false;
}

//已检查
bool MetaData::add_table(std::string table_name_, std::vector<Attribute> attributes_) {

    const std::string attribute_type_str[] = {"INTEGER","CHAR","TEXT"};

    string prefix="/dbs/"+default_db_name+"/"+table_name_+"/attributes/";//目录结构为/dbs/db_name/table_name/attributes/...

    etcd.set("/dbs/"+default_db_name+"/"+table_name_,"");
    etcd.set("/dbs/"+default_db_name+"/"+table_name_+"/attributes","");

    for(int i=0;i<attributes.size();i++){
        etcd.set(prefix+attributes_[i].attr_name_,"");
        std::string type_str = attribute_type_str[static_cast<int>(attributes_[i].type_)];
        etcd.set(prefix+attributes_[i].attr_name_+"/type",type_str);
        etcd.set(prefix+attributes_[i].attr_name_+"/data_length",std::to_string(attributes_[i].data_length_));
        if(attributes_[i].is_primary_key_){
            etcd.set(prefix+attributes_[i].attr_name_+"/is_primary_key","true");
        }else{
            etcd.set(prefix+attributes_[i].attr_name_+"/is_primary_key","false");
        }
    }
    return true;
}

//已检查
bool MetaData::drop_table(std::string table_name_) {
    pplx::task<etcd::Response> response_task = etcd.get("/dbs/"+default_db_name+"/"+table_name_);
    try{
        etcd::Response response = response_task.get();
        if(!response.is_ok()){//如果table_name_在etcd并没有存储，就会进入该分支
            std::cout<<"MetaData::drop_table() failed,may bacause of using nonexistent table_name,details:"
                     <<response.error_message()<<std::endl;
            return false;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }

    /*删除四个节点下的该表的分片信息 */
    etcd::Response resp=etcd.ls("/sites/").get();
    int location=-1;
    for(int i=0;i<resp.keys().size();i++){
        location=get_location_of_str(resp.keys(i),3);
        if(location==-1){//当前key为 /sites/site_name
            etcd.rmdir(resp.keys(i)+"/fragments/"+table_name_,true).wait();//这里默认了分片名前缀为表名
        }
    }
    /*删除子目录 */
    etcd.rmdir("/dbs/"+default_db_name+"/"+table_name_+"/",true).wait();
    /*删除/dbs/db_name/table_name */
    etcd.rmdir("/dbs/"+default_db_name+"/"+table_name_,false).wait();
    return true;
}

//已检查
bool MetaData::add_fragment(std::string table_name_,Fragment fragment) {

    const std::string fragment_type_str[] = {"HORIZONTAL","VERTICAL"};
    const std::string operationTypeStr_[] = {"JOIN", "EQUAL", "LESS", "LESS_EQUAL", "GREAT", "GREAT_EQUAL", "DEFAULT"};

    etcd.set("/dbs/"+default_db_name+"/"+table_name_+"/fragments","");
    etcd.set("/dbs/"+default_db_name+"/"+table_name_+"/fragments/"+fragment.fragment_name_,"");

    string prefix="/dbs/"+default_db_name+"/"+table_name_+"/fragments/"+fragment.fragment_name_+"/";//目录结构为/dbs/db_name/table_name/fragments/frag_name/...
    etcd.set(prefix+"type",fragment_type_str[static_cast<int>{fragment.fragment_type_}]);
    etcd.set(prefix+"record_count",std::to_string(fragment.record_count_));
    etcd.set(prefix+"site_name","");
    etcd.set(prefix+"attributes","");
    for(int i=0;i<fragment.attributes_.size();i++){
        etcd.set(prefix+"attributes/"+fragment.attributes_[i],"");
    }
    etcd.set(prefix+"predicates","");
    for(int i=0;i<fragment.predicates_.size();i++){
        std::string prefix_predicate=prefix+"predicates/"+std::to_string(i)+"/";
        etcd.set(prefix+"predicates/"+std::to_string(i),"");
        etcd.set(prefix_predicate+"attribute_name",fragment.predicate_[i].attribute_name_);
        etcd.set(prefix_predicate+"table_name",fragment.predicate_[i].table_name_);
        etcd.set(prefix_predicate+"right_value",fragment.predicate_[i].right_value_);
        etcd.set(prefix_predicate+"left_value",fragment.predicate_[i].left_value_);
        etcd.set(prefix_predicate+"right_attribute_name",fragment.predicate_[i].right_attribute_name_);
        etcd.set(prefix_predicate+"right_table_name",fragment.predicate_[i].right_table_name_);
        etcd.set(prefix_predicate+"operation_type",operationTypeStr_[static_cast<int>{fragment.predicate_[i].operation_type_}]);
        etcd.set(prefix_predicate+"left_operation_type",operationTypeStr_[static_cast<int>{fragment.predicate_[i].left_operation_type_}]);
    }
    return true;
}

//已检查
bool MetaData::allocate(std::string fragment_name, std::string site_name, std::string table_name) {
    etcd.set("/sites/"+site_name+"/fragments","");
    etcd.set("/sites/"+site_name+"/fragments/"+fragment_name,"");
    etcd.set("/dbs/"+default_db_name+"/"+table_name+"/fragments/"+fragment_name+"/site_name",site_name);
    return true;
}

//已检查
Table MetaData::get_table(std::string table_name_) {
    pplx::task<etcd::Response> response_task = etcd.get("/dbs/"+default_db_name+"/"+table_name_);
    try{
        etcd::Response response0 = response_task.get();
        if(!response0.is_ok()){//如果db_name在etcd并没有存储，就会进入该分支
            std::cout<<"MetaData::get_table() shoudl faile,bacause of using nonexistent table_name,details:"
                     <<response0.error_message()<<std::endl;
            //在这里处理table_name_在etcd中并不存在的情况
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }

    Table table = Table();
    table.table_name_=table_name_;
    /*处理table/attributes*/
    //获取以"/dbs/db_name/table_name/attributes/"为前缀的所有kv
    etcd::Response response=etcd.ls("/dbs/"+default_db_name+"/"+table_name+"/attributes/").get();
    int location1=get_location_of_str("/dbs/"+default_db_name+"/"+table_name+"/attributes/",5);
    for(int i=0;i<response.keys().size();i++){
        int location2=get_location_of_str(response.keys(i),6);
        if(location2==-1){//说明当前key是 /dbs/db_name/table_name/attributes/attr_name
            std::string prefix_attribute=response.keys(i);// /dbs/db_name/table_name/attributes/attr_name
            Attribute attribute;
            attribute.attr_name_=prefix_attribute.substr(location1+1,prefix_attribute.size()-location1-1);
            etcd::Response temp_resp=etcd.get(prefix_attribute+"/data_length").get();
            attribute.data_length_=atoi(temp_resp.value().as_string(),0,10);//将value字符串从0位置开始转换为10进制的int
            temp_resp=etcd.get(prefix_attribute+"/is_primary_key").get();
            if(temp_resp.value().as_string()=="true"){
                attribute.is_primary_key_=true;
            }else if(temp_resp.value().as_string()=="false"){
                attribute.is_primary_key_=false;
            }else{
                std::cout<<"MetaData::get_table() error:"<<prefix_attribute
                         <<"/is_primary_key store value neither true nor false"<<std::endl;
            }
            temp_resp=etcd.get(prefix_attribute+"/type").get();
            string attr_type=temp_resp.value().as_string();
            if(attr_type=="INTEGER"){
                attribute.type_=AttributeType::INTEGER;
            }else if(attr_type=="CHAR"){
                attribute.type_=AttributeType::CHAR;
            }else if(attr_type=="TEXT"){
                attribute.type_=AttributeType::TEXT;
            }else{
                std::cout<<"MetaData::get_table() error:"<<prefix_attribute
                         <<"/attr_type store value not in {INTEGER,CHAR,TEXT},it is"
                         <<attr_type<<std::endl;
            }
            //处理完一个table的attribute
            table.attributes_.push_back(attribute);
        }
    }

    /*处理table/fragments*/
    //获取以"/dbs/db_name/table_name/fragments/"为前缀的所有kv
    response=etcd.ls("/dbs/"+default_db_name+"/"+table_name+"/fragments/").get();
    location1=get_location_of_str("/dbs/"+default_db_name+"/"+table_name+"/fragments/",5);
    for(int i=0;i<response.keys().size();i++){
        int location2=get_location_of_str(response.keys(i),6);
        if(location2==-1){//说明当前key是 /dbs/db_name/table_name/fragments/frag_name
            std::string prefix_frag=response.keys(i);// /dbs/db_name/table_name/fragments/frag_name
            Fragment fragment;
            fragment.fragment_name_=prefix_frag.substr(location1+1,prefix_frag.size()-location1-1);
            etcd::Response temp_resp=etcd.get(prefix_frag+"/record_count").get();
            fragment.record_count_=atoi(temp_resp.value().as_string(),0,10);//将string转换为10进制int
            temp_resp=etcd.get(prefix_frag+"/type").get();
            if(temp_resp.value().as_string()=="HORIZONTAL"){
                fragment.fragment_type_=FragmentType::HORIZONTAL;
            }else if(temp_resp.value().as_string()=="VERTICAL"){
                fragment.fragment_type_=FragmentType::VERTICAL;
            }else{
                std::cout<<"MetaData::get_table() error:"<<prefix_frag
                         <<"/type store value neither HORIZONTAL nor VERTICAL,it is"
                         <<temp_resp.value().as_string()<<std::endl;
            }
            temp_resp=etcd.get(prefix_frag+"/site_name").get();
            if(temp_resp.value().as_string()==""){
            }else{
                fragment.site_name_=temp_resp.value().as_string();
            }
            /*处理fragment的predicates*/
            int location3=prefix_frag.size()+strlen("predicates/");// 路径“/dbs/db_name/table_name/fragments/frag_name/predicates/”的最后一个'/'的位置
            etcd::Response resp4predicate = etcd.ls(prefix_frag+"/predicates/").get();
            for(int j=0;j<resp4predicate.keys().size();j++){
                int location4=get_location_of_str(resp4predicate.keys(j),8);
                if(location4==-1){//当前key为 "/dbs/db_name/table_name/fragments/frag_name/predicates/predicate索引号"
                    std::string prefix_predicate=resp4predicate.keys(j);//
                    Predicate predicate;
                    etcd::Response temp_resp_4_predicate=etcd.get(prefix_predicate+"/attribute_name").get();
                    predicate.attribute_name_=temp_resp_4_predicate.value().as_string();
                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/table_name").get();
                    predicate.table_name_=temp_resp_4_predicate.value().as_string();
                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/right_value").get();
                    predicate.right_value_=temp_resp_4_predicate.value().as_string();
                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/left_value").get();
                    predicate.left_value_=temp_resp_4_predicate.value().as_string();
                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/right_attribute_name").get();
                    predicate.right_attribute_name_=temp_resp_4_predicate.value().as_string();
                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/right_table__name").get();
                    predicate.right_table_name_=temp_resp_4_predicate.value().as_string();

                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/operation_type").get();
                    if(temp_resp_4_predicate.value().as_string()=="JOIN"){
                        predicate.operation_type_=OperationType::JOIN;
                    }else if(temp_resp_4_predicate.value().as_string()=="EQUAL"){
                        predicate.operation_type_=OperationType::EQUAL;
                    }else if(temp_resp_4_predicate.value().as_string()=="LESS"){
                        predicate.operation_type_=OperationType::LESS;
                    }else if(temp_resp_4_predicate.value().as_string()=="LESS_EQUAL"){
                        predicate.operation_type_=OperationType::LESS_EQUAL;
                    }else if(temp_resp_4_predicate.value().as_string()=="GREAT"){
                        predicate.operation_type_=OperationType::GREAT;
                    }else if(temp_resp_4_predicate.value().as_string()=="DEFAULT"){
                        predicate.operation_type_=OperationType::DEFAULT;
                    }else{
                        std::cout<<"MetaData::get_table() error:"<<prefix_predicate<<"/operation_type store:"
                                 <<temp_resp_4_predicate.value().as_string()<<endl;
                    }

                    temp_resp_4_predicate=etcd.get(prefix_predicate+"/left_operation_type").get();
                    if(temp_resp_4_predicate.value().as_string()=="JOIN"){
                        predicate.left_operation_type_=OperationType::JOIN;
                    }else if(temp_resp_4_predicate.value().as_string()=="EQUAL"){
                        predicate.left_operation_type_=OperationType::EQUAL;
                    }else if(temp_resp_4_predicate.value().as_string()=="LESS"){
                        predicate.left_operation_type_=OperationType::LESS;
                    }else if(temp_resp_4_predicate.value().as_string()=="LESS_EQUAL"){
                        predicate.left_operation_type_=OperationType::LESS_EQUAL;
                    }else if(temp_resp_4_predicate.value().as_string()=="GREAT"){
                        predicate.left_operation_type_=OperationType::GREAT;
                    }else if(temp_resp_4_predicate.value().as_string()=="DEFAULT"){
                        predicate.left_operation_type_=OperationType::DEFAULT;
                    }else{
                        std::cout<<"MetaData::get_table() error:"<<prefix_predicate<<"/left_operation_type store"
                                 <<temp_resp_4_predicate.value().as_string()<<endl;
                    }
                    //处理完一个fragment的predicate
                    fragment.predicate_.push_back(predicate);
                }

            }
            /*处理fragment/attibutes*/
            etcd::Response temp_resp_4_attribute=etcd.ls(prefix_frag+"/attribute/").get();
            for(int j=0;j<temp_resp_4_attribute.keys().size();j++){
                std::string temp_str_attr=temp_resp_4_attribute.keys(i);
                int location_temp = get_location_of_str(temp_str_attr,7);
                std::string temp_str_attr_name=temp_str_attr(location_temp+1,temp_str_attr.size()-location_temp-1);
                fragment.attibutes_.push_back(temp_str_attr_name);
            }
            //处理完一个table的fragment
            table.fragments_.push_back(fragment);
        }
    }
    return table;
}

//已检查
void MetaData::show_sites() {
    etcd::Response response=etcd.ls("/sites/").get();
    int location1=-1;
    for(int i=0;i<response.keys().size();i++){
        location1=get_location_of_str(response.keys(i),3);
        if(location1==-1){//当前key为 /sites/site_name
            int location2=get_location_of_str(response.keys(i),2)
            std::string site_name=response.keys(i).substr(location2+1,response.keys(i).size()-location2-1);
        }
    }
}

//已检查
void MetaData::show_databases() {
    etcd::Response response=etcd.ls("/dbs/").get();
    int location1=-1;
    for(int i=0;i<response.keys().size();i++){
        location1=get_location_of_str(response.keys(i),3);
        if(location1==-1){//当前key为 /dbs/db_name
            int location2=get_location_of_str(response.keys(i),2)
            std::string db_name=response.keys(i).substr(location2+1,response.keys(i).size()-location2-1);
        }
    }
}

//已检查
void MetaData::show_tables() {
    etcd::Response response=etcd.ls("/dbs/").get();
    int location1=-1;
    for(int i=0;i<response.keys().size();i++){
        location1=get_location_of_str(response.keys(i),3);
        if(location1==-1){//当前key为 /dbs/db_name
            etcd::Response response1=etcd.ls(response.keys(i)+"/");//查找以"/dbs/db_name/"为前缀的所有key
            location1=response.keys(i).size();// “/dbs/db_name/”第三个'/'的位置
            for(int j=0;j<response1.keys().size();j++){
                int location2=get_location_of_str(response1.keys(i),4);
                if(location2==-1){// 当前key为 /dbs/db_name/table_name
                    std::string table_name=response1.keys(j).substr(location1+1,response1.keys(j).size()-location1-1);
                }
            }
        }
    }
}

//已检查
void MetaData::show_fragments() {
    etcd::Response response=etcd.ls("/sites/").get();
    for(int i=0;i<response.keys().size();i++){
        string key=response.keys(i);
        int location1=get_location_of_str(key,5);
        if(location1!=-1){//说明当前key为: /sites/site_name/fragments/frag_name
            int location2=get_location_of_str(key,4);
            string frag_name=key.substr(location2+1,key.size()-location2-1);
        }
    }
}

bool MetaData::write_query_tree_to_etcd(QueryTree query_tree){
    const std::string node_type_str[] = {"PROJECTION","SELECTION","JOIN","UNION"};
    const std::string operationTypeStr_[] = {"JOIN", "EQUAL", "LESS", "LESS_EQUAL", "GREAT", "GREAT_EQUAL", "DEFAULT"};

    etcd.set("/query_tree","");
    etcd.set("/query_tree/"+std::to_string(query_tree.root_index),"");
    std::string prefix="/query_tree/"+std::to_string(query_tree.root_index)+"/";
    etcd.set(prefix+"n",std::to_string(query_tree.n));
    etcd.set(prefix+"nodes","");
    for(int i=0;i<query_tree.n;i++){
        std::string prefix_node=prefix+"nodes/"+std::to_string(i);
        etcd.set(prefix_node,"");
        prefix_node=prefix_node+"/";
        string node_type=node_type_str[static_cast<int>{query_tree.nodes[i].node_type_}];
        etcd.set(prefix_node+"node_type",node_type);
        etcd.set(prefix_node+"site",query_tree.nodes[i].site_);
        etcd.set(prefix_node+"table_name",query_tree.nodes[i].table_name_);
        /*处理vector<string>attr_name_*/
        etcd.set(prefix_node+"attributes","");
        for(int j=0;j<query_tree.nodes[i].attr_name_().size();j++){
            etcd.set(prefix_node+"attributes/"+query_tree.nodes[i].attr_name_[j],"");
        }
        /*处理vector<Predicate>select_predicates_*/
        etcd.set(prefix_node+"select_predicates","");
        for(int j=0;j<query_tree.nodes[i].select_predicates_().size();j++){
            std::string prefix_predicate=prefix_node+"select_predicates/"+std::to_string(i)+"/";
            etcd.set(prefix_node+"select_predicates/"+std::to_string(i),"");
            etcd.set(prefix_predicate+"attribute_name",query_tree.nodes[i].select_predicates_[j].attribute_name_);
            etcd.set(prefix_predicate+"table_name",query_tree.nodes[i].select_predicates_[j].table_name_);
            etcd.set(prefix_predicate+"right_value",query_tree.nodes[i].select_predicates_[j].right_value_);
            etcd.set(prefix_predicate+"left_value",query_tree.nodes[i].select_predicates_[j].left_value_);
            etcd.set(prefix_predicate+"right_attribute_name",query_tree.nodes[i].select_predicates_[j].right_attribute_name_);
            etcd.set(prefix_predicate+"right_table_name",query_tree.nodes[i].select_predicates_[j].right_table_name_);
            etcd.set(prefix_predicate+"operation_type",operationTypeStr_[static_cast<int>{query_tree.nodes[i].select_predicates_[j].operation_type_}]);
            etcd.set(prefix_predicate+"left_operation_type",operationTypeStr_[static_cast<int>{query_tree.nodes[i].select_predicates_[j].left_operation_type_}]);
        }
        /*处理vector<Predicate>join_predicate_*/
        etcd.set(prefix_node+"join_predicate","");
        std::string prefix_predicate=prefix_node+"join_predicate/"+std::to_string(i)+"/";
        etcd.set(prefix_node+"join_predicate/"+std::to_string(i),"");
        etcd.set(prefix_predicate+"attribute_name",query_tree.nodes[i].join_predicate_.attribute_name_);
        etcd.set(prefix_predicate+"table_name",query_tree.nodes[i].join_predicate_.table_name_);
        etcd.set(prefix_predicate+"right_value",query_tree.nodes[i].join_predicate_.right_value_);
        etcd.set(prefix_predicate+"left_value",query_tree.nodes[i].join_predicate_.left_value_);
        etcd.set(prefix_predicate+"right_attribute_name",query_tree.nodes[i].join_predicate_.right_attribute_name_);
        etcd.set(prefix_predicate+"right_table_name",query_tree.nodes[i].join_predicate_.right_table_name_);
        etcd.set(prefix_predicate+"operation_type",operationTypeStr_[static_cast<int>{query_tree.nodes[i].join_predicate_.operation_type_}]);
        etcd.set(prefix_predicate+"left_operation_type",operationTypeStr_[static_cast<int>{query_tree.nodes[i].join_predicate_.left_operation_type_}]);

        etcd.set(prefix_node+"parent",std:to_string(query_tree.nodes[i].parent_));
        /*处理vector<int>sons_*/
        etcd.set(prefix_node+"sons","");
        for(int j=0;j<query_tree.nodes[i].sons_().size();j++){
            etcd.set(prefix_node+"sons"+std:to_string(query_tree.nodes[i].sons_[j]));
        }

        if(query_tree.nodes[i].is_parent_){
            etcd.set(prefix_node+"is_parent","true");
        }else{
            etcd.set(prefix_node+"is_parent","false");
        }
        if(query_tree.nodes[i].is_leaf_){
            etcd.set(prefix_node+"is_leaf","true");
        }else{
            etcd.set(prefix_node+"is_leaf","false");
        }
    }
}

QueryTree Metadata::read_query_tree_from_etcd(int root_id){
    pplx::task<etcd::Response> response_task = etcd.get("/query_tree/"+std::to_string(root_id));
    try{
        etcd::Response response = response_task.get();
        if(!response.is_ok()){//如果root_id在etcd并没有存储，就会进入该分支
            std::cout<<"MetaData::read_query_tree_from_etcd() failed,may bacause of using nonexistent root_id,details:"
                     <<response.error_message()<<std::endl;
            return false;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }
    std::string prefix="/query_tree/"+std::to_string(root_id)+"/";
    QueryTree query_tree = QueryTree();
    etcd::Response resp=etcd.get(prefix+"n").get();
    query_tree.root_index=root_id;
    query_tree.n=stoi(resp.value().as_string(),0,10);//将字符串转换为10进制int
    /*处理nodes[]*/
    int location1=get_location_of_str(prefix+"nodes/",4);// "/query_tree/root_index/nodes/"第四个'/'的位置
    etcd::Response response_node=etcd.ls(prefix+"nodes/").get();
    for(int i=0;j<response_node.keys().size();i++){
        int location2=get_location_of_str(response_node.keys[i],"5");
        if(location2==-1){//该key为 /query_tree/root_index/nodes/node[]索引号
            int node_index=stoi(response_node.keys[i].substr(location1+1,response_node.keys[i].size()-location1-1),0,10);//字符串转换为int
            QueryNode query_node=QueryNode();
            std::string prefix_node=response_node.keys[i]+"/";
            /*处理node_type_*/
            etcd::Response response_temp=etcd.get(prefix_node+"node_type").get();
            if(response_temp.value().as_string()=="PROJECTION"){
                query_node.node_type_=NodeType::PROJECTION;
            }else if(response_temp.value().as_string()=="SELECTION"){
                query_node.node_type_=NodeType::SELECTION;
            }else if(response_temp.value().as_string()=="JOIN"){
                query_node.node_type_=NodeType::JOIN;
            }else if(response_temp.value().as_string()=="UNION"){
                query_node.node_type_=NodeType::UNION;
            }else{
                std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                         <<prefix_node<<"node_type store value is"<<response_temp.value().as_string()
                         <<", which is not in {PROJECTION,SELECTION,JOIN,UNION}"<<std::endl;
            }
            query_node.site_=etcd.get(prefix_node+"site").get().value().as_string();
            query_node.table_name_=etcd.get(prefix_node+"table_name").get().value().as_string();
            /*处理attr_name_*/
            response_temp=etcd.ls(prefix_node+"attributes/").get();
            int location3=get_location_of_str(prefix_node+"attributes/",6);// /query_tree/root_index/nodes/索引号/attributes/
            for(int j=0;j<response_temp.keys().size();j++){
                query_node.attr_name_().push_back(response_temp.keys[j].substr(location3+1,response_temp.keys[j].size()-location3-1));
            }
            /*处理select_predicates_*/
            std::string prefix_select_predicates=prefix_node+"select_predicates/";
            location3=get_location_of_str(prefix_select_predicates,6);// /query_tree/root_index/nodes/索引号/select_predicates/
            response_temp=etcd.ls(prefix_select_predicates).get();
            for(int j=0;j<response_temp.keys().size();j++){
                if(get_location_of_str(response_temp.keys[j],7)==-1){
                    prefix_select_predicates=response_temp.keys[j]+"/";
                    Predicate predicate=Predicate();
                    predicate.attribute_name_=etcd.get(prefix_select_predicates+"attribute_name").get().value().as_string();
                    predicate.table_name_=etcd.get(prefix_select_predicates+"table_name").get().value().as_string();
                    predicate.right_value_=etcd.get(prefix_select_predicates+"right_value").get().value().as_string();
                    predicate.right_table_name_=etcd.get(prefix_select_predicates+"right_table_name").get().value().as_string();
                    std::string operation_type=etcd.get(prefix_select_predicates+"operation_type").get().value().as_string();
                    if(operation_type=="JOIN"){
                        predicate.operation_type_=OperationType::JOIN;
                    }else if(operation_type=="EQUAL"){
                        predicate.operation_type_=OperationType::EQUAL;
                    }else if(operation_type=="LESS"){
                        predicate.operation_type_=OperationType::LESS;
                    }else if(operation_type=="LESS_EQUAL"){
                        predicate.operation_type_=OperationType::LESS_EQUAL;
                    }else if(operation_type=="GREAT"){
                        predicate.operation_type_=OperationType::GREAT;
                    }else if(operation_type=="GREAT_EQUAL"){
                        predicate.operation_type_=OperationType::GREAT_EQUAL;
                    }else if(operation_type=="DEFAULT"){
                        predicate.operation_type_=OperationType::DEFAULT;
                    }else{
                        std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                                 <<prefix_select_predicates<<"opeation_type store value is"<<opeation_type
                                 <<", which is not in {JOIN,EQUAL,LESS_EQUAL,GREAT,GREAT_EQUAL,DEFAULT}"<<std::endl;
                    }
                    operation_type=etcd.get(prefix_select_predicates+"left_operation_type").get().value().as_string();
                    if(operation_type=="JOIN"){
                        predicate.left_operation_type_=OperationType::JOIN;
                    }else if(operation_type=="EQUAL"){
                        predicate.left_operation_type_=OperationType::EQUAL;
                    }else if(operation_type=="LESS"){
                        predicate.left_operation_type_=OperationType::LESS;
                    }else if(operation_type=="LESS_EQUAL"){
                        predicate.left_operation_type_=OperationType::LESS_EQUAL;
                    }else if(operation_type=="GREAT"){
                        predicate.left_operation_type_=OperationType::GREAT;
                    }else if(operation_type=="GREAT_EQUAL"){
                        predicate.left_operation_type_=OperationType::GREAT_EQUAL;
                    }else if(operation_type=="DEFAULT"){
                        predicate.left_operation_type_=OperationType::DEFAULT;
                    }else{
                        std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                                 <<prefix_select_predicates<<"opeation_type store value is"<<opeation_type
                                 <<", which is not in {JOIN,EQUAL,LESS_EQUAL,GREAT,GREAT_EQUAL,DEFAULT}"<<std::endl;
                    }
                    query_node.select_predicates_().push_back(predicate);

                }
            }
            /*处理join_predicate_*/
            std::string prefix_join_predicate=prefix_node+"join_predicate/";
            query_node.join_predicate_.attribute_name_=etcd.get(prefix_select_predicates+"attribute_name").get().value().as_string();
            query_node.join_predicate_.table_name_=etcd.get(prefix_join_predicate+"table_name").get().value().as_string();
            query_node.join_predicate_.right_value_=etcd.get(prefix_join_predicate+"right_value").get().value().as_string();
            query_node.join_predicate_.right_table_name_=etcd.get(prefix_join_predicate+"right_table_name").get().value().as_string();
            std::string operation_type=etcd.get(prefix_join_predicate+"operation_type").get().value().as_string();
            if(operation_type=="JOIN"){
                query_node.join_predicate_.operation_type_=OperationType::JOIN;
            }else if(operation_type=="EQUAL"){
                query_node.join_predicate_.operation_type_=OperationType::EQUAL;
            }else if(operation_type=="LESS"){
                query_node.join_predicate_.operation_type_=OperationType::LESS;
            }else if(operation_type=="LESS_EQUAL"){
                query_node.join_predicate_.operation_type_=OperationType::LESS_EQUAL;
            }else if(operation_type=="GREAT"){
                query_node.join_predicate_.operation_type_=OperationType::GREAT;
            }else if(operation_type=="GREAT_EQUAL"){
                query_node.join_predicate_.operation_type_=OperationType::GREAT_EQUAL;
            }else if(operation_type=="DEFAULT"){
                query_node.join_predicate_.operation_type_=OperationType::DEFAULT;
            }else{
                std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                         <<prefix_join_predicate<<"opeation_type store value is"<<opeation_type
                         <<", which is not in {JOIN,EQUAL,LESS_EQUAL,GREAT,GREAT_EQUAL,DEFAULT}"<<std::endl;
            }
            operation_type=etcd.get(prefix_join_predicate+"left_operation_type").get().value().as_string();
            if(operation_type=="JOIN"){
                query_node.join_predicate_.left_operation_type_=OperationType::JOIN;
            }else if(operation_type=="EQUAL"){
                query_node.join_predicate_.left_operation_type_=OperationType::EQUAL;
            }else if(operation_type=="LESS"){
                query_node.join_predicate_.left_operation_type_=OperationType::LESS;
            }else if(operation_type=="LESS_EQUAL"){
                query_node.join_predicate_.left_operation_type_=OperationType::LESS_EQUAL;
            }else if(operation_type=="GREAT"){
                query_node.join_predicate_.left_operation_type_=OperationType::GREAT;
            }else if(operation_type=="GREAT_EQUAL"){
                query_node.join_predicate_.left_operation_type_=OperationType::GREAT_EQUAL;
            }else if(operation_type=="DEFAULT"){
                query_node.join_predicate_.left_operation_type_=OperationType::DEFAULT;
            }else{
                std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                         <<prefix_join_predicate<<"opeation_type store value is"<<opeation_type
                         <<", which is not in {JOIN,EQUAL,LESS_EQUAL,GREAT,GREAT_EQUAL,DEFAULT}"<<std::endl;
            }
            query_node.parent_=stoi(etcd.get(prefix_node+"parent").get().value().as_string(),0,10);
            /*处理sons_*/
            response_temp=etcd.ls(prefix_node+"sons/").get();
            location3=get_location_of_str(prefix_node+"sons/",6);// /query_tree/root_index/nodes/索引号/sons/
            for(int j=0;j<response_temp.keys().size();j++){
                std::string str_son=response_temp.keys[j].substr(location3+1,response_temp.keys[j].size()-location3-1);
                query_node.sons_().push_back(stoi(str_son,0,10));
            }
            if(etcd.get(prefix_node+"is_parent").get().value().as_string()=="true"){
                query_node.is_parent_="true";
            }else if(etcd.get(prefix_node+"is_parent").get().value().as_string()=="false"){
                query_node.is_parent="false";
            }else{
                std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                         <<prefix_node<<"is_parent store value is"
                         <<etcd.get(prefix_node+"is_parent").get().value().as_string()
                         <<std::endl;
            }
            if(etcd.get(prefix_node+"is_leaf").get().value().as_string()=="true"){
                query_node.is_leaf_="true";
            }else if(etcd.get(prefix_node+"is_leaf").get().value().as_string()=="false"){
                query_node.is_leaf_="false";
            }else{
                std::cout<<"MetaData::read_query_tree_from_etcd() error:"
                         <<prefix_node<<"is_leaf store value is"
                         <<etcd.get(prefix_node+"is_leaf").get().value().as_string()
                         <<std::endl;
            }
            query_tree.nodes[node_index]=query_node;
        }
    }
    return query_tree;
}

bool MetaData::delete_query_tree_from_etcd(int root_id){
    pplx::task<etcd::Response> response_task = etcd.get("/query_tree/"+std::to_string(root_id));
    try{
        etcd::Response response = response_task.get();
        if(!response.is_ok()){//如果root_id在etcd并没有存储，就会进入该分支
            std::cout<<"MetaData::delete_query_tree_from_etcd() failed,may bacause of using nonexistent root_id,details:"
                     <<response.error_message()<<std::endl;
            return false;
        }
    }catch(std::exception const & ex){
        std::cerr << "communication problem,details: "<<ex.what();
    }
    etcd.rmdir("/query_tree/"+std::to_string(root_id),true).wait();
    return true;
}