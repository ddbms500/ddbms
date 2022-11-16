#include <iostream>
#include <mysql++/mysql++.h>
#include <iomanip>
#include <stdio.h>
#include <string.h>
#include <typeinfo>
#include <cxxabi.h>
using namespace std;

int _main(mysqlpp::Connection conn,string tablename)
{
    try{
        //mysqlpp::Connection conn("test10","127.0.0.1","wyl","123456");
        string str = "select * from "+tablename+ " limit 1";
        cout<<str<<endl;
        //mysqlpp::Query query = conn.query(str);
        mysqlpp::Query query = conn.query("select * from "+tablename+" limit 1");
        mysqlpp::StoreQueryResult res = query.store();
        char widths[]={12,22,46};
        cout.setf(ios::left);
        cout << setw(widths[0]) << "Field" 
             << setw(widths[1]) << "SQL Type"
             << setw(widths[2]) << "Equivalent C++ Type" 
             << endl;
        for(size_t i=0;i<sizeof(widths)/sizeof(widths[0]);++i){
            cout << string(widths[i]-1,'=') << ' ';
        }//输出分割线
        cout << endl;
        for(size_t i=0;i<res.field_names()->size();i++){
            const char * cname =
                abi::__cxa_demangle(res.field_type(int(i)).name(),0,0,0);
            /*const char * cname = 
                abi::__cxa_demangle(typeid(res.field_type(int(i))).name(),0,0,0);
                */
            mysqlpp::FieldTypes::value_type ft =res.field_type(int(i));
            ostringstream os;
            os << ft.sql_name() << " (" << ft.id() <<')';
            cout << setw(widths[0]) << res.field_name(int(i)).c_str() 
                 << setw(widths[1]) << os.str()
                 << setw(widths[2]) << cname
                 << endl;
        }
        cout<<endl;
        if(res.field_type(1) == typeid(string)){
            cout << "第二列的SQL属性在C++中用string表示" << endl;
        }
    }catch(const mysqlpp::BadQuery& er){
        cerr<<"查询失败："<<er.what()<<endl;
        return -1;
 
    }catch(const mysqlpp::Exception& er){
        cerr<<"错误："<<er.what()<<endl;
        return -1;
    }

    return 0;
}

int main(){
    mysqlpp::Connection conn("test10","127.0.0.1","wyl","123456");
    _main(conn,"girls");
    return 0;
}
