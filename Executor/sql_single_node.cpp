#include <iostream>
#include <iomanip>
#include <mysql++/mysql++.h>
#include <stdio.h>
#include <typeinfo>
#include <cxxabi.h>
#include <string>
using namespace std;

bool sql_table_structure(mysqlpp::Connection conn,string table_name){
    try{
        mysqlpp::Query query = 
            conn.query("select * from "+table_name+" limit 1");
        mysqlpp::StoreQueryResult res = query.store();
        char widths[]={12,22};
        cout.setf(ios::left);
        cout<<table_name+ "的属性名和类型为:"<<endl 
            <<"============================"<<endl
            <<setw(widths[0]) << "Field"
            <<setw(widths[1]) << "SQL Type"
            <<endl;
        for(size_t i=0;i<sizeof(widths)/sizeof(widths[0]);++i){
            cout<<string(widths[i]-1,'-') << '|';
        }
        cout<<endl;
        for(size_t i=0;i<res.field_names()->size();i++){
            mysqlpp::FieldTypes::value_type ft = res.field_type(int(i));
            ostringstream os;
            os << ft.sql_name() << " (" << ft.id()<<')';
            cout<<setw(widths[0])<<res.field_name(int(i)).c_str()
                <<setw(widths[1])<<os.str()<<endl;
        }
        cout<<"============================"<<endl;
    }catch(const mysqlpp::BadQuery& er){
        cout<<"在查询"<<table_name<<"表的结构时出错:"<<er.what()<<endl;
        return false;
    }catch(const mysqlpp::Exception& er){
        cout<<"sql_table_structure()错误:"<<er.what()<<endl;
        return false;
    }
    return true;
}

bool SQL_select(mysqlpp::Connection conn,string sql){
    try{
        mysqlpp::Query query = conn.query();
        query << sql.c_str();//可能标红但其实没错
        mysqlpp::StoreQueryResult res = query.store();
        cout<<"查询结果为:"<<endl;
        int c_nums=res.field_names()->size();
        for(size_t i=0;i<c_nums;i++){//输出列名
            cout.setf(ios::left);
            cout << setw(14) << res.field_name(int(i)).c_str()<<"|";
        }
        cout<<endl;
        mysqlpp::StoreQueryResult::const_iterator it;
        for(it = res.begin();it!=res.end();++it){
            mysqlpp::Row row = *it;
            for(int i=0;i<c_nums&&i<row.size();i++){
                cout<< setw(14) << row[i] <<"|";
            }
            cout<<endl;
        }
        cout<<endl;    
    }catch(const mysqlpp::Exception& er){
        cout<<"sql执行错误:"<<er.what()<<endl;
        return false;
    }
    return true;
}

bool SQL_insert(mysqlpp::Connection conn,string sql){
    try{
        mysqlpp::Query query= conn.query();
        query << sql.c_str();
        mysqlpp::SimpleResult res=query.execute();
        cout<<"插入成功";
        if(strlen(res.info())!=0){
            cout<<",返回为:"<<res.info()<<endl;
        }
    }catch(const mysqlpp::Exception& er){
        cout<<"sql执行错误:"<<er.what()<<endl;
        return false;
    }
    cout<<endl;
    return true;
}

bool SQL_delete(mysqlpp::Connection conn,string sql){
    try{
        mysqlpp::Query query= conn.query();
        query << sql.c_str();
        mysqlpp::SimpleResult res=query.execute();
        cout<<"删除成功";
        if(strlen(res.info())!=0){
            cout<<",返回为:"<<res.info()<<endl;
        }
    }catch(const mysqlpp::Exception& er){
        cout<<"sql执行错误:"<<er.what()<<endl;
        return false;
    }
    cout<<endl;
    return true;
}

bool SQL_update(mysqlpp::Connection conn,string sql){
    try{
        cout<<"进入SQL_update成功"<<endl;
        mysqlpp::Query query= conn.query();
        query << sql.c_str();
        mysqlpp::SimpleResult res=query.execute();
        cout<<"更新成功";
        if(strlen(res.info())!=0){
            cout<<",返回为:"<<res.info()<<endl;
        }
    }catch(const mysqlpp::Exception& er){
        cout<<"sql执行错误:"<<er.what()<<endl;
        return false;
    }
    cout<<endl;
    return true;
}

int main()
{
    try{
        mysqlpp::Connection conn("test10","127.0.0.1","wyl","123456");
        mysqlpp::Query query = conn.query("show tables");
        //Tables_in_test10
        if(mysqlpp::UseQueryResult res = query.use()){
            cout.setf(ios::left);
            cout<<"============================"<<endl;
            cout<< "数据库test10中目前有表:"<<endl;
            while(mysqlpp::Row row = res.fetch_row()){
                cout<<setw(10)<<row["Tables_in_test10"]<<endl;
            }
            cout<<"============================"<<endl;
            for(int i=0;i<1;i++){
                cout<<"请输入选择使用的数据库表:";
                string table_name;
                getline(cin,table_name);
                if(!sql_table_structure(conn,table_name)) i--;
            }
            for(int i=0;i<1;i++){
                cout<<"请输入想要执行的sql语句(select insert delete update exit):"<<endl;
                string op;
                getline(cin,op);
                const char*point=strstr(op.c_str(),"select");
                if(point!=NULL && point == op.c_str()){
                    i--;
                    SQL_select(conn,op);
                }else{
                    point = strstr(op.c_str(),"insert");
                    if(point!=NULL && point==op.c_str()){
                        i--;
                        SQL_insert(conn,op);
                    }else{
                        point = strstr(op.c_str(),"delete");
                        if(point!=NULL && point==op.c_str()){
                            i--;
                            SQL_delete(conn,op);
                        }else{
                            point = strstr(op.c_str(),"update");
                            if(point!=NULL && point==op.c_str()){
                                i--;
                                SQL_update(conn,op);
                            }else{
                                point = strstr(op.c_str(),"exit");
                                if(point!=NULL && point==op.c_str()){
                                    cout<<"Good Bye"<<endl;
                                }else{
                                    i--;
                                    cout<<"未定义的操作类型(sql前不要有空格)"<<endl;
                                }
                            }
                        }
                    }
                }
            }
        }else{
            cout<<"查询\"show tables\"报错："<<endl<<query.error()<<endl;
            return -1;
        }
    }catch(const mysqlpp::Exception& er){
        cout<<"错误:"<<er.what()<<endl;
    }
    return 0;
}

