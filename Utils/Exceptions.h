#ifndef DDBMS_EXCEPTION_H
#define DDBMS_EXCEPTION_H

#include <exception>
#include <iostream>

class InvalidCommandException: public std::exception{
public:
    InvalidCommandException(std::string msg) : message("InvalidCommandException: " + msg) {}
    const char* what() const throw() { return message.c_str(); }
private:
    std::string message;
};

#endif