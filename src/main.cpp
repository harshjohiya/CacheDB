#include "server.hpp"

int main()
{
    Server server(6379);
    server.start();
    return 0;
}