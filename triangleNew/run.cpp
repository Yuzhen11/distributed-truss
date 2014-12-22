#include "pregel_triangle.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_tri(argv[1], argv[2]);
    worker_finalize();
    return 0;
}
