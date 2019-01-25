#include "parser.h"

int main(int argc, char **argv)
{
    void **file_buffers;

    file_buffers = get_file_buffers(argv[1], 0);
    free_file_buffers(file_buffers);

    return 0;
}
