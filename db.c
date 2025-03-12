#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100

// #define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
// const uint32_t ID_SIZE = size_of_attribute(Row, id);
// const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
// const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_SIZE = 4;        // bytes
const uint32_t USERNAME_SIZE = 32; // bytes
const uint32_t EMAIL_SIZE = 255;   // bytes
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096; // bytes
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

// Common node header layout
// Contains: node type, is root, pointer to parent
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Additional headers for leaf nodes
// Contains: number of key-value pairs (aka cells)
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Body layout for leaf nodes
// Leaf nodes contains keys and values (rows)
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t); // what exactly are we using as keys? the row id?
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_AVAILABLE_CELL_SPACE = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_AVAILABLE_CELL_SPACE / LEAF_NODE_CELL_SIZE;

typedef enum
{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

typedef struct
{
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
    uint32_t num_rows;
    Pager *pager;
} Table;

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef struct
{
    Table *table;
    uint32_t row_idx;
    bool end_of_table;
} Cursor;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum
{
    PREPARE_STATEMENT_SUCCESS,
    PREPARE_STATEMENT_UNRECOGNIZED_COMMAND,
    PREPARE_STATEMENT_SYNTAX_ERROR
    // PREPARE_STRING_TOO_LONG,
} PrepareResult;

typedef enum
{
    EXECUTE_STATEMENT_SUCCESS,
    EXECUTE_STATEMENT_TABLE_FULL,
    EXECUTE_STATEMENT_ERROR
} ExecuteResult;
typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;
typedef struct
{
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

/* Initializes a cursor pointing to start of a table */
Cursor *init_cursor_table_start(Table *table)
{
    // malloc a new cursor object
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));

    // initialize properties of cursor
    cursor->table = table;
    cursor->row_idx = 0;
    cursor->end_of_table = false;

    // return cursor
    return cursor;
}

/* Initializes a cursor pointing to end of a table */
Cursor *init_cursor_table_end(Table *table)
{
    // malloc a new cursor object
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));

    // initialize properties of cursor
    cursor->table = table;
    cursor->row_idx = table->num_rows;
    cursor->end_of_table = true;

    // return cursor
    return cursor;
}

/* Initializes Pager struct */
Pager *open_pager(const char *filename)
{
    // open file and get fd
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        printf("Error opening file\n");
        exit(EXIT_FAILURE); // error handling
    }

    // get file length
    off_t file_length = lseek(fd, 0, SEEK_END); // get offset of file end?
    if (file_length == -1)
    {
        printf("Error getting file length\n");
        exit(EXIT_FAILURE);
    }

    // allocate memory and init properties for pager
    Pager *pager = (Pager *)malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }

    // return address for pager
    return pager;
}

/*
Open db connection with input file
- Init pager and table
- Load file into pager
- Load pager in table
*/
Table *open_db(const char *filename)
{
    // init pager
    Pager *pager = open_pager(filename);

    // since we are loading the db file, num_rows in table depends on how much data is already in the db file
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    // init table
    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = num_rows;
    table->pager = pager;

    return table;
}

/*
Writes size bytes from page at pages[page_idx] to file

I think specifying size as a parameter is useful if writing partial page to file
*/
void flush_page(Pager *pager, uint32_t page_idx, uint32_t size)
{
    int fd = pager->file_descriptor;

    // set file offset
    off_t offset = page_idx * PAGE_SIZE;
    lseek(fd, offset, SEEK_SET);
    if (offset == -1)
    {
        printf("Error seeking\n");
        exit(EXIT_FAILURE);
    }

    // write bytes to file
    ssize_t bytes_written = write(fd, pager->pages[page_idx], size);
    if (bytes_written == -1)
    {
        printf("Error writing page to file\n");
        exit(EXIT_FAILURE);
    }
}

void close_db(Table *table)
{
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    // flush pages and free memory
    for (int i = 0; i < num_full_pages; i++)
    {
        void *page = pager->pages[i];
        if (page)
        {
            flush_page(pager, i, PAGE_SIZE);
            free(page);
            pager->pages[i] = NULL;
        }
    }

    // flush partial page
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0 && pager->pages[num_full_pages])
    {
        // how many bytes to write? number of extra rows x row size?
        flush_page(pager, num_full_pages, num_additional_rows * ROW_SIZE);
        free(pager->pages[num_full_pages]);
        pager->pages[num_full_pages] = NULL;
    }

    close(pager->file_descriptor); // close file

    free(pager);
    free(table);
}

/*
Init InputBuffer object
*/
InputBuffer *new_input_buffer()
{
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer *input_buffer)
{
    ssize_t bytes_read =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0)
    {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

bool StartsWith(const char *a, const char *b)
{
    return strncmp(a, b, strlen(b)) == 0;
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buffer);
        close_db(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

/*
Serialize row by copying row contents into destination byte array
*/
void serialize_row(Row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

/*
Deserialize row by copying destination byte array contents into Row struct
*/
void deserialize_row(void *source, Row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}
/*
Parses input and constructs Statement
*/
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    if (StartsWith(input_buffer->buffer, "SELECT"))
    {
        // handle input starting with SELECT
        statement->type = STATEMENT_SELECT;
        return PREPARE_STATEMENT_SUCCESS;
    }

    if (StartsWith(input_buffer->buffer, "INSERT"))
    {
        // handle input starting with INSERT
        statement->type = STATEMENT_INSERT;

        // parse "insert 1 cstack foo@bar.com" -> id, username, email
        int inputs_matched = sscanf(input_buffer->buffer, "INSERT %d %s %s",
                                    &(statement->row_to_insert.id),
                                    statement->row_to_insert.username,
                                    statement->row_to_insert.email);

        if (inputs_matched != 3)
        {
            return PREPARE_STATEMENT_SYNTAX_ERROR;
        }

        return PREPARE_STATEMENT_SUCCESS;
    }

    // all other inputs, return not recognized
    return PREPARE_STATEMENT_UNRECOGNIZED_COMMAND;
}

/*
Get the address of page based on its index
Allocates memory for page and loads data from file if it doesn't exist in memory yet
*/
void *get_page_address(Pager *pager, uint32_t page_idx)
{
    if (page_idx >= TABLE_MAX_PAGES)
    {
        printf("Tried to fetch page idx out of bounds.");
        exit(EXIT_FAILURE);
    }

    void *page = pager->pages[page_idx];
    if (page == NULL)
    {
        // cache miss, allocate memory for page using PAGE_SIZE
        page = pager->pages[page_idx] = malloc(PAGE_SIZE);

        // uint32_t num_pages = pager->file_length / PAGE_SIZE; // number of full pages
        // // add 1 for partial page
        // if (pager->file_length % PAGE_SIZE)
        // {
        //     num_pages += 1;
        // }

        // Set the file offset to wherever page_idx is
        // SEEK_SET -> the file offset is set to offset (page_idx * PAGE_SIZE) bytes
        lseek(pager->file_descriptor, page_idx * PAGE_SIZE, SEEK_SET);

        // Read file into allocated page
        ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
    }

    return pager->pages[page_idx];
}

/*
Convert cursor object to memory address described by cursor
*/
void *get_row_address(Cursor *cursor)
{
    Table *table = cursor->table;
    Pager *pager = table->pager;

    // Get page address //
    uint32_t page_index = cursor->row_idx / ROWS_PER_PAGE;
    void *page = get_page_address(pager, page_index);

    // Get page offset //
    // to get offset, figure out which row idx on the page, then convert to bytes
    uint32_t offset = (cursor->row_idx % ROWS_PER_PAGE) * ROW_SIZE;

    return page + offset;
}

/* Advance cursor by one row */
void advance_cursor(Cursor *cursor)
{
    cursor->row_idx += 1;

    // why >=? why not just ==?
    if (cursor->row_idx >= cursor->table->num_rows)
    {
        cursor->end_of_table = true;
    }
}

ExecuteResult execute_insert(Table *table, Statement *statement)
{
    // check if table is full
    if (table->num_rows == TABLE_MAX_ROWS)
    {
        return EXECUTE_STATEMENT_TABLE_FULL;
    }

    // create cursor at end of table
    Cursor *cursor = init_cursor_table_end(table);

    // get address to insert row
    void *row_addr = get_row_address(cursor);

    // insert serialized row into address
    serialize_row(&(statement->row_to_insert), row_addr);

    // increment table->num_rows by 1
    table->num_rows += 1;

    free(cursor);

    return EXECUTE_STATEMENT_SUCCESS;
}

void print_row(Row *row)
{
    printf("%d %s %s\n", row->id, row->username, row->email);
}

ExecuteResult execute_select(Table *table, Statement *statement)
{
    // print all rows
    Row row;

    // init cursor at start of table
    Cursor *cursor = init_cursor_table_start(table);

    // for each row, deserialize and print
    while (!(cursor->end_of_table))
    {
        deserialize_row(get_row_address(cursor), &row);
        print_row(&row);
        advance_cursor(cursor);
    }

    free(cursor);

    return EXECUTE_STATEMENT_SUCCESS;
}

ExecuteResult execute_statement(Table *table, Statement *statement)
{
    switch (statement->type)
    {
    case (STATEMENT_INSERT):
        return execute_insert(table, statement);
    case (STATEMENT_SELECT):
        return execute_select(table, statement);
    }
}

/* Returns pointer to num cells in a leaf node */
uint32_t *leaf_node_num_cells(void *node)
{
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

/* Returns pointer to cell at cell_num of leaf node */
void *leaf_node_cell(void *node, uint32_t cell_num)
{
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

/* Returns pointer to key at cell_num of leaf node */
void *leaf_node_key(void *node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num);
}

/* Returns pointer to value at cell_num of leaf node */
void *leaf_node_value(void *node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node)
{
    // set the number of cells to 0
    *leaf_node_num_cells(node) = 0;
}

int main(int argc, char *argv[])
{
    const char *filename = "data.db"; // TODO: replace with command line argument
    InputBuffer *input_buffer = new_input_buffer();
    Table *table = open_db(filename);

    while (true)
    {
        print_prompt();
        read_input(input_buffer);

        // printf("Command: '%s'.\n", input_buffer->buffer);

        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer, table))
            {
            case (META_COMMAND_SUCCESS):
                break;
            case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("Unrecognized command '%s'.\n", input_buffer->buffer);
                continue;
            }
        }

        // prepare result by converting input to Statement
        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case (PREPARE_STATEMENT_SUCCESS):
            break;
        // case (PREPARE_STRING_TOO_LONG):
        //     printf("Input string is too long.\n");
        //     continue;
        case (PREPARE_STATEMENT_SYNTAX_ERROR):
            printf("Syntax error in statement '%s'.\n", input_buffer->buffer);
            continue;
        case (PREPARE_STATEMENT_UNRECOGNIZED_COMMAND):
            printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
            continue;
        }

        // execute Statement
        switch (execute_statement(table, &statement))
        {
        case (EXECUTE_STATEMENT_SUCCESS):
            printf("Executed.\n");
            break;
        case (EXECUTE_STATEMENT_TABLE_FULL):
            printf("Failed to insert, table is full.\n");
            continue;
        case (EXECUTE_STATEMENT_ERROR):
            printf("Error executing statement, please retry.\n");
        }
    }
}