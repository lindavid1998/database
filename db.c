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

void print_constants()
{
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_AVAILABLE_CELL_SPACE: %d\n", LEAF_NODE_AVAILABLE_CELL_SPACE);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

typedef enum
{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

typedef struct
{
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
    uint32_t root_page_idx; // identifies root node?
    Pager *pager;
} Table;

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

/* Describes a position in a Table */
typedef struct
{
    Table *table;
    uint32_t page_idx;
    uint32_t cell_idx;
    bool end_of_table; // Indicates a position one past the last element
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

/* Returns pointer to num cells in a leaf node */
uint32_t *leaf_node_num_cells(void *node)
{
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

/* Returns pointer to cell at cell_idx of leaf node */
void *leaf_node_cell(void *node, uint32_t cell_idx)
{
    return node + LEAF_NODE_HEADER_SIZE + cell_idx * LEAF_NODE_CELL_SIZE;
}

/* Returns pointer to key at cell_idx of leaf node */
void *leaf_node_key(void *node, uint32_t cell_idx)
{
    return leaf_node_cell(node, cell_idx);
}

/* Returns pointer to value at cell_idx of leaf node */
void *leaf_node_value(void *node, uint32_t cell_idx)
{
    return leaf_node_cell(node, cell_idx) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node)
{
    // set the number of cells to 0
    *leaf_node_num_cells(node) = 0;
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

        // get number of pages
        uint32_t num_pages = pager->file_length / PAGE_SIZE; // full pages
        if (pager->file_length % PAGE_SIZE)
        {
            num_pages += 1; // add 1 for partial page
        }

        // Set the file offset to wherever page_idx is
        // SEEK_SET -> the file offset is set to offset (page_idx * PAGE_SIZE) bytes
        lseek(pager->file_descriptor, page_idx * PAGE_SIZE, SEEK_SET);

        // Read file into allocated page
        ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

        // update number of pages if accessing beyond current number of pages
        if (page_idx >= pager->num_pages)
        {
            pager->num_pages = page_idx + 1;
        }
    }

    return pager->pages[page_idx];
}

/* Initializes a cursor pointing to start of a table */
Cursor *init_cursor_table_start(Table *table)
{
    // malloc a new cursor object
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));

    // initialize properties of cursor
    cursor->table = table;
    cursor->cell_idx = 0;
    cursor->page_idx = table->root_page_idx;

    void *root_node = get_page_address(table->pager, table->root_page_idx);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0); // start is also end of table if there are no cells

    return cursor;
}

/* Initializes a cursor pointing to end of a table (just past the last cell index) */
Cursor *init_cursor_table_end(Table *table)
{
    // malloc a new cursor object
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));

    // initialize properties of cursor
    cursor->table = table;
    cursor->page_idx = table->root_page_idx;

    void *root_node = get_page_address(table->pager, table->root_page_idx);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_idx = num_cells;
    cursor->end_of_table = true;

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
    if (file_length % PAGE_SIZE != 0)
    {
        printf("Db file is not whole number of pages. Corrupted file.\n");
        exit(EXIT_FAILURE);
    }

    // allocate memory and init properties for pager
    Pager *pager = (Pager *)malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;
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
    // uint32_t num_rows = pager->file_length / ROW_SIZE;

    // init table
    Table *table = (Table *)malloc(sizeof(Table));
    table->root_page_idx = 0; // set page 0 to be root
    table->pager = pager;

    if (pager->num_pages == 0)
    {
        // new database file. init page 0 as leaf
        void *root_node = get_page_address(pager, table->root_page_idx);
        initialize_leaf_node(root_node);
    }

    return table;
}

/*
Writes PAGE_SIZE bytes from leaf node at pages[page_idx] to file
*/
void flush_page(Pager *pager, uint32_t page_idx)
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
    ssize_t bytes_written = write(fd, pager->pages[page_idx], PAGE_SIZE);
    if (bytes_written == -1)
    {
        printf("Error writing page to file\n");
        exit(EXIT_FAILURE);
    }
}

void close_db(Table *table)
{
    Pager *pager = table->pager;

    // flush pages and free memory
    for (int i = 0; i < pager->num_pages; i++)
    {
        void *page = pager->pages[i];
        if (page)
        {
            flush_page(pager, i);
            free(page);
            pager->pages[i] = NULL;
        }
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
    else if (strcmp(input_buffer->buffer, ".constants") == 0)
    {
        print_constants();
        return META_COMMAND_SUCCESS;
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
Convert cursor object to memory address described by cursor
*/
void *cursor_value(Cursor *cursor)
{
    Table *table = cursor->table;

    // Get node address //
    void *node = get_page_address(table->pager, cursor->page_idx);

    return leaf_node_value(node, cursor->cell_idx);
}

/* Advance cursor to next cell */
void advance_cursor(Cursor *cursor)
{
    uint32_t page_idx = cursor->page_idx;
    void *node = get_page_address(cursor->table->pager, page_idx);
    cursor->cell_idx += 1;

    if (cursor->cell_idx >= *leaf_node_num_cells(node))
    {
        // wouldn't this only be true if
        // the cursor was pointing to the rightmost node?
        // unless we are assuming right now that theres only 1 leaf node
        cursor->end_of_table = true;
    }
}

/*
Inserts a key value pair at position represented by cursor into a leaf node
*/
void leaf_node_insert_cell(Cursor *cursor, uint32_t key, Row *value)
{
    // get address of node pointed by cursor
    Pager *pager = cursor->table->pager;
    void *node = get_page_address(pager, cursor->page_idx);

    // get number of cells currently in leaf node
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS)
    {
        // leaf node is full, need to split node
        printf("Need to implement node splitting\n");
        exit(EXIT_FAILURE);
    }

    // shift cells over if not inserting at end of node
    if (cursor->cell_idx < num_cells)
    {
        for (uint32_t i = num_cells; i > cursor->cell_idx; i--)
        {
            // copy cell[i-1] into cell[i]
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE); // dest, src, num bytes
        }
    }

    // insert key and value into cell
    *(uint32_t *)(leaf_node_key(node, cursor->cell_idx)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_idx));

    // increment num cells in leaf node
    *(leaf_node_num_cells(node)) += 1;
}

ExecuteResult execute_insert(Table *table, Statement *statement)
{
    // for now, table is represented as a single leaf node

    // get addr of root leaf node
    void *node = get_page_address(table->pager, table->root_page_idx);
    
    if (*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)
    {
        // table is full
        return EXECUTE_STATEMENT_TABLE_FULL;
    }

    // create cursor at end of table
    Cursor *cursor = init_cursor_table_end(table);

    // insert cell
    Row *row_to_insert = &(statement->row_to_insert);
    uint32_t key = row_to_insert->id;
    leaf_node_insert_cell(cursor, key, row_to_insert);

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
        deserialize_row(cursor_value(cursor), &row);
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
                continue;
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