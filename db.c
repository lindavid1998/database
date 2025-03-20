#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
#define INVALID_PAGE_IDX UINT32_MAX

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
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

// Body layout for leaf nodes
// Leaf nodes contains keys and values (rows)
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t); // what exactly are we using as keys? the row id?
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_AVAILABLE_CELL_SPACE = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_AVAILABLE_CELL_SPACE / LEAF_NODE_CELL_SIZE;

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/*
Internal node header layout
Contains: num_keys, right_child pointer
*/
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
Internal node body layout
The body is an array of cells where each cell contains a child pointer and a key.
Every key should be the maximum key contained in the child to its left.
*/
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_AVAILABLE_CELL_SPACE = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3; // for testing
// const uint32_t INTERNAL_NODE_MAX_CELLS = INTERNAL_NODE_AVAILABLE_CELL_SPACE / INTERNAL_NODE_CELL_SIZE;

// for child pointers, are we storing a pointer, or the page index?
// i think just pointer, since the Pager will convert index to address

void print_constants()
{
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_AVAILABLE_CELL_SPACE: %d\n", LEAF_NODE_AVAILABLE_CELL_SPACE);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
    printf("INTERNAL_NODE_CELL_SIZE: %d\n", INTERNAL_NODE_CELL_SIZE);
    printf("INTERNAL_NODE_MAX_CELLS: %d\n", INTERNAL_NODE_MAX_CELLS);
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
    EXECUTE_STATEMENT_ERROR,
    EXECUTE_DUPLICATE_KEY
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
uint32_t *leaf_node_key(void *node, uint32_t cell_idx)
{
    return leaf_node_cell(node, cell_idx);
}

/* Returns pointer to value at cell_idx of leaf node */
void *leaf_node_value(void *node, uint32_t cell_idx)
{
    return leaf_node_cell(node, cell_idx) + LEAF_NODE_KEY_SIZE;
}

uint32_t *leaf_node_next_leaf(void *node)
{
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t *internal_node_cell(void *node, uint32_t cell_idx)
{
    return (uint32_t *)(node + INTERNAL_NODE_HEADER_SIZE + cell_idx * INTERNAL_NODE_CELL_SIZE);
}

uint32_t *internal_node_key(void *node, uint32_t cell_idx)
{
    return (void *)internal_node_cell(node, cell_idx) + INTERNAL_NODE_CHILD_SIZE;
}

/* Returns pointer to num keys for internal node */
uint32_t *internal_node_num_keys(void *node)
{
    return (uint32_t *)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

/* Returns pointer to page index of right child of internal node */
uint32_t *internal_node_right_child(void *node)
{
    return (uint32_t *)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

/* Returns pointer to page index of internal node i'th child */
uint32_t *internal_node_child(void *node, uint32_t child_idx)
{
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_idx > num_keys)
    {
        printf("Tried to access child idx (%d) when there are only (%d) keys\n", child_idx, num_keys);
        exit(EXIT_FAILURE);
    }

    if (child_idx == num_keys)
    {
        if (*internal_node_right_child(node) == INVALID_PAGE_IDX)
        {
            printf("Tried to access right child of node when it is empty\n");
            exit(EXIT_FAILURE);
        }
        return internal_node_right_child(node);
    }

    uint32_t *child = internal_node_cell(node, child_idx);
    if (*child == INVALID_PAGE_IDX)
    {
        printf("Tried to access child %d of node, but was invalid page\n", child_idx);
        exit(EXIT_FAILURE);
    }

    return child;
}

NodeType get_node_type(void *node)
{
    uint8_t value = *((uint8_t *)node + NODE_TYPE_OFFSET);
    return (NodeType)value;
}

void set_node_type(void *node, NodeType type)
{
    uint8_t value = (uint8_t)type;
    *(uint8_t *)(node + NODE_TYPE_OFFSET) = value;
}

bool is_node_root(void *node)
{
    uint8_t is_root = *(uint8_t *)(node + IS_ROOT_OFFSET);
    return (bool)is_root;
}

void set_node_root(void *node, bool is_root)
{
    uint8_t value = (uint8_t)is_root;
    *(uint8_t *)(node + IS_ROOT_OFFSET) = value;
}

void initialize_leaf_node(void *node)
{
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0; // 0 represents no sibling
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
}

void initialize_internal_node(void *node)
{
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
    *internal_node_right_child(node) = INVALID_PAGE_IDX; // empty node has no right child
}

uint32_t *node_parent(void *node)
{
    return node + PARENT_POINTER_OFFSET;
}

/*
Get the address of page based on its index
Allocates memory for page and loads data from file if it doesn't exist in memory yet
*/
void *get_page(Pager *pager, uint32_t page_idx)
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

uint32_t get_node_max_key(Pager *pager, void *node)
{
    if (get_node_type(node) == NODE_LEAF)
    {
        // max key will be the last key in the node
        return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }

    // internal node, recursively get the max key of the right child
    void *right_child = get_page(pager, *internal_node_right_child(node));
    return get_node_max_key(pager, right_child);
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
        // new database file. init page 0 as root and leaf
        void *root_node = get_page(pager, table->root_page_idx);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
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

void indent(uint32_t level)
{
    for (uint32_t i = 0; i < level; i++)
    {
        printf("  ");
    }
}

void print_tree(Pager *pager, uint32_t page_idx, uint32_t indentation_level)
{
    void *node = get_page(pager, page_idx);
    uint32_t num_keys, child;

    switch (get_node_type(node))
    {
    case (NODE_LEAF):
        num_keys = *leaf_node_num_cells(node);
        indent(indentation_level);
        printf("- leaf (size %d)\n", num_keys);

        for (uint32_t i = 0; i < num_keys; i++)
        {
            indent(indentation_level + 1);
            printf("- %d\n", *leaf_node_key(node, i));
        }

        break;
    case (NODE_INTERNAL):
        num_keys = *internal_node_num_keys(node);
        indent(indentation_level);
        printf("- internal (size %d)\n", num_keys);

        for (uint32_t i = 0; i < num_keys; i++)
        {
            child = *internal_node_child(node, i);
            print_tree(pager, child, indentation_level + 1);
            indent(indentation_level + 1);
            printf("- key %d\n", *internal_node_key(node, i));
        }

        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
        break;
    }
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
    else if (strcmp(input_buffer->buffer, ".btree") == 0)
    {
        void *node = get_page(table->pager, table->root_page_idx);
        print_tree(table->pager, table->root_page_idx, 0);
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
    void *node = get_page(table->pager, cursor->page_idx);

    return leaf_node_value(node, cursor->cell_idx);
}

/* Advance cursor to next cell */
void advance_cursor(Cursor *cursor)
{
    uint32_t page_idx = cursor->page_idx;
    void *node = get_page(cursor->table->pager, page_idx);
    cursor->cell_idx += 1;

    uint32_t num_cells = *leaf_node_num_cells(node);
    uint32_t num_pages = cursor->table->pager->num_pages;

    if (cursor->cell_idx >= num_cells)
    {
        // go to next leaf node
        uint32_t next_leaf_idx = *leaf_node_next_leaf(node);

        if (next_leaf_idx == 0)
        {
            // no more leaf nodes, set end of table to true
            cursor->end_of_table = true;
        }
        else
        {
            cursor->page_idx = next_leaf_idx;
            cursor->cell_idx = 0;
        }
    }
}

/*
Retrieves index of an unused page
For now, unused pages are always at end of database file
Once deletion is implemented, pages may become recycled
*/
uint32_t get_unused_page_idx(Pager *pager)
{
    return pager->num_pages;
}

void create_root_node(Table *table, uint32_t right_child_page_idx)
{
    // so at this point, the root has been split up and the right child has been made
    // now we need to create the left child, copy the contents of the root into it, and then
    // re-init the old root as the new root

    // why table and right child index as an argument?
    // table is identified by page idx of root, so we need to update that after creating the root
    // we also need to update the parent pointer of the right child

    /*
    Handle splitting the root.
    Old root copied to new page, becomes left child.
    Address of right child passed in.
    Re-initialize root page to contain the new root node.
    New root node points to two children.
    */

    // get current root
    void *root = get_page(table->pager, table->root_page_idx);
    void *right_child = get_page(table->pager, right_child_page_idx);

    // init a left child node
    uint32_t left_child_page_idx = get_unused_page_idx(table->pager);
    void *left_child = get_page(table->pager, left_child_page_idx);

    if (get_node_type(root) == NODE_INTERNAL)
    {
        initialize_internal_node(right_child);
        initialize_internal_node(left_child);
    }

    /* Left child has data copied from old root */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    if (get_node_type(left_child) == NODE_INTERNAL)
    {
        /*
        This code is necessary because:
        When we move the old root to become the left child, its page index changes
        (it's no longer at the root page index)

        All the children of this node still have their parent pointers
        pointing to the old root's page index

        We need to update all these children to point to the
        new page index of their parent
        */

        void *child;
        for (int i = 0; i < *internal_node_num_keys(left_child); i++)
        {
            child = get_page(table->pager, *internal_node_child(left_child, i));
            *node_parent(child) = left_child_page_idx;
        }
        child = get_page(table->pager, *internal_node_right_child(left_child));
        *node_parent(child) = left_child_page_idx;
    }

    /* Root node is a new internal node with one key and two children */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;

    // set the left child pointer and key
    *internal_node_child(root, 0) = left_child_page_idx;
    uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
    *internal_node_key(root, 0) = left_child_max_key;

    // set the right child pointer
    *internal_node_right_child(root) = right_child_page_idx;

    // set parent of left and right child to root
    *node_parent(left_child) = table->root_page_idx;
    *node_parent(right_child) = table->root_page_idx;
}

Cursor *leaf_node_find(Table *table, uint32_t page_idx, uint32_t key)
{
    // get leaf node
    void *node = get_page(table->pager, page_idx);

    // get num cells in leaf node
    uint32_t num_cells = *leaf_node_num_cells(node);

    // init two pointers, one at first cell and one past last cell
    int32_t min_idx = 0;
    int32_t max_idx = num_cells;

    // perform binary search to find the correct cell idx to insert key
    // printf("DEBUG: Performing binary search with min_idx (%d) and max_idx (%d) initial\n", min_idx, max_idx);
    while (min_idx < max_idx)
    {
        uint32_t index = (min_idx + max_idx) / 2;
        uint32_t key_at_index = *(uint32_t *)leaf_node_key(node, index);
        if (key == key_at_index)
        {
            min_idx = index;
            break;
        }
        if (key > key_at_index)
        {
            min_idx = index + 1;
        }
        else
        {
            max_idx = index;
        }
    }

    // printf("DEBUG: Insert key at idx %d\n", min_index);
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->cell_idx = min_idx;
    cursor->page_idx = page_idx;
    cursor->end_of_table = (min_idx == num_cells);

    return cursor;
}

/*
Return the index of the child within node `which should contain the given key.
*/
uint32_t internal_node_find_child(void *node, uint32_t key)
{
    uint32_t num_keys = *internal_node_num_keys(node);

    uint32_t min_index = 0;
    uint32_t max_index = num_keys; /* there is one more child than key */
    while (min_index < max_index)
    {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);

        if (key_to_right >= key)
        {
            max_index = index;
        }
        else
        {

            min_index = index + 1;
        }
    }

    return min_index;
}

/* Update key inside internal node */
void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key)
{
    // find the index inside node where old_key is
    uint32_t old_child_index = internal_node_find_child(node, old_key);

    if (old_child_index == *internal_node_num_keys(node))
    {
        // updating key associated with right child
        return;
    }

    // update its value
    *internal_node_key(node, old_child_index) = new_key;
}

void print_cells(void *node)
{
    if (get_node_type(node) == NODE_INTERNAL)
    {

        printf("\ncells of node %p\n", node);
        uint32_t num_cells = *internal_node_num_keys(node);
        for (uint32_t i = 0; i < num_cells; i++)
        {
            printf("\tCell {%d} -> child index %d, key %d\n", i, *internal_node_child(node, i), *internal_node_key(node, i));
        }
        printf("\tRight child -> child index %d\n", *internal_node_right_child(node));
    }
    printf("\n");
}

void internal_node_split_and_insert(Table *table, uint32_t parent_idx, uint32_t child_idx);

/*
Add a new child/key pair to parent that corresponds to child
note: parent_idx and child_idx are nodes represented by page indexes
*/
void internal_node_insert(Table *table, uint32_t parent_idx, uint32_t child_idx)
{
    // get parent and child node
    void *parent = get_page(table->pager, parent_idx);
    void *child = get_page(table->pager, child_idx);

    uint32_t original_num_keys = *internal_node_num_keys(parent);

    /* Case 1: internal node is full */
    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS)
    {
        printf("Inserting into internal node that is full\n");
        internal_node_split_and_insert(table, parent_idx, child_idx);
        return;
    }

    /* Case 2: internal node is empty */
    uint32_t right_child_idx = *internal_node_right_child(parent);
    if (right_child_idx == INVALID_PAGE_IDX)
    {
        // internal node is empty, set the right child
        printf("Internal node is empty, setting right child\n");
        *internal_node_right_child(parent) = child_idx;
        print_cells(parent);
        return;
    }

    /* Case 3: Internal node is neither empty nor full */

    // get max key of child (this will be inserted as the key)
    uint32_t max_key = get_node_max_key(table->pager, child);

    // determine where to insert key in parent node
    uint32_t idx_to_insert = internal_node_find_child(parent, max_key);

    void *right_child = get_page(table->pager, right_child_idx);
    
    // update number of keys
    *internal_node_num_keys(parent) = original_num_keys + 1;

    if (max_key > get_node_max_key(table->pager, right_child))
    {
        // replace right child
        // move right child/key pair to last child index (original_num_keys)
        *internal_node_child(parent, original_num_keys) = right_child_idx;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);

        // set new right child
        *internal_node_right_child(parent) = child_idx;
    }
    else
    {
        // make room for new cell
        for (uint32_t i = original_num_keys; i > idx_to_insert; i--)
        {
            void *dest = internal_node_cell(parent, i);
            void *src = internal_node_cell(parent, i - 1);
            memcpy(dest, src, INTERNAL_NODE_CELL_SIZE);
        }

        // insert cell (child and key)
        *internal_node_child(parent, idx_to_insert) = child_idx;
        *internal_node_key(parent, idx_to_insert) = max_key;
    }
}

/*
Splits node represented by parent_pg_idx
Then inserts child node into left or right sibling, depending on its max key
*/
void internal_node_split_and_insert(Table *table, uint32_t parent_pg_idx, uint32_t child_pg_idx)
{
    // track the node to be split
    uint32_t old_page_idx = parent_pg_idx;
    void *old_node = get_page(table->pager, parent_pg_idx);
    // this key will be updated in the parent after the split
    uint32_t old_max = get_node_max_key(table->pager, old_node);

    // find the key to add into the node
    void *child_node = get_page(table->pager, child_pg_idx);
    uint32_t child_max_key = get_node_max_key(table->pager, child_node);

    // allocate memory for a new node (right sibling)
    uint32_t new_page_idx = get_unused_page_idx(table->pager);

    uint32_t splitting_root = is_node_root(old_node);

    void *parent;
    void *new_node;
    if (splitting_root)
    {
        // Handle splitting root
        create_root_node(table, new_page_idx);
        parent = get_page(table->pager, table->root_page_idx);
        // at this point, right child of new root should be empty

        // the page index of the node to split will change if it was originally the root
        // so get the updated values
        old_page_idx = *internal_node_child(parent, 0);
        old_node = get_page(table->pager, old_page_idx);
    }
    else
    {
        /* if we are not splitting the root, we cannot insert our
        newly created sibling node into the old node’s parent right away,
        because it does not yet contain any keys and therefore will
        not be placed at the right index among the other key/child pairs
        which may or may not already be present in the parent node.
        need to move keys into the sibling node before inserting into the parent. */

        // parent of the newly split nodes will either be the new root, or the parent of the old node
        parent = get_page(table->pager, *node_parent(old_node));

        // why are the two lines below not needed if the old_node is the root?
        // create_root_node calls get_page on the new_page_idx
        // create_root_node also calls initialize_internal_node
        new_node = get_page(table->pager, new_page_idx);
        initialize_internal_node(new_node);
    }

    /* Move the right child + half the keys from old node to sibling node */
    uint32_t *old_num_keys = internal_node_num_keys(old_node);

    // Move the old node's right child to sibling node
    uint32_t cur_page_idx = *internal_node_right_child(old_node);
    void *cur = get_page(table->pager, cur_page_idx);
    internal_node_insert(table, new_page_idx, cur_page_idx);
    *node_parent(cur) = new_page_idx;
    *internal_node_right_child(old_node) = INVALID_PAGE_IDX;

    // Move half the keys
    for (uint32_t i = INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2; i--)
    {
        uint32_t cur_idx = *internal_node_child(old_node, i);
        void *cur_node = get_page(table->pager, cur_idx);

        // insert pair into sibling
        internal_node_insert(table, new_page_idx, cur_idx);

        // update child's parent value to point to sibling
        *node_parent(cur_node) = new_page_idx;

        // decrement old node's number of keys
        // this effectively erases the cells, but without freeing memory
        (*old_num_keys)--;
    }

    // set the highest key of old node as its right child
    uint32_t new_right_child_idx = *internal_node_child(old_node, *old_num_keys - 1);
    *internal_node_right_child(old_node) = new_right_child_idx;
    (*old_num_keys)--;

    // Insert child node in old node or sibling node depending on value of its max key
    uint32_t max_after_split = get_node_max_key(table->pager, old_node);
    uint32_t destination_idx = child_max_key < max_after_split ? old_page_idx : new_page_idx;

    internal_node_insert(table, destination_idx, child_pg_idx);

    *node_parent(child_node) = destination_idx;

    // Update original node's key in parent to reflect its new max after the split
    update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));

    // what is this??
    if (!splitting_root)
    {
        // update sibling node's parent pointer
        internal_node_insert(table, *node_parent(old_node), new_page_idx);
        *node_parent(new_node) = *node_parent(old_node);
    }
}

/*
Returns a cursor that points to where a key is
If the key doesn't exist, it points to where it should be inserted

page_idx refers to the internal node to search
*/
Cursor *internal_node_find(Table *table, uint32_t page_idx, uint32_t key)
{
    void *node = get_page(table->pager, page_idx);

    // find which child of node will contain key
    uint32_t child_idx = internal_node_find_child(node, key);

    // convert child index to page index, then use pg index to get pointer to child
    uint32_t child_page_idx = *internal_node_child(node, child_idx);
    void *child = get_page(table->pager, child_page_idx);

    if (get_node_type(child) == NODE_INTERNAL)
    {
        return internal_node_find(table, child_page_idx, key);
    }
    else
    {
        return leaf_node_find(table, child_page_idx, key);
    }
}

/*
Create new node and move half the cells over
Insert value into one of the two nodes
Update parent or create a new parent
*/
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value)
{
    // printf("DEBUG: Splitting leaf node\n");
    void *old_node = get_page(cursor->table->pager, cursor->page_idx);
    uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);

    // create new leaf node
    uint32_t new_page_idx = get_unused_page_idx(cursor->table->pager);
    void *new_node = get_page(cursor->table->pager, new_page_idx);
    // note that get_page will increment num_pages

    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node); // set parent of new node to be same as old node

    // update next leaf
    // have right node point to what left node used to point to
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    // have left node point to right node
    *leaf_node_next_leaf(old_node) = new_page_idx;

    /*
    Migrate half the values from old node to new node

    All existing keys plus new key should be divided
    evenly between old (left) and new (right) nodes.

    Starting from the right, move each key to correct position.
    Keys to the right of the new key will be shifted right to make room for it
    */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--)
    {
        // Determine which node to put cell in
        void *destination_node;
        if (i < LEAF_NODE_LEFT_SPLIT_COUNT)
        {
            destination_node = old_node;
        }
        else
        {
            destination_node = new_node;
        }

        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void *destination = leaf_node_cell(destination_node, index_within_node);
        if (i == cursor->cell_idx)
        {
            // insert new key and value (row) at cell_idx
            *(leaf_node_key(destination_node, index_within_node)) = key;
            serialize_row(value, leaf_node_value(destination_node, index_within_node));
        }
        else if (i > cursor->cell_idx)
        {
            // shift cells to the right to make room for insert
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        }
        else
        {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    // update cell counts
    *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *leaf_node_num_cells(new_node) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    // update parents
    if (is_node_root(old_node))
    {
        // old node was root and did not have parent node
        // create parent node and set as new root
        create_root_node(cursor->table, new_page_idx);
    }
    else
    {
        // update key in parent to be new max after split
        uint32_t parent_idx = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
        void *parent_node = get_page(cursor->table->pager, parent_idx);
        update_internal_node_key(parent_node, old_max, new_max);

        // insert key/child pointer of new node into internal node
        internal_node_insert(cursor->table, parent_idx, new_page_idx);
    }
}

/*
Inserts a key value pair at position represented by cursor into a leaf node
*/
void leaf_node_insert_cell(Cursor *cursor, uint32_t key, Row *value)
{
    // get address of node pointed by cursor
    Pager *pager = cursor->table->pager;
    void *node = get_page(pager, cursor->page_idx);

    // get number of cells currently in leaf node
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS)
    {
        // leaf node is full, need to split node
        leaf_node_split_and_insert(cursor, key, value);
        return;
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
    // printf("DEBUG: inserted key (%d) and row (%s)\n", key, value->username);

    // increment num cells in leaf node
    *(leaf_node_num_cells(node)) += 1;
}

/*
Returns a cursor pointing to position of key
If key does not exist, return position where it should be inserted
*/
Cursor *table_find(Table *table, uint32_t key)
{
    // get root node
    void *node = get_page(table->pager, table->root_page_idx);

    if (get_node_type(node) == NODE_INTERNAL)
    {
        return internal_node_find(table, table->root_page_idx, key);
    }

    Cursor *cursor = leaf_node_find(table, table->root_page_idx, key);
    return cursor;
}

/* Initializes a cursor pointing to start of a table */
Cursor *init_cursor_table_start(Table *table)
{
    Cursor *cursor = table_find(table, 0); // key of 0 should return cursor pointing to leftmost leaf node
    void *node = get_page(table->pager, cursor->page_idx);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0); // start is also end of table if there are no cells

    return cursor;
}

ExecuteResult execute_insert(Table *table, Statement *statement)
{
    // for now, table is represented as a single leaf node
    // instead of inserting at end of leaf node, we need to insert such that order is maintained

    // get addr of root leaf node
    void *node = get_page(table->pager, table->root_page_idx);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Row *row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;

    // set cursor at the correct cell index within the leaf node to insert
    Cursor *cursor = table_find(table, key_to_insert);
    // printf("DEBUG: Set cursor at page index (%d), cell index (%d)\n", cursor->page_idx, cursor->cell_idx);

    if (cursor->cell_idx < num_cells)
    {
        uint32_t key_at_cell_idx = *(uint32_t *)leaf_node_key(node, cursor->cell_idx);
        if (key_at_cell_idx == key_to_insert)
        {
            // key already exists
            printf("Key (%d) already exists in table\n", key_to_insert);
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    // insert cell
    leaf_node_insert_cell(cursor, key_to_insert, row_to_insert);

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
        case (EXECUTE_DUPLICATE_KEY):
            printf("Failed to insert, key already exists.\n");
            continue;
        case (EXECUTE_STATEMENT_ERROR):
            printf("Error executing statement, please retry.\n");
        }
    }
}