#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

typedef struct
{
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

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

Table *new_table()
{
    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = 0;
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
    {
        table->pages[i] = NULL;
    }
    return table;
}

void free_table(Table *table)
{
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
    {
        free(table->pages[i]);
    }
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

MetaCommandResult do_meta_command(InputBuffer *input_buffer)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buffer);
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
Get address for where to insert row
i.e. convert row num to address
*/
void *get_row_address(Table *table, uint32_t row_idx)
{
    // Get page address //
    uint32_t page_index = row_idx / ROWS_PER_PAGE;
    void *page = table->pages[page_index];
    // if page hasn't been allocated yet (null pointer)
    if (page == NULL)
    {
        // allocate memory for it using PAGE_SIZE
        page = table->pages[page_index] = malloc(PAGE_SIZE);
    }

    // Get page offset //
    // to get offset, figure out which row idx on the page, then convert to bytes
    uint32_t row_index = row_idx % ROWS_PER_PAGE;
    uint32_t offset = row_index * ROW_SIZE;

    return page + offset;
}

ExecuteResult execute_insert(Table *table, Statement *statement)
{
    // check if table is full
    if (table->num_rows == TABLE_MAX_ROWS)
    {
        return EXECUTE_STATEMENT_TABLE_FULL;
    }

    // get address to insert row
    void *row_addr = get_row_address(table, table->num_rows);

    // insert serialized row into address
    serialize_row(&(statement->row_to_insert), row_addr);

    // increment table->num_rows by 1
    table->num_rows += 1;

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
    // for each row, deserialize and print
    for (uint32_t i = 0; i < table->num_rows; i++)
    {
        deserialize_row(get_row_address(table, i), &row);
        print_row(&row);
    }
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
    InputBuffer *input_buffer = new_input_buffer();
    Table *table = new_table(); // table in memory, does not persist to disk

    while (true)
    {
        print_prompt();
        read_input(input_buffer);

        // printf("Command: '%s'.\n", input_buffer->buffer);

        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer))
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