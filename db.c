#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

// #define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
// const uint32_t ID_SIZE = size_of_attribute(Row, id);
// const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
// const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_SIZE = 4; // bytes
const uint32_t USERNAME_SIZE = 32; // bytes
const uint32_t EMAIL_SIZE = 255; // bytes
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

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
} PrepareResult;

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

void execute_statement(Statement *statement)
{
    switch (statement->type)
    {
    case (STATEMENT_INSERT):
        // todo
        printf("Handle insert\n");
        break;
    case (STATEMENT_SELECT):
        // todo
        printf("Handle select\n");
        break;
    }
}

int main(int argc, char *argv[])
{
    InputBuffer *input_buffer = new_input_buffer();

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
        case (PREPARE_STATEMENT_SYNTAX_ERROR):
            printf("Syntax error in statement '%s'.\n", input_buffer->buffer);
            continue;
        case (PREPARE_STATEMENT_UNRECOGNIZED_COMMAND):
            printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
            continue;
        }

        // execute Statement
        execute_statement(&statement);
        printf("Executed.\n");
    }
}