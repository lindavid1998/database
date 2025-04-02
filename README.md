# SQLite Clone in C

## Overview

This project is an implementation of a lightweight database in C based on SQLite. Inspired by [cstack's db_tutorial](https://cstack.github.io/db_tutorial/), this clone was built to deepen my understanding of database implementation, including query processing and data persistence.

## Motivation

I undertook this project to answer fundamental questions about databases:
- How are user commands processed and executed?
- How is data persisted to and retrieved from disk efficiently?
- Why are B-trees the data structure of choice for database indexing?

## Key Features

- Simple REPL interface for database interaction
- Single table creation and row insertion functionality
- Persistent storage to disk
- B-tree implementation for efficient data access

## Implementation Details

The database follows a modular design with these key components:

1. **REPL (Read-Eval-Print Loop)**: Handles user input and output
2. **Parser & Compiler**: Translates SQL-like statements into executable operations
3. **Virtual Machine**: Executes prepared statements
4. **B-tree**: Manages data storage
5. **Pager**: Handles disk I/O and memory management

## What I Learned & Challenges

B-trees provide ideal database indexing.
- Their self-balancing property guarantees O(log n) time complexity for all queries
- Linked leaf nodes enable efficient range queries through sibling pointers
- High fan-out ratio minimizes disk I/O operations (critical for persistent storage)

Decoupled components create more maintainable and flexible systems. For example, by separating statement preparation (i.e. syntax validation) from statement execution, there was no need for validation during execution.

For more info on an interesting bug I ran into, please visit [here](./loop_bug.md).

## Building and Running

```bash
$ gcc db.c
$ ./a.out
```

The REPL supports these commands:
- INSERT (user_id) (name) (email) - Add a new row to the database
- SELECT - Display all rows
- .btree - Debug command to show B-tree structure
- .exit - Quit the program

An example is shown below:
```bash
db > INSERT 1 user1 user1@email.com
Executed.
db > SELECT
1 user1 user1@email.com
Executed.
db > .exit
```

Note that the data will be stored in a file called `data.db`.

## Future Enhancements
- Add range queries
- Implement UPDATE and DELETE operations
- Add support for multiple tables

## Resources

This project was heavily inspired by cstack's db_tutorial.