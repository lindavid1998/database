import subprocess

MAX_ROWS = 1400

def compile_c_code():
    compile_command = "gcc db.c"
    result = subprocess.run(compile_command, shell=True)
    if result.returncode != 0:
        print("Compilation failed.")
        exit(1)


def start_process():
    process = subprocess.Popen(
        ["./a.out"], 
        stdin=subprocess.PIPE, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
     )

    return process
    

def test_syntax_error():
    process = start_process()

    command = "INSERT foo bar 1\n"
    process.stdin.write(command)
    process.stdin.flush()

    # Read the output
    output, errors = process.communicate()

    if "Syntax error" in output:
        print("Test passed: INSERT syntax error handled correctly.")
    else:
        print("Test failed: Unexpected output.")
        print("Output:", output)


# Test the insert function when the table is full
def test_insert_full_table():
    process = start_process()

    for i in range(MAX_ROWS):  
        command = f"INSERT {i} user{i} user{i}@example.com\n"
        process.stdin.write(command)
        process.stdin.flush()

    # Try to insert one more row to trigger the full table condition
    command = "INSERT 100 user100 user100@example.com\n"
    process.stdin.write(command)
    process.stdin.flush()

    # Read the output
    output, errors = process.communicate()

    if "Failed to insert, table is full" in output:
        print("Test passed: Full table condition handled correctly.")
    else:
        print("Test failed: Unexpected output.")
        print("Output:", output)


if __name__ == "__main__":
    compile_c_code()
    test_syntax_error()
    test_insert_full_table()