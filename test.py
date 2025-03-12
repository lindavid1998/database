import subprocess
import unittest
import os

MAX_ROWS = 1400
MAX_USERNAME_LENGTH = 32
MAX_EMAIL_LENGTH = 255

class TestDatabase(unittest.TestCase):
    def tearDown(self):
        os.remove("data.db")

    def run_script(self, commands):
        process = subprocess.Popen(
            ["./a.out"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        for command in commands:
            try:
                process.stdin.write(command + "\n")
            except BrokenPipeError:
                break

        process.stdin.close()
        output = process.stdout.read()
        process.stdout.close()
        process.stderr.close()
        process.wait()

        return output.splitlines()
    

    def test_insert_and_retrieve_row(self):
        result = self.run_script([
            "INSERT 1 user1 person1@example.com",
            "SELECT",
            ".exit",
        ])
        # if .exit is excluded, there seems to be an error thrown 
        # this error is thrown when bytes_read from the input is 0

        self.assertEqual(result, [
            "db > Executed.",
            "db > 1 user1 person1@example.com",
            "Executed.",
            "db > ",
        ])


    def test_insert_syntax_error(self):
        commands = ["INSERT foo bar 1", ".exit"]
        result = self.run_script(commands)

        self.assertEqual(result, [
            f"db > Syntax error in statement '{commands[0]}'.",
            "db > "
        ])
    

    def test_unrecognized_keyword(self):
        commands = ["SELETC", ".exit"]
        result = self.run_script(commands)

        self.assertEqual(result, [
            f"db > Unrecognized keyword at start of '{commands[0]}'.",
            "db > "
        ])


    # Tests that table can hold MAX_ROWS    
    def test_insert_at_full_table(self):
        commands = [f"INSERT {i} user{i} user{i}@example.com" for i in range(MAX_ROWS)]
        commands += [".exit"]

        result = self.run_script(commands)

        self.assertEqual(result[-2:], [
            "db > Executed.",
            "db > "
        ])


    # Test the insert function when the table is full
    def test_insert_exceed_full_table(self):
        commands = [f"INSERT {i} user{i} user{i}@example.com" for i in range(MAX_ROWS + 1)]
        commands += [".exit"]

        result = self.run_script(commands)

        self.assertEqual(result[-2:], [
            "db > Failed to insert, table is full.",
            "db > "
        ])

    # 'allows inserting strings that are the maximum length'
    def test_insert_max_length_strings(self):
        username = "a" * MAX_USERNAME_LENGTH
        email = "a" * MAX_EMAIL_LENGTH

        commands = [f"INSERT 0 {username} {email}", ".exit"]

        result = self.run_script(commands)

        self.assertEqual(result, [
            "db > Executed.",
            "db > "
        ])

    @unittest.skip("This test is skipped because it's under development")
    def test_insert_string_too_long(self):
        # NOTE: this test fails, so code does not correctly handle right now
        username = "a" * (MAX_USERNAME_LENGTH + 1)
        email = "a" * (MAX_EMAIL_LENGTH + 1)

        commands = [f"INSERT 0 {username} {email}", ".exit"]

        result = self.run_script(commands)

        self.assertEqual(result, [
            "db > Input string is too long.",
            "db > "
        ])
    
    def test_data_persists(self):
        commands = ["INSERT 0 user0 user0@email.com", ".exit"]
        result = self.run_script(commands)

        commands = ["SELECT", ".exit"]
        result = self.run_script(commands)

        self.assertEqual(result[-3:], [
            "db > 0 user0 user0@email.com",
            "Executed.",
            "db > "
        ])

    def test_constants(self):
        commands = [".constants", ".exit"]
        result = self.run_script(commands)

        self.assertEqual(result, [
            "db > ROW_SIZE: 291",
            "COMMON_NODE_HEADER_SIZE: 6",
            "LEAF_NODE_HEADER_SIZE: 10",
            "LEAF_NODE_CELL_SIZE: 295",
            "LEAF_NODE_AVAILABLE_CELL_SPACE: 4086",
            "LEAF_NODE_MAX_CELLS: 13",
            "db > "
        ])

if __name__ == '__main__':
    unittest.main()