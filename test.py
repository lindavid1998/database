import subprocess
import unittest
import os

MAX_ROWS = 1400
MAX_ROWS_IN_LEAF = 13
MAX_KEYS_IN_INTERNAL = 3
# MAX_KEYS_IN_INTERNAL = 510
MAX_USERNAME_LENGTH = 32
MAX_EMAIL_LENGTH = 255

class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Compile with shell=True to properly execute gcc command
        process = subprocess.Popen(
            "gcc db.c",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Wait for compilation to complete and check for errors
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise Exception(f"Compilation failed: {stderr.decode()}")

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
    @unittest.skip("This test is skipped, need to implement internal node searching")
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

        self.assertEqual(result[-1:], [
            "db > Trying to insert cell but internal node is full. Need to implement splitting internal node",
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
            "LEAF_NODE_HEADER_SIZE: 14",
            "LEAF_NODE_CELL_SIZE: 295",
            "LEAF_NODE_AVAILABLE_CELL_SPACE: 4082",
            "LEAF_NODE_MAX_CELLS: 13",
            "INTERNAL_NODE_CELL_SIZE: 8",
            f"INTERNAL_NODE_MAX_CELLS: {MAX_KEYS_IN_INTERNAL}",
            "db > "
        ])

    def test_keys_inserted_in_increasing_order(self):
        commands = [
            "INSERT 3 user3 user3@email.com",
            "INSERT 1 user1 user1@email.com",
            "INSERT 2 user2 user2@email.com",
            ".btree",
            ".exit"
        ]
        result = self.run_script(commands)

        expected = [
            "db > - leaf (size 3)",
            "  - 1",
            "  - 2",
            "  - 3",
            "db > "
        ]

        self.assertEqual(result[-5:], expected)

    def test_duplicate_keys(self):
        commands = [
            "INSERT 1 user1 user1@email.com",
            "INSERT 1 user1 user1@email.com",
            ".exit"
        ]
        result = self.run_script(commands)

        self.assertEqual(result, [
            "db > Executed.",
            "db > Key (1) already exists in table",
            "Failed to insert, key already exists.",
            "db > "
        ])
    
    def test_root_node_splits_when_full(self):
        commands = [f"INSERT {i} user{i} user{i}@example.com" for i in range(MAX_ROWS_IN_LEAF + 1)]
        commands += [".btree", ".exit"]

        result = self.run_script(commands)

        expected = [
            "db > - internal (size 1)",
            "  - leaf (size 7)",
            "    - 0",
            "    - 1",
            "    - 2",
            "    - 3",
            "    - 4",
            "    - 5",
            "    - 6",
            "  - key 6",
            "  - leaf (size 7)",
            "    - 7",
            "    - 8",
            "    - 9",
            "    - 10",
            "    - 11",
            "    - 12",
            "    - 13",
            "db > "
        ]

        self.assertEqual(result[-1 * len(expected):], expected)
    
    def test_print_all_rows_in_multi_level_tree(self):
        commands = [f"INSERT {i} user{i} user{i}@email.com" for i in range(MAX_ROWS_IN_LEAF + 2)]
        commands += ["SELECT", ".exit"]

        result = self.run_script(commands)

        expected = [
            "db > 0 user0 user0@email.com",
            "1 user1 user1@email.com",
            "2 user2 user2@email.com",
            "3 user3 user3@email.com",
            "4 user4 user4@email.com",
            "5 user5 user5@email.com",
            "6 user6 user6@email.com",
            "7 user7 user7@email.com",
            "8 user8 user8@email.com",
            "9 user9 user9@email.com",
            "10 user10 user10@email.com",
            "11 user11 user11@email.com",
            "12 user12 user12@email.com",
            "13 user13 user13@email.com",
            "14 user14 user14@email.com",
            "Executed.",
            "db > "
        ]

        self.assertEqual(result[-1 * len(expected):], expected)

    def test_print_multi_level_tree(self):
        commands = [f"INSERT {i} user{i} user{i}@example.com" for i in range(MAX_ROWS_IN_LEAF + 2)]
        commands += [".btree", ".exit"]

        result = self.run_script(commands)

        expected = [
            "db > - internal (size 1)",
            "  - leaf (size 7)",
            "    - 0",
            "    - 1",
            "    - 2",
            "    - 3",
            "    - 4",
            "    - 5",
            "    - 6",
            "  - key 6",
            "  - leaf (size 8)",
            "    - 7",
            "    - 8",
            "    - 9",
            "    - 10",
            "    - 11",
            "    - 12",
            "    - 13",
            "    - 14",
            "db > "
        ]

        self.assertEqual(result[-1 * len(expected):], expected)

    def test_allows_printing_of_4_leaf_node_tree(self):
        commands = [
            "INSERT 18 user18 person18@example.com",
            "INSERT 7 user7 person7@example.com",
            "INSERT 10 user10 person10@example.com",
            "INSERT 29 user29 person29@example.com",
            "INSERT 23 user23 person23@example.com",
            "INSERT 4 user4 person4@example.com",
            "INSERT 14 user14 person14@example.com",
            "INSERT 30 user30 person30@example.com",
            "INSERT 15 user15 person15@example.com",
            "INSERT 26 user26 person26@example.com",
            "INSERT 22 user22 person22@example.com",
            "INSERT 19 user19 person19@example.com",
            "INSERT 2 user2 person2@example.com",
            "INSERT 1 user1 person1@example.com",
            "INSERT 21 user21 person21@example.com",
            "INSERT 11 user11 person11@example.com", 
            "INSERT 6 user6 person6@example.com",
            "INSERT 20 user20 person20@example.com",
            "INSERT 5 user5 person5@example.com",
            "INSERT 8 user8 person8@example.com",
            "INSERT 9 user9 person9@example.com",
            "INSERT 3 user3 person3@example.com",
            "INSERT 12 user12 person12@example.com",
            "INSERT 27 user27 person27@example.com",
            "INSERT 17 user17 person17@example.com",
            "INSERT 16 user16 person16@example.com",
            "INSERT 13 user13 person13@example.com",
            "INSERT 24 user24 person24@example.com",
            "INSERT 25 user25 person25@example.com",
            "INSERT 28 user28 person28@example.com",
            ".btree",
            ".exit",
        ]

        result = self.run_script(commands)

        expected = [
            "db > - internal (size 3)",
            "  - leaf (size 7)",
            "    - 1",
            "    - 2",
            "    - 3",
            "    - 4",
            "    - 5",
            "    - 6",
            "    - 7",
            "  - key 7",
            "  - leaf (size 8)",
            "    - 8",
            "    - 9",
            "    - 10",
            "    - 11",
            "    - 12",
            "    - 13",
            "    - 14",
            "    - 15",
            "  - key 15",
            "  - leaf (size 7)",
            "    - 16",
            "    - 17",
            "    - 18",
            "    - 19",
            "    - 20",
            "    - 21",
            "    - 22",
            "  - key 22",
            "  - leaf (size 8)",
            "    - 23",
            "    - 24",
            "    - 25",
            "    - 26",
            "    - 27",
            "    - 28",
            "    - 29",
            "    - 30",
            "db > "
        ]

        self.assertEqual(result[-1 * len(expected):], expected)

if __name__ == '__main__':
    unittest.main()