import subprocess
import unittest

MAX_ROWS = 1400

class TestDatabase(unittest.TestCase):
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


    # Test the insert function when the table is full
    def test_insert_full_table(self):
        commands = [f"INSERT {i} user{i} user{i}@example.com" for i in range(MAX_ROWS + 1)]
        commands += [".exit"]

        result = self.run_script(commands)

        self.assertEqual(result[-2:], [
            "db > Failed to insert, table is full.",
            "db > "
        ])


if __name__ == '__main__':
    unittest.main()