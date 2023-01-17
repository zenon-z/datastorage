class ModelCli:
    commands = ["ADD_TRIPLE", "QUERY_TRIPLE", "DELETE_TRIPLE", "BULK_ADD", "BULK_GET", "BULK_DELETE"]

    def __init__(self, utilities):
        self.utilities = utilities

    def start_cli(self):
        print("The following commands are available:")
        for command in self.commands:
            if command.__contains__("_TRIPLE"):
                print(f"{command} <s> <p> <o>")
            elif command.__contains__("BULK_"):
                print(f"{command} <turtle file>")
        while True:
            self.parse_command(input("Enter a command\n"))

    def add_triple(self, s, p, o):
        self.utilities.add_triple(subject_pattern=parse_pattern_variable(s),
                                  predicate_pattern=parse_pattern_variable(p),
                                  object_pattern=parse_pattern_variable(o))

        print(f"Triple {s} - {p} - {o} added")

    def delete_triple(self, s, p, o):
        self.utilities.delete_triple(subject_pattern=parse_pattern_variable(s),
                                     predicate_pattern=parse_pattern_variable(p),
                                     object_pattern=parse_pattern_variable(o))

        print(f"Triple {s} - {p} - {o} deleted")

    def query_triple(self, s, p, o):
        answer = self.utilities.get_triple(subject_pattern=parse_pattern_variable(s),
                                           predicate_pattern=parse_pattern_variable(p),
                                           object_pattern=parse_pattern_variable(o))
        for elem in answer:
            print(elem)

    def parse_command(self, command):

        parameters = command.split(" ")
        if len(parameters) != 4 or not parameters[0] in self.commands:
            print("UNKNOWN COMMAND")

        if parameters[0] == "ADD_TRIPLE":
            self.add_triple(parameters[1], parameters[2], parameters[3])

        if parameters[0] == "DELETE_TRIPLE":
            self.delete_triple(parameters[1], parameters[2], parameters[3])

        if parameters[0] == "QUERY_TRIPLE":
            self.query_triple(parameters[1], parameters[2], parameters[3])

        if parameters[0] == "BULK_ADD":
            self.utilities.bulk_add_triple(parameters[1])

        if parameters[0] == "BULK_GET":
            self.utilities.bulk_get_triple(parameters[1])

        if parameters[0] == "BULK_DELETE":
            self.utilities.bulk_delete_triple(parameters[1])


def parse_pattern_variable(pattern_var):
    return "" if pattern_var[0] == "?" else pattern_var
