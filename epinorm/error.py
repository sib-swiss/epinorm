class UserError(Exception):
    "Custom error class for errors due to user input."

    def __init__(self, msg):
        super().__init__(f"ERROR: {msg}")
