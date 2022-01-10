from enum import Enum
class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)

class periodOption(Enum):
    last_day = 1
    last_2day = 2
    last_week = 3
    last_month = 4
    last_quarter = 5
    manual = 6

