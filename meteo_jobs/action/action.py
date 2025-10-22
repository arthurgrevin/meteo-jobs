from typing import Protocol, Iterator

class Action(Protocol):

    def __init__(self):
        """"""
        super().__init__()
  
    def execute(self, records: Iterator) -> Iterator:
        """ Execute an action on each record of records"""