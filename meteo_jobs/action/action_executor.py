from .action import Action
from typing import Iterator

class ActionExecutor:

    def __init__(self, action: Action):
        self.action = action
    
    def execute(self, records: Iterator) -> Iterator:
        return self.action.execute(records)