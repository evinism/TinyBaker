from typing import Dict, Set
from abc import ABC, abstractmethod
from .fileref import FileRef
from .exceptions import FileSetError

PathDict = Dict[str, str]
FileDict = Dict[str, FileRef]


class StepDefinition(ABC):
    input_file_set = set()
    output_file_set = set()

    def __init__(self, input_paths: PathDict, output_paths: PathDict, config=None):
        self.input_files: FileDict = {}
        self.output_files: FileDict = {}
        self.config = config
        self._init_file_dicts(input_paths, output_paths)

    def _init_file_dicts(self, input_paths: PathDict, output_paths: PathDict):
        if set(input_paths) != self.input_file_set:
            raise FileSetError(set(input_paths), self.input_file_set)

        if set(output_paths) != self.output_file_set:
            raise FileSetError(set(output_paths), self.output_file_set)

        for f in input_paths:
            self.input_files[f] = FileRef(
                input_paths[f], read_bit=True, write_bit=False
            )

        for f in output_paths:
            self.output_files[f] = FileRef(
                output_paths[f], read_bit=False, write_bit=True
            )

    def build(self, overwrite=False):
        for tag in input_files:
            if input_files[tag]: 
        self.script()

    @abstractmethod
    def script(self):
        pass
