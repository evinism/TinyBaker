from fs import open_fs
from typing import Dict, Set
from abc import ABC, abstractmethod
from .fileref import FileRef
from .exceptions import FileSetError, CircularFileSetError, BakerError

PathDict = Dict[str, str]
FileDict = Dict[str, FileRef]
TagSet = Set[str]


class StepDefinition(ABC):
    input_file_set: TagSet = set()
    output_file_set: TagSet = set()

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

        input_path_set = {input_paths[tag] for tag in input_paths}
        output_path_set = {output_paths[tag] for tag in output_paths}
        intersection = set.intersection(input_path_set, output_path_set)
        if len(intersection):
            raise CircularFileSetError(
                "File included as both input and output: {}".format(
                    ", ".join(intersection)
                )
            )

        for f in input_paths:
            self.input_files[f] = FileRef(
                input_paths[f], read_bit=True, write_bit=False
            )

        for f in output_paths:
            self.output_files[f] = FileRef(
                output_paths[f], read_bit=False, write_bit=True
            )

    def build(self, overwrite=True):
        for tag in self.input_files:
            file_ref = self.input_files[tag]
            if not file_ref.exists():
                raise BakerError(
                    "Referenced input path {} does not exist!".format(file_ref.path)
                )
        for tag in self.output_files:
            file_ref = self.output_files[tag]
            if file_ref.exists() and not overwrite:
                raise BakerError(
                    "Referenced output path {} already exists, and overwrite is not enabled".format(
                        file_ref.path
                    )
                )
        self.script()

    @abstractmethod
    def script(self):
        pass
