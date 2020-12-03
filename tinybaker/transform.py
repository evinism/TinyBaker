from fs import open_fs
from typing import Dict, Set
import inspect
from abc import ABC, abstractmethod
from .fileref import FileRef
from .exceptions import (
    FileSetError,
    CircularFileSetError,
    BakerError,
    SeriousErrorThatYouShouldOpenAnIssueForIfYouGet,
)
from .context import BakerContext, DefaultContext


PathDict = Dict[str, str]
FileDict = Dict[str, FileRef]
TagSet = Set[str]


class Transform(ABC):
    input_tags: TagSet = set()
    output_tags: TagSet = set()

    def __init__(
        self,
        input_paths: PathDict,
        output_paths: PathDict,
        context: BakerContext = DefaultContext(),
    ):
        self.input_paths = input_paths
        self.output_paths = output_paths
        self.input_files: FileDict = {}
        self.output_files: FileDict = {}
        self.context = context
        self._current_run_info = None

    def _init_file_dicts(self, input_paths: PathDict, output_paths: PathDict):
        if set(input_paths) != self.input_tags:
            raise FileSetError(set(input_paths), self.input_tags)

        if set(output_paths) != self.output_tags:
            raise FileSetError(set(output_paths), self.output_tags)

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
                input_paths[f],
                read_bit=True,
                write_bit=False,
                run_info=self._current_run_info,
            )

        for f in output_paths:
            self.output_files[f] = FileRef(
                output_paths[f],
                read_bit=False,
                write_bit=True,
                run_info=self._current_run_info,
            )

    def _validate_file_existence(self):
        overwrite = self.context.overwrite
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

    def build(self):
        self.context.run_transform(self)

    def exec_internal(self, run_info):
        self._current_run_info = run_info
        self._init_file_dicts(self.input_paths, self.output_paths)
        self._validate_file_existence()
        if not run_info:
            raise SeriousErrorThatYouShouldOpenAnIssueForIfYouGet(
                "No current run information, somehow!"
            )
        self.script()

    @abstractmethod
    def script(self):
        pass
