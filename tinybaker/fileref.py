from typing import Union
from .exceptions import SeriousErrorThatYouShouldOpenAnIssueForIfYouGet
from io import BufferedWriter, BufferedReader, TextIOWrapper, StringIO, BytesIO
from base64 import b64decode
from .exceptions import BakerError
import fsspec


def get_fname(path):
    items = path.split("/")
    if not len(items):
        return ""
    return items[-1]


def get_truncated_path(path, fname):
    suffix = "/{}".format(fname)
    if not path.endswith(suffix):
        raise SeriousErrorThatYouShouldOpenAnIssueForIfYouGet(
            "Tried to truncate a path that didnt end with said path"
        )
    return path[: -len(suffix)]


class FileRef:
    """Represents a reference to a file. TinyBaker generates these for use in the script() function"""

    def __init__(self, path, read_bit, write_bit, worker_context):
        self.path = path
        self._read = read_bit
        self._write = write_bit
        self.opened = False
        self.worker_context = worker_context

    def exists(self) -> bool:
        """
        Determine whether the file specified by the FileRef exists

        :return: Whether the file exists
        """
        if self._get_protocol() == "data":
            return True

        try:
            with fsspec.open(self.path):
                pass
        except:
            return False
        return True

    def open(self) -> TextIOWrapper:
        """
        Open the FileRef for use with textual data

        :return: The stream object for interacting with the FileRef
        """
        return self._open_helper()

    def openbin(self) -> Union[BufferedWriter, BufferedReader]:
        """
        Open the FileRef for use with binary data

        :return: The stream for interacting with the FileRef
        """
        return self._open_helper(True)

    def touch(self):
        """
        Mark the FileRef as being opened, without actually opening it.

        This is useful if you want to perform some operation on a fileref
        outside of TinyBaker's file abstractions, e.g. `tag.touch()`,
        followed by `make_some_app_specific_mutation_on(tag.path)`
        """
        self.opened = True

    def _open_helper(self, bin=False):
        self.touch()

        if self._get_protocol() == "data":
            data = b64decode(self.path.split("://")[1])
            if self._write:
                raise BakerError("Cannot write to data protocol")
            if bin:
                return BytesIO(initial_bytes=data)
            else:
                return StringIO(initial_value=data.decode("utf-8"))

        mode = ""
        if self._read:
            mode = mode + "r"
        if self._write:
            mode = mode + "w"
        if bin:
            mode = mode + "b"
        return fsspec.open(self.path, mode)

    def _get_protocol(self):
        if "://" in self.path:
            return self.path.split("://")[0]
        return None
