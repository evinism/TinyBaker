from typing import Union
from fs import open_fs
from fs.opener.parse import parse_fs_url
from .exceptions import SeriousErrorThatYouShouldOpenAnIssueForIfYouGet
from io import BufferedWriter, BufferedReader, TextIOWrapper, StringIO, BytesIO
from base64 import b64decode
from .exceptions import BakerError


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

    def __init__(self, path, read_bit, write_bit, run_info):
        self.path = path
        self._read = read_bit
        self._write = write_bit
        self.opened = False
        self.run_info = run_info

    def exists(self) -> bool:
        """
        Determine whether the file specified by the FileRef exists

        :return: Whether the file exists
        """
        if self._get_protocol() == "data":
            return True

        filesystem, resource, should_close = self._get_fs_and_path()
        try:
            return filesystem.exists(resource)
        finally:
            if should_close:
                filesystem.close()

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

        filesystem, resource, should_close = self._get_fs_and_path()
        try:
            mode = ""
            if self._read:
                mode = mode + "r"
            if self._write:
                mode = mode + "w"
            method = filesystem.openbin if bin else filesystem.open
            return method(resource, mode)
        finally:
            if should_close:
                filesystem.close()

    def _get_fs_and_path(self):
        # Annoyingly, relative dirs are not parsed well by parse_fs_url
        protocol = self._get_protocol()
        if protocol in self.run_info.open_fses:
            # pre-opened case
            parsed = parse_fs_url(self.path)
            fs = self.run_info.open_fses[protocol]
            fname = parsed.resource
            should_close = False
        else:
            # self-managed case
            fname = get_fname(self.path)
            truncated_path = get_truncated_path(self.path, fname)
            fs = open_fs(truncated_path)
            should_close = True
        return (fs, fname, should_close)

    def _get_protocol(self):
        return self.path.split("://")[0]