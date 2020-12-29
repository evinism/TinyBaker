from typing import Union
from fs import open_fs
from fs.opener.parse import parse_fs_url
from .exceptions import SeriousErrorThatYouShouldOpenAnIssueForIfYouGet
from io import BufferedWriter, BufferedReader, TextIOWrapper, StringIO, BytesIO
from base64 import b64decode


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

        filesystem, previously_opened_filesystem, resource = self._get_fs_and_path()
        if filesystem:
            with filesystem:
                return filesystem.exists(resource)
        return previously_opened_filesystem.exists(resource)

    def _open_helper(self, bin=False):
        self.touch()

        # TODO: Get a bunch of shit done.
        if self._get_protocol() == "data":
            data = b64decode(self.path.split("://")[1])
            if self._write:
                raise BakerError("Cannot write to data protocol")
            if bin:
                return BytesIO(initial_bytes=data)
            else:
                return StringIO(initial_value=data.decode("utf-8"))

        # TERRIBLE AND UGLY
        filesystem, previously_opened_filesystem, resource = self._get_fs_and_path()
        if filesystem:
            with filesystem:
                mode = ""
                if self._read:
                    mode = mode + "r"
                if self._write:
                    mode = mode + "w"
                if bin:
                    return filesystem.openbin(resource, mode)
                return filesystem.open(resource, mode)

        else:
            mode = ""
            if self._read:
                mode = mode + "r"
            if self._write:
                mode = mode + "w"
            if bin:
                return previously_opened_filesystem.openbin(resource, mode)
            return previously_opened_filesystem.open(resource, mode)

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

    def _get_fs_and_path(self):
        # Annoyingly, relative dirs are not parsed well by parse_fs_url
        protocol = self._get_protocol()
        if protocol in self.run_info.open_fses:
            # So we see if it's got an appropriate url before trying:
            parsed = parse_fs_url(self.path)
            return (None, self.run_info.open_fses[protocol], parsed.resource)

        fname = get_fname(self.path)
        truncated_path = get_truncated_path(self.path, fname)
        return (open_fs(truncated_path), None, fname)

    def _get_protocol(self):
        return self.path.split("://")[0]