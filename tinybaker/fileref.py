from fs import open_fs
import os.path


def _protocol_aware_split(path):
    arr = path.split("://")
    if len(arr) == 2:
        protocol, standard_path = arr
    else:
        protocol = None
        standard_path = path
    directory, fname = os.path.split(standard_path)
    if protocol:
        directory = "{}://{}".format(protocol, directory)
    return directory, fname


class FileRef:
    def __init__(self, path, read_bit, write_bit):
        self.path = path
        self._read = read_bit
        self._write = write_bit
        self.opened = False

    def exists(self):
        directory, fname = _protocol_aware_split(self.path)
        with open_fs(directory) as filesystem:
            return filesystem.exists(fname)

    def open(self):
        self.opened = True
        directory, fname = _protocol_aware_split(self.path)
        with open_fs(directory) as filesystem:
            mode = ""
            if self._read:
                mode = mode + "r"
            if self._write:
                mode = mode + "w"
            return filesystem.open(fname, mode)
