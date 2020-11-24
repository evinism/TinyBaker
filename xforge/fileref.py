from os import path


class FileRef:
    def __init__(self, path, read_bit, write_bit):
        self.path = path
        self._read = read_bit
        self._write = write_bit
        self.opened = False

    def exists(self):
        return path.exists(self.path)

    def open(self):
        self.opened = True
        mode = ""
        if self._read:
            mode = mode + "r"
        if self._write:
            mode = mode + "w"
        return open(self.path, mode)