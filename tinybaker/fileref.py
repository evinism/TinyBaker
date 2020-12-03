from fs import open_fs
from fs.opener.parse import parse_fs_url
from .exceptions import SeriousErrorThatYouShouldOpenAnIssueForIfYouGet


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
    def __init__(self, path, read_bit, write_bit, run_info):
        self.path = path
        self._read = read_bit
        self._write = write_bit
        self.opened = False
        self.run_info = run_info

    def exists(self):
        filesystem, previously_opened_filesystem, resource = self._get_fs_and_path()
        if filesystem:
            with filesystem:
                return filesystem.exists(resource)
        return previously_opened_filesystem.exists(resource)

    def _open_helper(self, bin=False):
        self.opened = True

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

    def open(self):
        return self._open_helper()

    def openbin(self):
        return self._open_helper(True)

    def _get_fs_and_path(self):
        # Annoyingly, relative dirs are not parsed well by parse_fs_url
        protocol = self.path.split("://")[0]
        if protocol in self.run_info.open_fses:
            # So we see if it's got an appropriate url before trying:
            parsed = parse_fs_url(self.path)
            return (None, self.run_info.open_fses[protocol], parsed.resource)

        fname = get_fname(self.path)
        truncated_path = get_truncated_path(self.path, fname)
        return (open_fs(truncated_path), None, fname)
