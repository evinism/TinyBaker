FILESET_ANNOT = "fileset::"


def get_annotation(tag):
    if "::" in tag:
        return tag.split("::")[0]
    else:
        return "file"


def fileset(tag):
    if not is_fileset(tag):
        tag = FILESET_ANNOT + tag
    return tag


def is_fileset(tag: str):
    return tag.startswith(FILESET_ANNOT)
