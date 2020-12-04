FILESET_ANNOT = "fileset::"


def fileset(tag):
    if not is_fileset(tag):
        tag = FILESET_ANNOT + tag
    return tag


def is_fileset(tag: str):
    return tag.startswith(FILESET_ANNOT)
