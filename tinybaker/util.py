from .workarounds import is_fileset


def get_files_in_path_dict(pathdict):
    retval = set()
    for tag in pathdict:
        if is_fileset(tag):
            for file in pathdict[tag]:
                retval.add(file)
        else:
            retval.add(pathdict[tag])
    return retval