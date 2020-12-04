from .workarounds.annot import is_fileset


def affected_files_for_transform(transform):
    return set.union(
        get_files_in_path_dict(transform.input_paths),
        get_files_in_path_dict(transform.output_paths),
    )


def get_files_in_path_dict(pathdict):
    retval = set()
    for tag in pathdict:
        if is_fileset(tag):
            for file in pathdict[tag]:
                retval.add(file)
        else:
            retval.add(pathdict[tag])
    return retval