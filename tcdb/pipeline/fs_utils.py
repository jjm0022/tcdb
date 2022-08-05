import pathlib
import filecmp
import gzip
import shutil
from datetime import datetime
from loguru import logger

now = datetime.now()

def isContentsUnique(file_path, cmp_files):
    """
    Checks to see if the contents `file_path` is the same as any of the files in `cmp_files`.
    If a matching file is found it is removed and False is returned
    If no match is found True is returned
    """
    for cmp_file in cmp_files:
        if filecmp.cmp(file_path, cmp_file):
            logger.debug(f"{file_path.name} is the same as {cmp_file.as_posix()}")
            file_path.unlink()
            return False
    return True

def removeDuplicateFiles(directory_path, glob_pattern):
    assert isinstance(directory_path, pathlib.Path), f"expected `directory_path` to be an instance of pathlib.Path not {type(directory_path)}"
    assert isinstance(glob_pattern, str), f"expected `glob_pattern` to be a str not {type(glob_pattern)}"

    # get list of unique file prefixes
    file_prefixes = set(['_'.join(file_path.name.split('_')[:-1]) for file_path in directory_path.glob(glob_pattern)])

    total_removed = 0

    for file_prefix in file_prefixes:
        removed_files = 0
        kept_files = 0
        for ind, file_path in enumerate(sorted(directory_path.glob(f"{file_prefix}{glob_pattern}"))):
            if ind == 0:
                logger.trace(f"{file_path.name} is unique")
                diff_files = [file_path]
                kept_files += 1
                continue
            else:
                # if conents of file_path are the same as the last file in diff_list remove it, otherwise do nothing
                if filecmp.cmp(diff_files[-1], file_path, shallow=False):
                    logger.trace(f"{file_path.name} is the same as {diff_files[-1].name}")
                    file_path.unlink()
                    removed_files += 1
                else:
                    logger.trace(f"{file_path.name} is unique")
                    diff_files.append(file_path)
                    kept_files += 1

        total_removed = total_removed + removed_files
        logger.debug(f"Keeping {kept_files} for prefix {file_prefix}")
        logger.debug(f"Removed {removed_files} files for prefix {file_prefix}")

    logger.info(f"Removed a total of {total_removed} duplicate files")

def extractGZip(in_path, out_dir, remove=False):
    out_name = in_path.stem
    out_path = out_dir.joinpath(out_name)
    with gzip.open(in_path, 'rb') as g:
        with open(out_path, 'wb') as f:
            shutil.copyfileobj(g, f)
    if remove:
        logger.trace(f"Removing {in_path.as_posix()}")
        in_path.unlink()
    return out_path