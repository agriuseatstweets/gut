import os

def get_abs_fps(dir_p, ext=None):
    '''returns absolute paths for all files with extension ext
    in directory dir_p'''
    for p, _, fns in os.walk(dir_p):
        for f in fns:
            if ext == None or os.path.splitext(f)[1] == ext:
                yield os.path.abspath(os.path.join(dir_p, f))

def safe_get(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct

def strip_list(l):
    return [x.strip() for x in l]
