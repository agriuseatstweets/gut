import pickle
import os


def load_pickle(fp):
    with open(fp, 'rb') as f:
        return pickle.load(f)


def dump_pickle(fp, obj):
    with open(fp, 'wb') as f:
        pickle.dump(obj, f)


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


def is_setofstr(x):
    return isinstance(x, set) and all(isinstance(y, str) for y in x)
