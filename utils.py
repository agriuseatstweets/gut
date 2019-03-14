import pickle
import os
import types
from itertools import compress


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


def isin_listofsets(x, listofsets, return_int=False):
    if return_int:
        return (1 if x in s else 0 for s in listofsets)
    else:
        return (x in s for s in listofsets)


def pairwise_combs(a):
    r = []
    for i, x in enumerate(a):
        for j, y in enumerate(a):
            if i < j:
                r += [(x, y)]
    return r


def innerjoin_lists(*lists):
    r_s = set(lists[0])
    for l in lists[1:]:
        l_s = set(l)
        r_s = r_s.intersection(l_s)
    return list(r_s)


def outerjoin_lists(*lists):
    r_s = set()
    for l in lists:
        l_s = set(l)
        r_s = r_s.union(l_s)
    return list(r_s)


def get_idxs_true(l):
    if isinstance(l, types.GeneratorType):
        l = list(l)
    return list(compress(range(len(l)), l))


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def strip_list(l):
    return [x.strip() for x in l]


def getattrs(object, attrs):
    return (getattr(object, x) for x in attrs)
