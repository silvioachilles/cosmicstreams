import numpy as np


KEY_SHAPE_Y = 'shape_y'
KEY_SHAPE_X = 'shape_x'
KEY_DTYPE = 'dtype'
KEY_BYTEORDER = 'byteorder'
KEY_ORDER = 'order'
KEY_IDENT = 'ident'
KEY_INDEX = 'index'


def get_array_order(array: np.ndarray):
    if array.flags['C_CONTIGUOUS']:
        return 'C'
    elif array.flags['F_CONTIGUOUS']:
        return 'F'
    else:
        raise ValueError('Array is neither C nor F contiguous')


def get_array_byteorder(array: np.ndarray):
    if array.dtype.byteorder == '=':
        return '<'
    elif array.dtype.byteorder == '<':
        return '<'
    elif array.dtype.byteorder == '>':
        return '>'
    else:
        raise ValueError('Array has unknown byteorder')


def get_array(bytesequence, dtype, shape_y, shape_x, byteorder, order):
    dt = np.dtype(dtype)
    dt = dt.newbyteorder(byteorder)
    array = np.frombuffer(bytesequence, dtype=dt)
    array = array.reshape((shape_y, shape_x), order=order)

    return array


def get_array_metadata(array: np.ndarray, metadata=None):
    if metadata is None:
        metadata = {}

    metadata.update({
        KEY_SHAPE_Y: array.shape[0],
        KEY_SHAPE_X: array.shape[1],
        KEY_DTYPE: array.dtype.name,
        KEY_BYTEORDER: get_array_byteorder(array),
        KEY_ORDER: get_array_order(array),
    })

    return metadata
