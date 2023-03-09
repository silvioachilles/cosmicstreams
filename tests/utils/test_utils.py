import numpy as np

from cosmicstreams.utils import utils


class Test_utils:
    def test_get_array(self):
        array = np.random.random([2, 2])

        array_metadata = utils.get_array_metadata(array)

        array2 = utils.get_array(
            array.data,
            array_metadata[utils.K.KEY_DTYPE],
            array_metadata[utils.K.KEY_SHAPE_Y],
            array_metadata[utils.K.KEY_SHAPE_X],
            array_metadata[utils.K.KEY_BYTEORDER],
            array_metadata[utils.K.KEY_ORDER]
        )

        assert np.array_equal(array, array2)

    def test_get_byteorder(self):
        array = np.random.random([2, 2])

        byteorder = utils.get_array_byteorder(array)

        assert byteorder == '<'
        assert not byteorder == '>'
        assert not byteorder == '='

    def test_get_array2(self):
        dt = np.dtype(float)
        dt = dt.newbyteorder('>')

        array = np.random.random([2, 2])
        array = array.astype(dt)

        array_metadata = utils.get_array_metadata(array)

        array2 = utils.get_array(
            array.data,
            array_metadata[utils.K.KEY_DTYPE],
            array_metadata[utils.K.KEY_SHAPE_Y],
            array_metadata[utils.K.KEY_SHAPE_X],
            array_metadata[utils.K.KEY_BYTEORDER],
            array_metadata[utils.K.KEY_ORDER]
        )

        assert np.array_equal(array, array2)
