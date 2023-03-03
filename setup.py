from distutils.core import setup

setup(
    name='cosmicstreams',
    version='0.1.0',
    packages=['cosmicstreams', 'cosmicstreams.sockets'],
    author='Silvio Achilles',
    author_email='silvio.achilles@desy.de',
    description='Python package containing the interfaces to stream diffraction data from the preprocessor to '
                'ptychocam.'
)
