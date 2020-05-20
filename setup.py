import os

from setuptools import setup, find_packages


def read(file_name):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


version = {}
exec(read('lc_macro_pipeline/__version__.py'), version)
required = read('requirements.txt').splitlines()


setup(
    name='lc_macro_pipeline',
    version=version['__version__'],
    description=('lcMacroPipeline provides a FOSS wrapper to Laserchicken '
                 'supporting the use of massive LiDAR point cloud data sets '
                 'for macro-ecology, from data preparation to scheduling and '
                 'execution of distributed processing across a cluster of '
                 'compute nodes.'),
    author='Netherlands eScience Center',
    author_email='team-atlas@esciencecenter.nl',
    license='Apache 2.0',
    keywords=['Python', 'Point cloud'],
    url='https://github.com/eEcoLiDAR/lcMacroPipeline',
    packages=find_packages(exclude=["tests"]),
    install_requires=required,
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
    ],
    entry_points={
        'console_scripts': [
            'lc_macro_pipeline = lc_macro_pipeline.main:main',
        ],
    }
)
