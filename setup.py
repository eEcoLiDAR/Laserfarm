import os

from setuptools import setup, find_packages


def read(file_name):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


required = read('requirements.txt').splitlines()


setup(
    name='lc_macro_pipeline',
    version='0.1',
    description='Point cloud toolkit for macroecology applications',
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
