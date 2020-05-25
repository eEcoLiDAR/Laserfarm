import os

from setuptools import setup, find_packages


def read(file_name):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


version = {}
exec(read('laserfarm/__version__.py'), version)
required = read('requirements.txt').splitlines()


setup(
    name='laserfarm',
    version=version['__version__'],
    description=('Laserchicken Framework for Applications '
                 'in Research in Macro-ecology'),
    author='Netherlands eScience Center',
    author_email='team-atlas@esciencecenter.nl',
    license='Apache 2.0',
    keywords=['Python', 'Point cloud'],
    url='https://github.com/eEcoLiDAR/Laserfarm',
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
            'laserfarm = laserfarm.main:main',
        ],
    }
)
