import os
from setuptools import setup, find_packages


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as fh:
    readme = fh.read()

setup(
    name='huey-sqlalchemy',
    version=__import__('huey_sqlalchemy').__version__,
    description='Huey Storage using SQLAlchemy',
    long_description=readme,
    author='Israël Hallé',
    author_email='israel.halle@flare.systems',
    url='http://github.com/Flared/huey-sqlalchemy/',
    packages=find_packages(),
    install_requires=[
        'huey>=2.0',
        'sqlalchemy>=1.2',
    ],
    tests_require=[
        'mypy',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
