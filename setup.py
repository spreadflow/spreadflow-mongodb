from setuptools import setup

tests_require = [
    'SpreadFlowDelta[tests]',
    'coveralls',
    'mock',
    'testtools'
]

setup(
    name='SpreadFlowMongoDB',
    version='0.0.1',
    description='MongoDB support for SpreadFlow metadata extraction and processing engine',
    author='Lorenz Schori',
    author_email='lo@znerol.ch',
    url='https://github.com/znerol/spreadflow-mongodb',
    packages=[
        'spreadflow_mongodb',
        'spreadflow_mongodb.test'
    ],
    install_requires=[
        'SpreadFlowCore',
        'pymongo'
    ],
    tests_require=tests_require,
    extras_require={
        'tests': tests_require
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: Multimedia'
    ]
)
