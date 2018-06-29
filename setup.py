from codecs import open
from os import path

from setuptools import setup

basedir = path.abspath(path.dirname(__file__))

with open(path.join(basedir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='simple-amqp-pubsub',
    version='0.4.2',
    description='Simple AMQP Pub/Sub lib',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/rudineirk/py-simple-amqp-pubsub',
    author='Rudinei Goi Roecker',
    author_email='rudinei.roecker@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='simple amqp pubsub',
    packages=['simple_amqp_pubsub', 'simple_amqp_pubsub.base'],
    install_requires=[
        'simple_amqp>=0.2.2',
        'msgpack',
    ],
    extras_require={
        'asyncio': [
            'aio-pika',
        ],
        'gevent': [
            'gevent',
        ]
    },
)
