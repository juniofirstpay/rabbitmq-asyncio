from setuptools import setup, find_packages

setup(
    name='rbmq-client',
    packages=['rbmq_client', 'rbmq_aio_client'],
    version='0.2.0',
    author="Develper Junio",
    author_email='developer@junio.in',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
    ],
    description="Zeta Microservice Service Client",
    license="MIT license",
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "addict",
        "aio-pika==8.3.0",
        "structlog"
    ]
)
