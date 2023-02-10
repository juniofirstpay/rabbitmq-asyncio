from setuptools import setup, find_packages

setup(
    name='rbmq-client',
    packages=['rbmq_client', 'rbmq_aio_client'],
    version='2.2.9',
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
        "addict==2.4.0",
        "aio-pika==8.3.0",
        "appnope==0.1.3",
        "asttokens==2.0.5",
        "backcall==0.2.0",
        "black==22.3.0",
        "click==8.1.3",
        "decorator==5.1.1",
        "executing==0.8.3",
        "ipython==8.3.0",
        "jedi==0.18.1",
        "matplotlib-inline==0.1.3",
        "mypy-extensions==0.4.3",
        "parso==0.8.3",
        "pathspec==0.9.0",
        "pexpect==4.8.0",
        "pickleshare==0.7.5",
        "pika==1.2.1",
        "platformdirs==2.5.2",
        "prompt-toolkit==3.0.29",
        "ptyprocess==0.7.0",
        "pure-eval==0.2.2",
        "Pygments==2.12.0",
        "six==1.16.0",
        "stack-data==0.2.0",
        "structlog==21.5.0",
        "tomli==2.0.1",
        "traitlets==5.1.1",
        "typing-extensions==4.2.0",
        "wcwidth==0.2.5"
    ]
)
