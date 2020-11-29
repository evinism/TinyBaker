from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="tinybaker",
    version="0.1.1",
    author="Evin Sellin",
    author_email="evinism@gmail.com",
    description="Lightweight file-to-file build tool built for production workloads",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/tinybaker",
    packages=find_packages(),
    install_requires=["fs"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
