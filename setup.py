from setuptools import setup, find_packages


setup(
    name="z3",
    version="0.1",
    platforms='any',
    packages=find_packages(),
    include_package_data=True,
    install_requires=["boto==2.28.0"],
    author="PressLabs SRL",
    author_email="contact@presslabs.com",
    url="https://github.com/presslabs/z3",
    description="z3",
    entry_points={
        'console_scripts': [
            'pput = z3.pput:main',
            'z3 = z3.snap:main',
            'z3_get = z3.get:main'
        ]
    },
    test_requirements=["pytest>=2.8.5"]
)
