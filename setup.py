from setuptools import setup, find_packages


setup(
    name="z3",
    version="0.2.0",
    platforms='any',
    packages=find_packages(),
    include_package_data=True,
    install_requires=["boto"],
    author="PressLabs SRL",
    author_email="contact@presslabs.com",
    url="https://github.com/presslabs/z3",
    description="Backup ZFS snapshots to S3",
    entry_points={
        'console_scripts': [
            'pput = z3.pput:main',
            'z3 = z3.snap:main',
            'z3_get = z3.get:main',
            'z3_ssh_sync = z3.ssh_sync:main'
        ]
    },
    keywords='ZFS backup',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Utilities",
    ],
)
