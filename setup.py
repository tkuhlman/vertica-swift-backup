from setuptools import setup, find_packages

setup(
    name="vertica-swift-backup",
    version="1.1.1",
    author="Tim Kuhlman",
    author_email="tim@backgroundprocess.com",
    description="A script to backup/restore Vertica to OpenStack Swift.",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Topic :: Database",
        "Topic :: System :: Archiving :: Backup"
    ],
    license="BSD",
    keywords="vertica swift openstack cloud backup",
    url="https://github.com/tkuhlman/vertica-swift-backup",
    test_suite="nose.collector",
    install_requires=["setuptools", "python-swiftclient", "python-keystoneclient", "PyYAML"],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    data_files=[('share/vertica-swift-backup/examples', ['backup.yaml-example']),
                ('share/vertica-swift-backup/restore', ['restore/README.md', 'restore/fabfile.py', 'restore/vertica.py'])],
    entry_points={
        'console_scripts': [
            'vertica_backup = vertica_backup.backup:main',
            'vertica_restore_download = vertica_backup.restore_download:main'
        ]
    }
)
