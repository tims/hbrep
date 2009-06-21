from setuptools import setup, find_packages


name = "hbrep"
version = "0.1"
description = "Postgresql to HBase replication tool."
maintainer = "Tim Sell"
executable = False  # requires lfm.name.main()
requires = []  # can be left empty


###########################################
# probably no need to edit anything below #
###########################################

kwargs = {
    "name": "lfm." + name,
    "version": version,
    "description": description,
    "maintainer": maintainer,
    "long_description": open("README.txt").read(),
    "packages": find_packages(exclude=["tests", "tests.*"]),
    "namespace_packages": ["lfm"],
    "install_requires": requires,
    "test_suite": "nose.collector",
    "tests_require": ["nose"]
}
if executable:
    kwargs["entry_points"] = {
        "console_scripts": [
            "lfm." + name + " = lfm." + name + ":main",
        ],
        "setuptools.installation": [
            "eggsecutable = lfm." + name + ":main",
        ]
    }
setup(**kwargs)
