import os
from setuptools import setup, find_packages


def make_data_files(dst, src):
    ret = []
    for directory, _, files in os.walk(src):
        if not directory.startswith("."):
            ret.append([
                    directory.replace(src, dst),
                    list(os.path.join(directory, f) for f in files)
            ])
    return ret

description = "Scalarizr converts any server to Scalr-manageable node"

data_files = make_data_files('/etc/scalr', 'etc')
data_files.extend(make_data_files('/usr/share/scalr', 'share'))
data_files.extend(make_data_files('/usr/local/scalarizr/scripts', 'scripts'))


cfg = dict(
        name = "scalarizr",
        version = open('src/scalarizr/version').read().strip(),
        description = description,
        long_description = description,
        author = "Scalr Inc.",
        author_email = "info@scalr.net",
        url = "https://scalr.net",
        license = "GPL",
        platforms = "any",
        package_dir = {"" : "src"},
        packages = find_packages("src"),
        include_package_data = True,
        requires = ["boto"],
        data_files = data_files,
        entry_points = {
            'console_scripts': [
                'scalr-upd-client = scalarizr.updclient.app:main',
                'scalarizr = scalarizr.app:main',
                'szradm = scalarizr.adm.app:main'
            ]
        }
)
setup(**cfg)












