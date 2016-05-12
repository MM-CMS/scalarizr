#!/usr/bin/python
import json
import glob
import os
import subprocess


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


def publish(reponum, name_patrn, repo):
    index = {}
    packages_list = []
    for filename in glob.glob(os.path.join(CURRENT_DIR, name_patrn)):
        print filename
        pkgname = os.path.split(filename)[1]
        packages_list.append({
            'name': 'mock-scalarizr',
            'version': pkgname.lstrip('mock-sclarizr_').rstrip('.msi'),
            'arch': 'x86_64',
            'path': pkgname,
            'md5': subprocess.check_output('md5 {0}'.format(pkgname), shell=True).split('=')[1].strip(' \n')
        })
    index = {
        "repository": 'repo{0}'.format(num),
        "packages": packages_list
    }
    print index
    with open('index', 'w+') as fp:
        fp.write(json.dumps(index))
    subprocess.check_call('s3cmd del -f  {0}/*'.format(repo), shell=True)
    subprocess.check_call('s3cmd put -f --acl-public {0} {1}'.format(name_patrn, repo), shell=True)
    subprocess.check_call('s3cmd put -f --acl-public index {1}'.format(name_patrn, repo), shell=True)


for num in ('10','20','30'):
    publish(num,
            'mock-scalarizr_{0}.0.*'.format(num),
            's3://scalr-labs/fixtures/scalarizer/pkgmgr/repo{0}/win/'.format(num[0]))
