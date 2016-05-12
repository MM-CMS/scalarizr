import os
import sys

from mock import Mock

sys.modules['requests'] = Mock()

from scalarizr.updclient import pkgmgr

fixtures_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/../../fixtures/updclient/')


class TestPkgMgr(object):
    def test_updatedb_yum(self):
        repomd_file = os.path.join(fixtures_dir, 'repomd.xml')
        primary_file = os.path.join(fixtures_dir, 'primary.xml.gz')

        def get(query):
            file = repomd_file if query.endswith("repomd.xml") else primary_file
            result = Mock()
            result.text = open(file).read()
            result.content = open(file, 'rb').read()
            return result
        sys.modules['requests'].get = Mock(side_effect=get)

        mgr = pkgmgr.YumManager("http://rpm.scalr.net/rpm/rhel/latest/x86_64/")
        mgr.updatedb()
        packages = mgr.packages

        assert len(packages) == 74
        assert packages.get('scalarizr-base')

    def test_updatedb_apt(self):
        release_file = os.path.join(fixtures_dir, 'Release')
        packages_file = os.path.join(fixtures_dir, 'Packages')

        def get(query):
            file = release_file if query.endswith("Release") else packages_file
            result = Mock()
            result.text = open(file).read()
            result.content = open(file, 'rb').read()
            return result
        sys.modules['requests'].get = Mock(side_effect=get)

        mgr = pkgmgr.AptManager("http://apt.scalr.net/debian master/")
        mgr.updatedb()
        packages = mgr.packages

        assert len(packages) == 14
        assert packages.get('scalarizr')
