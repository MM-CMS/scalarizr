import json
import re
import logging

from agent.scalrinit import metadata, datadir
from common.utils import mixutil, exc
from common.utils.facts import fact


LOG = logging.getLogger()


class Error(Exception):
    pass


class Userdata(dict):
    @classmethod
    def from_string(cls, data):
        return Userdata(re.findall("([^=]+)=([^;]*);?", data))

    @classmethod
    def from_json_string(cls, data):
        return Userdata(json.loads(data))


@mixutil.memorize
def provider():
    res = datadir.result()
    if res:
        pvds =[p for p in metadata._providers if res['datasource'] == p.name]
        if pvds:
            return pvds[0]
    raise Error('No Meta-data provider was found. Scalr-Init result: {}.\n{}'
        .format(res, _errmsg_check_updclient()))


@mixutil.memorize
def instance_id():
    return metadata.instance_id(provider())


@mixutil.memorize
def user_data():
    user_data = datadir.user_data()
    if provider().name in ['openstack', 'configdrive']:
        return Userdata.from_json_string(user_data)['meta']
    return Userdata.from_string(user_data)


def wait(timeout=0):
    try:
        datadir.result.wait(timeout)
    except exc.UserTimeoutError:
        raise Error('Scalr-Init completion flag {} still not available after {} seconds.\n{}'
            .format(datadir.result.link_path, timeout, _errmsg_check_updclient()))
    res = datadir.result()
    try:
        pvd =[p for p in metadata._providers if res['datasource'] == p.name][0]
    except IndexError:
        raise Error('No Meta-data provider was found. Scalr-Init result: {}.\n{}'
            .format(res, _errmsg_check_updclient()))
    else:
        metadata.provider = pvd



def _errmsg_check_updclient():
    svs = ('ScalrUpdClient' if fact['os']['family'] == 'windows'
            else 'scalr-upd-client')
    return 'Check that {} service has no errors in log.'.format(svs)
