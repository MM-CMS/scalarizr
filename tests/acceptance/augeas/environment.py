# coding: utf-8

"""
Created on 12.03.2014
@author: Eugeny Kurkovich
"""
import os
import shutil
from behave import *
import logging


PATH = os.path.dirname(__file__)

# AUGEAS_ROOT is a sand-box root directory used instead of environment variable AUGEAS_ROOT
AUGEAS_ROOT = '/tmp/augeas'

# AUGEAS_LENS_LIB is a colon-spearated list of directories that modules should be searched in
AUGEAS_LENS_LIB = os.path.abspath(os.path.join(PATH, '../../../share'))

logging.basicConfig(
    filename='augeas.log',
    level=logging.INFO,
    format='%(asctime)s-20s %(levelname)-8s - %(message)s')
LOG = logging.getLogger(__name__)


def before_all(context):
    setattr(context, 'AUGEAS_ROOT', AUGEAS_ROOT)
    setattr(context, 'AUGEAS_LENS_LIB', AUGEAS_LENS_LIB)


def after_scenario(context, scenario):
    parser = getattr(context, 'parser')
    if os.path.exists(AUGEAS_ROOT):
        shutil.rmtree(AUGEAS_ROOT)
        LOG.info('Augeas sandbox %s was removed properly' % AUGEAS_ROOT)
    parser.close()