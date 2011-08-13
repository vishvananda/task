# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 Vishvananda Ishaya
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

try:
    from setuptools import setup
except:
    from distutils.core import setup

config = {
    'description': 'Save python methods as tasks and run them again',
    'author': 'Vishvananda Ishaya',
    'url': 'https://github.com/vishvananda/task',
    'author_email': 'vishvananda@gmail.com',
    'version': '0.1',
    'install_requires': ['nose', 'sqlalchemy'],
    'packages': ['task'],
    'scripts': [],
    'name': 'task',
}

setup(**config)
