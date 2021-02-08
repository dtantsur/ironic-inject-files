# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import contextlib
import logging
import os
import tempfile

from ironic_lib import disk_utils
from ironic_lib import utils
from oslo_concurrency import processutils

from ironic_python_agent import hardware

LOG = logging.getLogger(__name__)


def partition_index_to_name(device, index):
    # The partition delimiter for all common harddrives (sd[a-z]+)
    part_delimiter = ''
    if 'nvme' in device:
        part_delimiter = 'p'
    return device + part_delimiter + str(index)


@contextlib.contextmanager
def partition_with_path(path):
    root_dev = hardware.dispatch_to_managers('get_os_install_device')
    partitions = disk_utils.list_partitions(root_dev)
    local_path = tempfile.mkdtemp()

    for part in partitions:
        if 'esp' in part['flags'] or 'lvm' in part['flags']:
            LOG.debug('Skipping partition %s', part)
            continue

        part_path = partition_index_to_name(root_dev, part['number'])
        try:
            with utils.mounted(part_path) as local_path:
                found_path = os.path.join(local_path, path)
                LOG.debug('Checking for path %s on %s', found_path, part_path)
                if not os.path.isdir(found_path):
                    continue

                LOG.info('Path found: /%s on %s', found_path, part_path)
                yield found_path
                return
        except processutils.ProcessExecutionError as exc:
            LOG.warning('Failure when inspecting partition %s: %s', part, exc)

    raise RuntimeError("No partition found with path %s, scanned: %s"
                       % (path, partitions))


class InjectFilesHardwareManager(hardware.HardwareManager):

    HARDWARE_MANAGER_NAME = 'InjectFilesHardwareManager'
    HARDWARE_MANAGER_VERSION = '1'

    def evaluate_hardware_support(self):
        return hardware.HardwareSupport.SERVICE_PROVIDER

    def get_deploy_steps(self, node, ports):
        return [
            {
                'interface': 'deploy',
                'step': 'inject_files',
                'priority': 0,
                'reboot_requested': False,
                'abortable': True,
                'argsinfo': {
                    'files': {
                        'required': True,
                        'description': 'Mapping between file paths and their '
                                       'base64 encoded contents'
                    }
                }
            }
        ]

    def inject_files(self, node, ports, files):
        with partition_with_path('etc') as path:
            for dest, content in files.items():
                content = base64.b64decode(content)
                fname = os.path.normpath(
                    os.path.join(path, '..', dest.lstrip('/')))
                LOG.info('Injecting %s into %s', dest, fname)
                with open(fname, 'wb') as fp:
                    fp.write(content)
