"""
Nova virt driver for direct Xen xl command management.
Implements basic VM lifecycle operations using xl commands directly
instead of going through XenAPI.

Requirements:
- Ubuntu 22.04
- Xen 4.18.1-pre
- python-libvirt
- Nova 2023.2
"""

import os
import json
import subprocess
from typing import Dict, List, Optional

from oslo_log import log as logging
from oslo_utils import units
from oslo_utils import uuidutils

from nova.compute import power_state
from nova.compute import vm_states
from nova import exception
from nova.virt import driver
from nova.virt import hardware
from nova.virt.libvirt import config as vconfig

LOG = logging.getLogger(__name__)

# Map xl domain states to Nova power states
POWER_STATE_MAP = {
    'running': power_state.RUNNING,
    'blocked': power_state.RUNNING,
    'paused': power_state.PAUSED,
    'shutdown': power_state.SHUTDOWN,
    'crashed': power_state.CRASHED,
    'dying': power_state.CRASHED
}


class XLDirectDriver(driver.ComputeDriver):
    """Xen xl command-line driver for Nova."""

    def __init__(self, virtapi):
        super(XLDirectDriver, self).__init__(virtapi)
        self._path_xl = '/usr/local/sbin/xl'
        # Validate xl binary exists
        if not os.path.exists(self._path_xl):
            msg = _("xl binary not found at %s") % self._path_xl
            raise exception.NovaException(msg)

    def _execute_xl(self, *cmd, **kwargs) -> subprocess.CompletedProcess:
        """Execute xl command and return CompletedProcess object."""
        xl_cmd = [self._path_xl] + list(cmd)
        LOG.debug("Executing: %s", ' '.join(xl_cmd))
        try:
            return subprocess.run(
                xl_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
                **kwargs
            )
        except subprocess.CalledProcessError as e:
            LOG.error("xl command failed: %s", e.stderr)
            raise exception.NovaException(e.stderr)

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, allocations, network_info=None,
              block_device_info=None, power_on=True):
        """Create a new VM instance."""
        instance_name = instance.name
        instance_dir = os.path.join('/etc/xen', instance_name)

        # Create instance directory
        os.makedirs(instance_dir, exist_ok=True)

        # Generate xl config file
        cfg = self._get_xl_config(instance, image_meta, network_info)
        cfg_path = os.path.join(instance_dir, 'config')
        with open(cfg_path, 'w') as f:
            f.write(cfg)

        # Create domain
        self._execute_xl('create', cfg_path)

        if not power_on:
            self._execute_xl('pause', instance_name)

    def _get_xl_config(self, instance, image_meta, network_info) -> str:
        """Generate xl config file content."""
        memory_mb = instance.flavor.memory_mb
        vcpus = instance.flavor.vcpus

        cfg = [
            f"name = '{instance.name}'",
            f"memory = {memory_mb}",
            f"vcpus = {vcpus}",
            f"uuid = '{instance.uuid}'",
            "builder = 'hvm'",
            "boot = 'c'",
            "acpi = 1",
            "apic = 1",
            f"disk = ['phy:/dev/vg0/{instance.name},hda,w']",
            "vnc = 1",
            "vnclisten = '0.0.0.0'"
        ]

        return '\n'.join(cfg)

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True):
        """Destroy the specified instance."""
        try:
            self._execute_xl('destroy', instance.name)
        except Exception as e:
            LOG.warning("Instance destroy failed: %s", e)

        if destroy_disks:
            instance_dir = os.path.join('/etc/xen', instance.name)
            try:
                subprocess.run(['rm', '-rf', instance_dir], check=True)
            except subprocess.CalledProcessError as e:
                LOG.warning("Failed to remove instance directory: %s", e)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        """Reboot the specified instance."""
        if reboot_type == 'SOFT':
            self._execute_xl('reboot', instance.name)
        else:
            self._execute_xl('reset', instance.name)

    def get_info(self, instance, use_cache=True) -> Dict:
        """Get info about a Xen domain."""
        try:
            out = self._execute_xl('list', '-l', instance.name)
            data = json.loads(out.stdout)

            if not data:
                raise exception.InstanceNotFound(instance_id=instance.uuid)

            domain_info = data[0]
            state = domain_info['state']

            return {
                'state': POWER_STATE_MAP.get(state, power_state.NOSTATE),
                'max_mem': domain_info['maxmem'] * units.Ki,
                'mem': domain_info['mem'] * units.Ki,
                'num_cpu': domain_info['vcpus'],
                'cpu_time': 0
            }
        except Exception:
            LOG.exception("Error getting instance info")
            raise exception.InstanceNotFound(instance_id=instance.uuid)

    def list_instances(self) -> List[str]:
        """Return names of all instances known to hypervisor."""
        out = self._execute_xl('list')
        instances = []

        # Skip header line
        for line in out.stdout.splitlines()[1:]:
            name = line.split()[0]
            instances.append(name)

        return instances

    def get_available_resource(self, nodename) -> Dict:
        """Retrieve resource info from Xen hypervisor."""
        memory_info = self._execute_xl('info', '-n')

        # Parse memory info
        total_memory_kb = 0
        free_memory_kb = 0
        for line in memory_info.stdout.splitlines():
            if 'total_memory' in line:
                total_memory_kb = int(line.split(':')[1].strip())
            elif 'free_memory' in line:
                free_memory_kb = int(line.split(':')[1].strip())

        # Get CPU info
        cpu_info = self._execute_xl('info', '-n', 'cpu')
        num_cpus = len(cpu_info.stdout.splitlines())

        return {
            'vcpus': num_cpus,
            'vcpus_used': 0,
            'memory_mb': total_memory_kb // 1024,
            'memory_mb_used': (total_memory_kb - free_memory_kb) // 1024,
            'local_gb': 0,
            'local_gb_used': 0,
            'disk_available_least': 0,
            'hypervisor_type': 'xen',
            'hypervisor_version': 4181,
            'hypervisor_hostname': nodename,
            'cpu_info': '{}',
            'supported_instances': [],
            'numa_topology': None
        }

    def get_host_ip_addr(self) -> str:
        """Get IP address of Xen host."""
        # For testing, return first non-loopback IP
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]

    def attach_interface(self, context, instance, image_meta, vif):
        """Attach network interface to instance."""
        self._execute_xl(
            'network-attach', instance.name,
            f'mac={vif["address"]}',
            f'bridge={vif["network"]["bridge"]}'
        )

    def detach_interface(self, context, instance, vif):
        """Detach network interface from instance."""
        self._execute_xl(
            'network-detach', instance.name,
            str(vif['id'])
        )