#!/usr/bin/env python
#
# Author: Lei Xu <eddyxu@gmail.com>

"""
The basic building block of running VSFS and benchmarks on EC2
"""

from __future__ import print_function
import boto
from fabric.api import env, sudo, settings
from fabric.colors import yellow
import os
import time

INSTALL_SCRIPT = \
    'https://raw.github.com/vsfs/vsfs-devtools/master/install-devbox.sh'


class VsfsEC2:
    """Run VSFS on Amazon EC2

    It requires that users define AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    as environment variables.
    """
    EC2_REGION = 'us-east-1'
    EC2_AMI = 'ami-e325768a'  # Ubuntu 13.10 instance
    EC2_KEY_FILE = '~/.ssh/ec2.pem'
    EC2_SECURITY_GROUPS = ['quick-start-1']
    EC2_USER = "ubuntu"

    def __init__(self, **kwargs):
        # Set running configuration.
        self.region = kwargs.get('region', self.EC2_REGION)
        self.ami = kwargs.get('ami', self.EC2_AMI)
        self.user = kwargs.get('user', self.EC2_USER)
        self.security_groups = kwargs.get('security_groups',
                                          self.EC2_SECURITY_GROUPS)

        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.aws_key_file = os.environ.get('AWS_KEY_FILE', self.EC2_KEY_FILE)

        self.conn = boto.ec2.connect_to_region(
            self.region, aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        if not self.conn:
            raise RuntimeError("Failed to initialize connection to EC2.")

    def create_image(self):
        """Creates a AMI image that contains a VSFS binary that is compiled
        against HEAD.
        """
        print(yellow("Create a new instance."))

        self.conn.run_instances(self.ami, instance_type='m1.small',
                                security_groups=self.security_groups,
                                key_name='eddy')

        reservation = self.conn.get_all_instances()
        print(reservation)

    def provision_image(self):
        reservations = self.conn.get_all_instances()
        for reservation in reservations:
            instances = reservation.instances
            if len(instances) != 1 or instances[0].state == 'terminated':
                print('Skip reservation: ', reservation)
                continue
            instance = instances[0]
            while instance.state == 'pending':
                print(yellow('Waitting for booting...'))
                time.sleep(5)
            print(instance, instance.state, instance.ip_address)

            env.host_string = "%s" % instance.ip_address
            with settings(user='ubuntu', key_filename=self.aws_key_file):
                sudo('wget %s -O- | sh' % INSTALL_SCRIPT, shell=True)

    def terminate_instance(self):
        pass
