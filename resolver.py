#!/usr/bin/env python
# -*- coding: utf-8 -*-

from consul import Consul


class ConsulResolver(object):

    def __init__(self, target):
        pair = target.split(":")
        self.client = Consul(pair[0], int(pair[1])) if len(pair) == 2 else Consul(target)

    def resolve(self, service_name):
        if not service_name:
            raise ValueError('xrpc: no service name provided')

        result = self.client.catalog.service(service_name)
        return [(n['ServiceAddress'], n['ServicePort']) for n in result[1]]
