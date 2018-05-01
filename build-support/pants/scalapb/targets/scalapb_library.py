#!/bin/python
from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
    unicode_literals, with_statement)

import logging

from pants.backend.jvm.targets.import_jars_mixin import ImportJarsMixin
from pants.backend.jvm.targets.jvm_target import JvmTarget
from pants.base.payload import Payload
from pants.base.payload_field import PrimitiveField

import os


logger = logging.getLogger(__name__)

class ScalaPBLibrary(ImportJarsMixin, JvmTarget):
    """A Java library generated from Protocol Buffer IDL files."""

    def __init__(self,
        payload=None,
        imports=None,
        java_conversions=False,
        flat_package=False,
        grpc=True,
        single_line_to_string=False,
        source_root=None,
        **kwargs):
        payload = payload or Payload()
        payload.add_fields({
            'java_conversions': PrimitiveField(java_conversions),
            'flat_package': PrimitiveField(flat_package),
            'grpc': PrimitiveField(grpc),
            'single_line_to_string': PrimitiveField(single_line_to_string),
            'import_specs': PrimitiveField(imports or ()),
            'source_root': PrimitiveField(source_root or '.')
        })
        super(ScalaPBLibrary, self).__init__(payload=payload, **kwargs)

    @classmethod
    def imported_jar_library_spec_fields(cls, kwargs=None, payload=None):
        """List of JarLibrary specs to import.
        Required to implement the ImportJarsMixin.
        """
        yield ('imports', 'import_specs')

    @property
    def source_root(self):
        return os.path.normpath(
            os.path.join(self.target_base, self.payload.source_root))