#!/bin/python
from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
    unicode_literals, with_statement)

import os
import subprocess
from collections import OrderedDict
from hashlib import sha1

from twitter.common.collections import OrderedSet

from pants.task.simple_codegen_task import SimpleCodegenTask
from pants.backend.jvm.targets.jar_library import JarLibrary
from pants.backend.jvm.targets.scala_library import ScalaLibrary
from pants.backend.jvm.tasks.jar_import_products import JarImportProducts
from pants.backend.jvm.tasks.nailgun_task import NailgunTask
from pants.base.build_environment import get_buildroot
from pants.base.exceptions import TaskError
from pants.build_graph.address import Address
from pants.fs.archive import ZIP

from scalapb.targets.scalapb_library import ScalaPBLibrary


class ScalaPBGen(SimpleCodegenTask, NailgunTask):
    def __init__(self, *args, **kwargs):
        super(ScalaPBGen, self).__init__(*args, **kwargs)

    @classmethod
    def register_options(cls, register):
        super(ScalaPBGen, cls).register_options(register)
        cls.register_jvm_tool(register, 'scalapbc')
        register('--protoc-version', fingerprint=True,
            help='Set a specific protoc version to use.', default='330')

    def synthetic_target_type(self, target):
        return ScalaLibrary

    def is_gentarget(self, target):
        return isinstance(target, ScalaPBLibrary)

    def execute_codegen(self, target, target_workdir):
        sources = target.sources_relative_to_buildroot()

        source_roots = self._calculate_source_roots(target)
        source_roots.update(self._proto_path_imports([target]))

        scalapb_options = []
        if target.payload.java_conversions:
            scalapb_options.append('java_conversions')
        if target.payload.grpc:
            scalapb_options.append('grpc')
        if target.payload.flat_package:
            scalapb_options.append('flat_package')
        if target.payload.single_line_to_string:
            scalapb_options.append('single_line_to_string')

        gen_scala = '--scala_out={0}:{1}'.format(','.join(scalapb_options), target_workdir)

        args = ['-v%s' % self.get_options().protoc_version, gen_scala]

        if target.payload.java_conversions:
            args.append('--java_out={0}'.format(target_workdir))

        for source_root in source_roots:
            args.append('--proto_path={0}'.format(source_root))

        classpath = self.tool_classpath('scalapbc')

        args.extend(sources)
        main = 'scalapb.ScalaPBC'
        result = self.runjava(classpath=classpath, main=main, args=args, workunit_name='scalapb-gen')

        if result != 0:
            raise TaskError('scalapb-gen ... exited non-zero ({})'.format(result))

    def _calculate_source_roots(self, target):
        source_roots = OrderedSet()

        def add_to_source_roots(target):
            if self.is_gentarget(target):
                source_roots.add(target.source_root)
        self.context.build_graph.walk_transitive_dependency_graph(
            [target.address],
            add_to_source_roots,
            postorder=True)
        return source_roots

    def _jars_to_directories(self, target):
        """Extracts and maps jars to directories containing their contents.
        :returns: a set of filepaths to directories containing the contents of jar.
        """
        files = set()
        jar_import_products = self.context.products.get_data(JarImportProducts)
        imports = jar_import_products.imports(target)
        for coordinate, jar in imports:
            files.add(self._extract_jar(coordinate, jar))
        return files

    def _extract_jar(self, coordinate, jar_path):
        """Extracts the jar to a subfolder of workdir/extracted and returns the path to it."""
        with open(jar_path, 'rb') as f:
            outdir = os.path.join(self.workdir, 'extracted', sha1(f.read()).hexdigest())
        if not os.path.exists(outdir):
            ZIP.extract(jar_path, outdir)
            self.context.log.debug('Extracting jar {jar} at {jar_path}.'
                .format(jar=coordinate, jar_path=jar_path))
        else:
            self.context.log.debug('Jar {jar} already extracted at {jar_path}.'
                .format(jar=coordinate, jar_path=jar_path))
        return outdir

    def _proto_path_imports(self, proto_targets):
        for target in proto_targets:
            for path in self._jars_to_directories(target):
                yield os.path.relpath(path, get_buildroot())

    @property
    def _copy_target_attributes(self):
        """Propagate the provides attribute to the synthetic java_library() target for publishing."""
        return ['provides', 'fatal_warnings']