# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
from irishep.analysis.columnar_analysis import ColumnarAnalysis
from jinja2 import Environment, PackageLoader, select_autoescape
import re


class NanoAODColumnarAnalysis(ColumnarAnalysis):
    obj_property_re = re.compile("^[A-Za-z0-9]+_(.*)")

    def __init__(self):
        super().__init__()
        self.env = Environment(
            loader=PackageLoader('irishep', 'templates'),
            autoescape=select_autoescape(['py'])
        )

    def _zip_entry(self, col_name):
        """
        Create a jagged array entry for zipping. This is where we convert the
        pandas Series to a jagged array
        :param col_name: Column Name
        :return: String that represents an entry in JaggedArray.zip for
        that column
        """
        match = re.match(self.obj_property_re, col_name)
        item = '{}={}.array[0].base'.format(match.group(1), col_name)
        return item

    def generate_udf(self, dataset, physics_objects, return_expr,
                     analysis_class):
        """
        Create a pandas dataframe UDF that can be passed into spark for
        implementing the analysis.
        :param dataset: The dataset this analysis is based on
        :param physics_objects: List of physics object names that will be passed
        into the UDF
        :param return_expr: Code to execute to return value from UDF
        :param analysis_class: Fully qualified class name for a subclass of
        user_analysis class
        :return: The generated function
        """

        # Create a directory of JaggedArray zip entries. One for each physics
        # object. Each entry in the dictionary is a list of zip entries. The
        # zip entries include one for the count series (i.e. nElectron)
        objects = {o:
                   ["{}.array".format(
                       dataset.count_column_for_physics_object(o))] +
                   [
                       self._zip_entry(c) for c in dataset.columns if
                       c.startswith(o + "_")
                   ] for o in physics_objects
                   }

        template = self.env.get_template('mytemplate.py')
        udf_str = template.render(physics_objects=objects,
                                  cols=dataset.columns_for_physics_objects(
                                      physics_objects),
                                  return_expr=return_expr,
                                  analysis_class=analysis_class)
        print(udf_str)
        exec(udf_str)

        # Assume that the template renders to a def udf(....)
        return locals()['udf']
