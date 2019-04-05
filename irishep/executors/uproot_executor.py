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
from irishep.datasets.uproot_dataset import UprootDataset
from irishep.executors.executor import Executor
import uproot


class FakeSparkBroadcastVar:
    def __init__(self, value):
        self.value = value


class FakeSparkAccumulator:
    def __init__(self, initial_val, accumulator):
        self.value = initial_val
        self.accumulator = accumulator

    def add(self, other):
        self.value = self.accumulator.addInPlace(self.value, other)


class UprootExecutor(Executor):
    templates = {
        "nanoAOD": "uproot_nanoaod.py"
    }

    def __init__(self, app_name):
        super().__init__(app_name)

    def read_files(self, dataset_name, files):
        if len(files) > 1:
            print(
                "WARN: Uproot implementation doesn't work with multiple " +
                "files in a dataset. Just reading the first file")

        root = uproot.open(files[0])
        dataset = UprootDataset(dataset_name, root["Events"])
        return dataset

    def register_accumulator(self, initial_value, accumulator):
        return FakeSparkAccumulator(initial_value, accumulator)

    def register_broadcast_var(self, var):
        return FakeSparkBroadcastVar(var)
