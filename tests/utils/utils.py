# Copyright (C) 2018 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import multiprocessing
import psutil


def kill_process_tree(pid):
    proc = psutil.Process(pid)
    children = proc.children(recursive=True)

    def kill_proc(p):
        try:
            p.kill()
        except psutil.AccessDenied:
            # Ignore this error, it can happen with
            # some setuid bwrap processes.
            pass

    # Bloody Murder
    for child in children:
        kill_proc(child)
    kill_proc(proc)


def read_file(file_path, text_mode=False):
    with open(file_path, 'r' if text_mode else 'rb') as in_file:
        return in_file.read()


def run_in_subprocess(function, *arguments, timeout=1):
    queue = multiprocessing.Queue()
    # Use subprocess to avoid creation of gRPC threads in main process
    # See https://github.com/grpc/grpc/blob/master/doc/fork_support.md
    process = multiprocessing.Process(target=function,
                                      args=(queue, *arguments))

    try:
        process.start()
        result = queue.get(timeout=timeout)
        process.join()

    except KeyboardInterrupt:
        kill_process_tree(process.pid)
        raise

    return result
