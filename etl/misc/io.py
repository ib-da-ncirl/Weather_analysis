#  The MIT License (MIT)
#  Copyright (c) 2020 Ian Buttimer
#
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the "Software"), to deal
#   in the Software without restriction, including without limitation the rights
#   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#   copies of the Software, and to permit persons to whom the Software is
#   furnished to do so, subject to the following conditions:
#   The above copyright notice and this permission notice shall be included in all
#   copies or substantial portions of the Software.
#
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#   SOFTWARE.

import os
from pathlib import Path


def verify_path(path, typ='file', create_dir=False):
    exists = os.path.exists(path)
    if typ == 'folder' or typ == 'dir':
        if not exists and create_dir:
            Path(path).mkdir(parents=True, exist_ok=True)
            exists = True
        valid = os.path.isdir(path)
    elif typ == 'file':
        valid = os.path.isfile(path)
    else:
        raise ValueError(f"Unrecognised 'typ' argument: {typ}")
    if not exists:
        msg = f"'{path}' does not exist"
    elif not valid:
        msg = f"'{path}' is not a {typ}"
    else:
        msg = None
    return exists and valid, msg

