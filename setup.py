# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import os
import setuptools


name = 'ox_bqpipeline'
description = 'Utility class for building data pipelines in BigQuery'
version = '0.0.4'
author = 'Jacob Ferriero'
author_email = 'jferriero@google.com'
url = 'https://github.com/openx/ox-bqpipeline'
download_url = 'https://github.com/openx/ox-bqpipeline/archive/v0.0.4.zip'
release_status = 'Development Status :: 3 - Alpha'
dependencies = [
    'google-cloud>=0.34.0',
    'google-api-python-client>=1.7.8',
    'google-cloud-bigquery>=1.9.0',
    'Jinja2>=2.10',
    'j2cli>=0.3.8',
    'sqlparse>=0.3.0',
    'pylint>=1.9.4',
    'google-cloud-bigquery >= 1.9.0',
    'Jinja2 >= 2.10'
]
extras = {}


# Setup boilerplate below this line.
package_root = os.path.join(os.path.abspath(
    os.path.dirname(__file__)), 'ox_bqpipeline')

readme_filename = 'README.md'
with io.open(readme_filename, encoding='utf-8') as readme_file:
    readme = readme_file.read()


setuptools.setup(
    name=name,
    version=version,
    description=description,
    author=author,
    author_email=author_email,
    url=url,
    download_url=download_url,
    long_description=readme,
    long_description_content_type='text/markdown',
    license='Apache 2.0',
    classifiers=[
        release_status,
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent',
        'Topic :: Internet',
    ],
    platforms='Posix; MacOS X; Windows',
    packages=['ox_bqpipeline'],
    install_requires=dependencies,
    extras_require=extras,
    python_requires='>=3.4',
    include_package_data=True,
    zip_safe=False,
)
