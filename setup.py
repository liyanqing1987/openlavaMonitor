import os
import sys
import stat
from setuptools import find_packages, setup


## This check and everything above must remain compatible with Python 3.5.
print('>>> Check python version.')
CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 5)

if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write("""
==========================
Unsupported Python version
==========================
This version of openlavaMonitor requires Python {}.{} (or higher version), 
but you're trying to install it on Python {}.{}.
""".format(*(REQUIRED_PYTHON + CURRENT_PYTHON)))
    sys.exit(1)
else:
    print('    Required python version : ' + str(REQUIRED_PYTHON))
    print('    Current  python version : ' + str(CURRENT_PYTHON))


## Generate config file.
print('\n>>> Generate config file.')
cwd = os.getcwd()
installPath = cwd
dbPath = str(installPath) + '/db'
tempPath = str(installPath) + '/temp'
configFile = str(cwd) + '/monitor/conf/config.py'

try:
    with open(configFile, 'w') as CF:
        CF.write('installPath = "' + str(installPath) + '"\n')
        CF.write('dbPath      = "' + str(dbPath) +      '"\n')
        CF.write('tempPath    = "' + str(tempPath) +    '"\n')
    os.chmod(configFile, stat.S_IRWXU+stat.S_IRWXG+stat.S_IRWXO)
except Exception as error:
    print('''
*Error*: Failed on opening config file "%s" for write: %s
''' % (configFile, error))
    sys.exit(1)

print('    Config file : ' + str(configFile))


## Generate required directories.
print('\n>>> Generate required directories.')
dirList = [dbPath, tempPath]
for dir in dirList:
    if not os.path.exists(dir):
        print('    Generate directory "' + str(dir) + '".')
        os.makedirs(dir)
        os.chmod(dir, stat.S_IRWXU+stat.S_IRWXG+stat.S_IRWXO)


## Set setup settings.
print('\n>>> For setup settings.')
def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

setup(
    name='openlavaMonitor',
    version='0.1',
    python_requires='>={}.{}'.format(*REQUIRED_PYTHON),
    author='yanqing.li',
    author_email='liyanqing1987@163.com',
    description=('openlavaMonitor is an open source software for openlava '
                 'data-collection, date-analysis and information display.'),
    long_description=read('README.md'),
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    scripts=[],
    entry_points={},
    install_requires=[],
    extras_require={},
    zip_safe=False,
    classifiers=[],
    project_urls={},
)
