import subprocess
import sys
import argparse
from multiprocessing import Pool

TOKEN_TELEGRAF_VER='${TELEGRAF_VER}'
TELEGRAF_VER = '1.30.2'

def run_commands_on_remote(host, commands, keyfile=None):
    for command in commands:
        # commands = [shlex.quote(command) for command in commands]
        flag_keyfile = f"-i {keyfile} " if keyfile else ""
        arg = f"ssh {flag_keyfile}{host} \"{command}\""
        print(command)
        proc = subprocess.run(
            arg,
            shell=True,
            stdout=sys.stdout,
        )

def process_host(host):

    files = files_add
    if (mode == 'script'):
        files = files + [target]

    proc = None

    for file in files:
        proc = subprocess.run(f'scp -i {path_key} {file} {host.strip()}:~/', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            return proc

    command = f"./{target.split('/')[-1]}" if mode == 'script' else target
    proc = subprocess.run(f"ssh -i {path_key} {host.strip()} {command}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return proc

parser = argparse.ArgumentParser('run_script_on_remotes')
parser.add_argument('mode', choices=['script', 'cmd'], help='Mode')
parser.add_argument('target', help='Script/command to be run on the remote hosts')
parser.add_argument('--hosts', default='hosts.txt', help='Hosts file')
parser.add_argument('--key', default='/home/lennart/.ssh/id_rsa_continuum', help='SSH keyfile')
parser.add_argument('--files', nargs='+', default=[], help='List of files to be added')
parser.add_argument('-v', '--verbose', action='store_true', help='Always print output.')

args = parser.parse_args()
path_hosts = args.hosts
path_key = args.key
files_add = args.files
mode = args.mode
target = args.target

print(f'''
{'Command' if mode == 'cmd' else 'Script'}: {target}
Hosts file: {path_hosts}
Keyfile: {path_key}
Additional files: {files_add}
''')

with open(path_hosts, 'r') as file_hosts:

    hosts = list(map(lambda host: host.strip(), file_hosts.readlines()))
    hosts = list(filter(lambda host: not host.startswith('#') and not host == '', hosts))

    with Pool() as pool:
        results = pool.map(process_host, hosts)

    for (host, proc) in zip(hosts, results):
        print('✅' if proc.returncode == 0 else '❌', end=' ')
        print(f"{host} retured with code {proc.returncode}")
        if proc.returncode != 0 or args.verbose:
            for line in proc.stdout.decode().split('\n'):
                print(f'[stdout({host}] {line}')
            for line in proc.stderr.decode().split('\n'):
                print(f'[stderr({host}] {line}')


