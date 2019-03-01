import json
import argparse
import subprocess
import os
import errno
try:
    from urllib.request import Request, urlopen  # Python 3
except ImportError:
    from urllib2 import Request, urlopen  # Python 2

reg_base_url = 'https://registry-1.docker.io/v2'
reg_auth_base_url = 'https://auth.docker.io'

def ensure_singularity_dir():
    home = os.path.expanduser("~")
    singularity_dir = os.path.join(home, '.oc', 'singularity')

    try:
        os.makedirs(singularity_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(singularity_dir):
            pass
        else:
            raise

    return singularity_dir


def singularity(repo, digest):
    image_name = '%s@%s' % (repo, digest)
    singularity_dir = ensure_singularity_dir()
    digest = digest.split(':')[1]
    container_path = os.path.join(singularity_dir, '%s.sif' % digest)

    if not os.path.exists(container_path):
        docker_uri  = 'docker://%s' % image_name
        subprocess.check_call(['singularity', 'build', container_path, docker_uri])

    return container_path

def docker(repo, digest):
    image_name = '%s@%s' % (repo, digest)
    subprocess.check_call(['docker', 'pull', image_name])

    return image_name

def write_descriptor(repo, tag, digest, image_uri):
    des = {
        'repository': repo,
        'tag': tag,
        'digest': digest,
        'imageUri': image_uri
    }

    with open('pull.json', 'w') as fp:
        json.dump(des, fp)

def authenticate(repo):
    resp = urlopen('%s/token?service=registry.docker.io&scope=repository:%s:pull' % (reg_auth_base_url, repo)).read()

    return json.loads(resp)['token']

def fetch_digest(repo, tag, token):
    req = Request('%s/%s/manifests/%s' % (reg_base_url, repo, tag))
    req.add_header('Authorization', 'Bearer %s' % token)
    req.add_header('Accept', 'application/vnd.docker.distribution.manifest.v2+json')
    resp = urlopen(req)

    return resp.info().get('Docker-Content-Digest')

def main():
    parser = argparse.ArgumentParser(description='Pull image from container registry.')
    parser.add_argument('-r', '--repository', type=str, help='the repository to pull from', required=True)
    parser.add_argument('-t', '--tag', type=str, help='the tag to pull', required=True)
    parser.add_argument('-c', '--container', choices=['docker', 'singularity'],
                        help='the type of container', required=True)

    args = parser.parse_args()
    container = args.container
    repo = args.repository
    tag = args.tag

    token = authenticate(repo)
    digest = fetch_digest(repo, tag, token)

    if container == 'singularity':
        image_uri = singularity(repo, digest)
    else:
        image_uri = docker(repo, digest)

    write_descriptor(repo, tag, digest, image_uri)

    print(image_uri)

if __name__ == "__main__":
    main()
