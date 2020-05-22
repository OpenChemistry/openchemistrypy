import json
import argparse
import subprocess
from subprocess import PIPE
import os
import errno


def split_image(img):
    split = img.split(':')
    repository = split[0]
    tag = split[1] if len(split) > 1 else 'latest'
    return repository, tag


def run_subprocess(args):
    # Runs subprocess and returns a tuple of the output and the error
    p = subprocess.Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        raise Exception('Command failed: ' + args.join(' '))

    return output.decode('utf-8'), err.decode('utf-8')


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


def singularity():
    singularity_dir = ensure_singularity_dir()

    images = []
    for f in os.listdir(singularity_dir):
        if not f.endswith('.sif'):
            continue

        # The digest is the name of the file, without .sif
        digest = f.replace('.sif', '')

        path = os.path.join(singularity_dir, f)
        output, err = run_subprocess(['singularity', 'inspect', path])

        info = json.loads(output)
        repo_tag_str = 'org.label-schema.usage.singularity.deffile.from'
        if repo_tag_str not in info:
            continue

        repository, tag = split_image(info[repo_tag_str])

        size = os.path.getsize(path)

        image = {
            'repository': repository,
            'tag': tag,
            'digest': digest,
            'size': size
        }
        images.append(image)

    return images


def docker():
    ls_args = ['docker', 'image', 'ls']
    output, err = run_subprocess(ls_args)

    images = []
    lines = output.split('\n')
    if lines:
        # Remove the header line
        lines.pop(0)

    for line in lines:
        line_split = line.split()
        if len(line_split) < 5:
            continue

        image_id = line_split[2]

        inspect_args = ['docker', 'inspect', image_id]
        output, err = run_subprocess(inspect_args)

        info = json.loads(output)
        if not info:
            continue

        info = info[0]

        if not info.get('RepoTags'):
            continue

        repository, tag = split_image(info['RepoTags'][0])

        if not info.get('RepoDigests'):
            # This will be empty for user-created images
            continue

        _, digest = split_image(info['RepoDigests'][0])

        size = info['Size']
        image = {
            'repository': repository,
            'tag': tag,
            'digest': digest,
            'size': size
        }
        images.append(image)

    return images


def shifter():
    output, err = run_subprocess(['shifterimg', 'images'])

    # FIXME: the below is just a guess, and probably doesn't work
    # I don't have access to NERSC to test this.
    images = []
    for line in output.split('\n'):
        line_split = line.split()
        if len(line_split) < 6:
            continue

        repository, tag = split_image(line_split[5])

        # Get the full digest
        digest, err = run_subprocess(['shifterimg', 'lookup', line_split[5]])

        # FIXME: implement a way to get the shifter image size
        # I am currently not aware of one, and don't have access to NERSC
        size = 0

        image = {
            'repository': repository,
            'tag': tag,
            'digest': digest,
            'size': size
        }
        images.append(image)

    return images


def write_descriptor(images):
    with open('list.json', 'w') as fp:
        json.dump(images, fp)


def main():
    parser = argparse.ArgumentParser(
        description='Pull image from container registry.')
    parser.add_argument('-c', '--container',
                        choices=['docker', 'singularity', 'shifter'],
                        help='the type of container', required=True)

    args = parser.parse_args()
    container = args.container

    if container == 'singularity':
        images = singularity()
    elif container == 'shifter':
        images = shifter()
    else:
        images = docker()

    write_descriptor(images)


if __name__ == "__main__":
    main()
