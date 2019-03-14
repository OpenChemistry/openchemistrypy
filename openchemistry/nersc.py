import os

def _nersc():
    return os.environ.get('GIRDER_HOST') == 'nersc.openchemistry.org'

def _create_cori_cluster(girder_client):
    body = {
        'config': {
            'host': 'cori'
        },
        'name': 'cori',
        'type': 'newt'
    }

    return girder_client.post('clusters', json=body)

def _fetch_cori_cluster(girder_client):
    # See if we already have a cori cluster
    params = {
        'type': 'newt',
        'name': 'cori'
    }
    clusters = girder_client.get('clusters', params)

    if len(clusters) > 0:
        return clusters[0]
    else:
        return _create_cori_cluster(girder_client)
