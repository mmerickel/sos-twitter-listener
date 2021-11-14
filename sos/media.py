import posixpath
from urllib.parse import urlparse

def media_info_from_status(status):
    results = {}

    def add_image(id, url):
        content_type, ext, url = image_info(url)
        results[id] = {
            'name': f'{status["id"]}-{len(results)}-{id}{ext}',
            'content_type': content_type,
            'url': url,
        }

    def add_video(id, info):
        url = video_url(info)
        results[id] = {
            'name': f'{status["id"]}-{len(results)}-{id}.mp4',
            'content_type': 'video/mp4',
            'url': url,
        }

    # entities is expected to just have a single jpg in it
    for m in status.get('entities', {}).get('media', []):
        add_image(m['id'], m['media_url_https'])

    # extended_entities contains the actual media
    # some media is not downloadable based on settings like advertisers
    # disabling embedding
    for m in status.get('extended_entities', {}).get('media', []):
        # the only difference from video to animated_gif afaik is sound so we
        # try to download them similarly by picking the best one available
        if 'video_info' in m:
            add_video(m['id'], m['video_info'])

        # otherwise assume a photo and just download the url
        else:
            add_image(m['id'], m['media_url_https'])

    return results.values()

def image_info(url):
    path = urlparse(url).path
    _, ext = posixpath.splitext(path)
    if ext == '.jpg':
        content_type = 'image/jpeg'
        url = f'{url}?format=jpg&name=large'
    elif ext == '.png':
        content_type = 'image/png'
        url = f'{url}?format=png&name=large'
    else:
        content_type = 'application/octet-stream'
    return content_type, ext, url

def video_url(info):
    # only download highest bitrate video/mp4
    for variant in sorted(
        info['variants'],
        key=lambda v: v.get('bitrate', 0),
        reverse=True,
    ):
        if variant['content_type'] == 'video/mp4':
            return variant['url']
