================
Twitter Listener
================

Configure a ``profile.yml`` containing your developer account credentials::

  twitter:
    access_token: '...'
    access_token_secret: '...'
    consumer_key: '...'
    consumer_secret: '...'

Configure a file containing filter parameters (``potus.yml``)::

  track:
    - potus

Start the listener::

  pipenv run sos api:stream potus.yml potus-stream

Eventually Ctrl-C the listener or send a SIGHUP to the process which will trigger it to rotate the file. Now you have a file that you can convert to json or to a csv.
