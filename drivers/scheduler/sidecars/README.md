# Wordpress CLI

The wordpress CLI waits for MySQL to be available then downloads
and installs wordpress into the shared volume.

Once complete, it touches the file `installed` in that directory
signalling the PHP pods that they can begin serving the Wordpress
site.
