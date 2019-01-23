To run the docker puller,

```bash
kubectl apply -f docker-puller.yaml
```

This will start a DaemonSet that starts pulling the image given in `-i` on all your nodes.

If `-w` is given, it will sleep/wait forever after pulling the image.
