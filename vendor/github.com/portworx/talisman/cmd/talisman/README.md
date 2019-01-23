### Running talisman

#### Upgrading PX

You can run run_px_upgrade.sh in scripts directory. For example:

```bash
./scripts/run_px_upgrade.sh --ocimontag 1.3.0-rc4
```

This will start a [Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/). You can monitor the job logs using:

```bash
kubectl logs -n kube-system -l job-name=talisman
```

### Restore scaled down shared applications

During the upgrade, Portworx might scale down all shared volume PX appplications to 0 replicas.
If the upgrade gets interuppted in between by an unexpected failure, you can restore the shared volume PX applications back to their original replica count using below command.

```bash
./scripts/run_shared_app_restore.sh
```

Run `./scripts/run_px_upgrade.sh --help` for more usage examples.

