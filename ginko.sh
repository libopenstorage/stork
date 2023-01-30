make clean && make build-backup
ginkgo -v /Users/sn/27Jan23/torpedo/bin/backup.test   -- -spec-dir /Users/sn/27Jan23/torpedo/drivers/scheduler/k8s/specs -log-location `pwd` --app-list postgres  --ginkgo.focus=DeleteUsersRole --backup-driver pxb
