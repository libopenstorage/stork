#!/bin/bash -e

TALISMAN_IMAGE=portworx/talisman
TALISMAN_TAG=latest
WIPE_CLUSTER=

usage()
{
	echo "
	usage: $0 [--talismanimage <img>] [--talismantag <tag>] [--wipecluster]
	examples:
            # Along with deleting Portworx Kubernetes components, also wipe Portworx cluster metadata
            $0 --wipecluster

            # Run using custom talisman image and tag
            $0 --talismanimage harshpx/talisman --talismantag latest

			"
	exit 1
}

while [ "$1" != "" ]; do
    case $1 in
        -ti | --talismanimage ) shift
                                TALISMAN_IMAGE=$1
                                ;;
        -tt | --talismantag )   shift
                                TALISMAN_TAG=$1
                                ;;
        --wipecluster )         WIPE_CLUSTER="--wipecluster"
                                ;;
        -h | --help )           usage
                                ;;
        * )                     usage
    esac
    shift
done

kubectl delete -n kube-system job talisman || true

cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: talisman-account
  namespace: kube-system
---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1beta1
metadata:
  name: talisman-role-binding
subjects:
- kind: ServiceAccount
  name: talisman-account
  namespace: kube-system
roleRef:
  kind: ClusterRole
  name: cluster-admin
  apiGroup: rbac.authorization.k8s.io
---

apiVersion: batch/v1
kind: Job
metadata:
  name: talisman
  namespace: kube-system
spec:
  backoffLimit: 1
  template:
    spec:
      serviceAccount: talisman-account
      containers:
      - name: talisman
        image: $TALISMAN_IMAGE:$TALISMAN_TAG
        args: ["-operation",  "delete", "$WIPE_CLUSTER"]
        volumeMounts:
        - name: etcpwx
          mountPath: /etc/pwx
      volumes:
      - name: etcpwx
        hostPath:
          path: /etc/pwx
      restartPolicy: Never
EOF
