#!/bin/bash -e

OCI_MON_IMAGE=
OCI_MON_TAG=
TALISMAN_IMAGE=portworx/talisman
TALISMAN_TAG=latest
SCALE_DOWN_SHARED_APPS_MODE=auto

usage()
{
	echo "
	usage: $0 --ocimontag <new oci tag> [--ocimonimage <new oci image>] [--talismanimage <img>] [--talismantag <tag>] [--scaledownsharedapps <auto|on|off>]
	examples:
            # Upgrade Portworx using oci monitor tag 1.3.0-rc4
            $0 --ocimontag 1.3.0-rc4

            # Upgrade Portworx using oci monitor tag 1.3.0-rc4 and custom oci mon image
            $0 --ocimonimage harshpx/oci-monitor --ocimontag 1.3.0-rc3

            # Upgrade Portworx using oci monitor tag 1.3.0-rc4 using talisman harshpx/talisman:latest
            $0 --ocimontag 1.3.0-rc4 --talismanimage harshpx/talisman --talismantag latest
			"
	exit 1
}

while [ "$1" != "" ]; do
    case $1 in
        --ocimonimage )         shift
                                OCI_MON_IMAGE=$1
                                ;;
        --ocimontag )           shift
                                OCI_MON_TAG=$1
                                ;;
        --scaledownsharedapps ) shift
                                SCALE_DOWN_SHARED_APPS_MODE=$1
                                ;;
        --talismanimage )       shift
                                TALISMAN_IMAGE=$1
                                ;;
        --talismantag )         shift
                                TALISMAN_TAG=$1
                                ;;
        -h | --help )           usage
                                ;;
        * )                     usage
    esac
    shift
done

if [ -z "$OCI_MON_IMAGE" ]; then
		OCI_MON_IMAGE="portworx/oci-monitor"
		echo "defaulting OCI Monitor image to $OCI_MON_IMAGE"
fi

if [ -z "$OCI_MON_TAG" ]; then
		echo "--ocimontag is required"
		usage
fi

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
        args: ["-operation",  "upgrade", "-ocimonimage", "$OCI_MON_IMAGE", "-ocimontag" ,"$OCI_MON_TAG", "-scaledownsharedapps", "$SCALE_DOWN_SHARED_APPS_MODE"]
      restartPolicy: Never
EOF
