#!/bin/bash -e

IMAGE=
WAIT=0 # 0 means don't wait. Run to completion.
STATUS_FILE=/tmp/docker-pull-done

rm -rf $STATUS_FILE

usage()
{
    echo "usage: pull_docker.sh [[[-i <image> ] [-w <wait-on-success>] ] | [-h]]"
}

cleanup ()
{
  kill -s SIGTERM $!
  exit 0
}

sleep_forever() {
  while : ; do
    sleep 60 &
    wait $!
  done
}

trap cleanup SIGINT SIGTERM

while [ "$1" != "" ]; do
    case $1 in
        -i | --image )          shift
                                IMAGE=$1
                                ;;
        -w | --wait  )          WAIT=1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

if [ -z "${IMAGE}" ]; then
  echo "Error: image is required. Use -i option to specify the image to pull."
  exit 1
fi

if [ -n "${REGISTRY_USER}" ] && [ -n "${REGISTRY_PASS}" ]; then
	docker login -u ${REGISTRY_USER} -p ${REGISTRY_PASS}
	if [ $? -ne 0 ]; then
		echo "failed to login with given credentials"
		exit 1
	fi
fi

docker pull $IMAGE
if [ $? -eq 0 ]; then
  touch $STATUS_FILE

  if [ "$WAIT" = "1" ]; then
    echo "Successfully pulled image. Sleeping..."
	  sleep_forever
  fi

  echo "Exiting..."
  exit 0
else
	exit $?
fi


