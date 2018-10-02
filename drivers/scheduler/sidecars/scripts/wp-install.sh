#!/usr/bin/env bash

function wait_for_mysql() {
    until nc -z -v -w30 ${WORDPRESS_DB_HOST} 3306
    do
      echo ":: Waiting for database connection..."
      sleep 5
    done
}

function log() {
    echo "$(date) $@"
}

function am_i_first() {
    mkdir /wordpress/inprogress >/dev/null 2>&1
    echo $?
}

function sleep_forever() {
    log ":: Sleeping forever. Exec into me for debugging"
    touch /tmp/alive
    while true; do sleep 30; done;
}

if [ "$(am_i_first)" = "0" ]
then
    log ":: This is the first installer. Proceeding with installation"
else
    log ":: This is not the first installer"
    sleep_forever
fi

wait_for_mysql

log ":: Downloading wordpress to temp directory..."
mkdir -p /tmp/wordpress
cd /tmp/wordpress
wp core download

log ":: Moving wordpress to PX shared volume..."
mv /tmp/wordpress/* /wordpress/
cd /wordpress

log ":: Generating wordpress config..."
wp config create --dbname=pwx --dbhost="${WORDPRESS_DB_HOST}" --dbuser="root" --dbpass="${WORDPRESS_DB_PASSWORD}" --force

log ":: Generating wordpress database..."
wp db create --dbuser=root --dbpass="${WORDPRESS_DB_PASSWORD}"

log ":: Allowing wordpress pods to come online"
touch /wordpress/installed

log ":: Waiting for wordpress to come online..."
while ! curl --connect-timeout 2 'wordpress:80' ; do sleep 1 ; done

log ":: Installing wordpress..."
wp core install --url="${WORDPRESS_URL}" --title="TestWordPress" --admin_user="admin" --admin_password="correcthorsebatterystaple" --admin_email="noreply@portworx.com" --skip-email

log ":: Generating junk posts..."
wp post generate --count=100

log "Installing plugins..."
for plugin_name in ${WORDPRESS_PLUGINS//,/ }
do
    log ":: Installing plugin ${plugin_name}"
    wp plugin install /home/www-data/"${plugin_name}" --activate
done

log ":: Installing theme..."
wp theme install "${WORDPRESS_THEME}" --activate

sleep_forever
