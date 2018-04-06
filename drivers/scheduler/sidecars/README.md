# Wordpress

The `wordpress-extract` image populates a volume with a base
Wordpress installation.

Use as an init container; the main containers will not launch
until the extraction is complete.


# MySQL

The `mysql-dump` container takes a previously created SQL dump
and pushes it into the database.

Use an an init container so the database is ready before the
application starts.

# Ping Test

More accurately called the `curl` test, this init container
simply delays the start of the application until after the given
URL responds.

This is useful in cases where the main application isn't able
to handle a liveliness probe and will crash if the URL isn't
available. Triggering a crash loop backoff is undesirable, so
this delays the start of the container.
