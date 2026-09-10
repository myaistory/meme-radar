# Deployment templates

`systemd/` contains hardened reference units for the read-only radar services.
They assume a dedicated `memeradar` user and conventional code, state, and
credential directories. Review every path and limit before installation.

The templates do not include endpoints or credentials. Keep environment files
outside Git with mode `0600`. Run the full test suite and a separate-database
DRY_RUN before enabling Telegram delivery.
