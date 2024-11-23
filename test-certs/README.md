TLS certificates used for local and CI/CD testing.

They are really long lived, 200y+ and should not require updates
unless the hosts or scope needs to be updated for some reason.

If so run

    make all

to regenerate all certs and then restart all docker compose services
for the changes to take effect.
