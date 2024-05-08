#!/bin/bash
if [ $# -eq 0 ]; then
    echo "Usage: $0 <replica_number | 'all'>"
    exit 1
fi

run_replica() {
    echo "Starting Replica $1..."
    /usr/bin/env /usr/lib/jvm/java-11-openjdk-amd64/bin/java @/tmp/cp_r52u0m6nzo8ny0toxreut4ga.argfile Replica $1 &
}

case "$1" in
    1|2|3)
        run_replica $1
        ;;
    all)
        run_replica 1
        run_replica 2
        run_replica 3
        ;;
    *)
        echo "Invalid argument: $1"
        echo "Usage: $0 <replica_number | 'all'>"
        exit 1
        ;;
esac

wait
echo "All processes completed."
