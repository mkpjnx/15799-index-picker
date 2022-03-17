import action_selection

VERBOSITY_DEFAULT = 2

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"


def task_project1_setup():
    sql_list = [
        "CREATE EXTENSION IF NOT EXISTS hypopg ;",
        "SELECT * FROM pg_extension;",
        "ALTER SYSTEM SET max_connections = '200';",
        "ALTER SYSTEM SET shared_buffers = '4GB';",
        "ALTER SYSTEM SET effective_cache_size = '12GB';",
        "ALTER SYSTEM SET maintenance_work_mem = '1GB';",
        "ALTER SYSTEM SET checkpoint_completion_target = '0.9';",
        "ALTER SYSTEM SET wal_buffers = '16MB';",
        "ALTER SYSTEM SET default_statistics_target = '100';",
        "ALTER SYSTEM SET random_page_cost = '1.1';",
        "ALTER SYSTEM SET effective_io_concurrency = '200';",
        "ALTER SYSTEM SET work_mem = '10485kB';",
        "ALTER SYSTEM SET min_wal_size = '1GB';",
        "ALTER SYSTEM SET max_wal_size = '4GB';",
        "ALTER SYSTEM SET max_worker_processes = '4';",
        "ALTER SYSTEM SET max_parallel_workers_per_gather = '2';",
        "ALTER SYSTEM SET max_parallel_workers = '4';",
        "ALTER SYSTEM SET max_parallel_maintenance_workers = '2';",
    ]
    pg_actions = [
        f'PGPASSWORD={DEFAULT_PASS} psql --host=localhost --dbname={DEFAULT_DB} --username={DEFAULT_USER} --command="{sql}"'
        for sql in sql_list
    ]

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            f"pip3 install -r ./requirements.txt",
            "sudo apt-get install -y postgresql-common",
            "sudo sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y",
            "sudo apt-get update",
            "sudo apt-get install -y postgresql-14-hypopg",
            *pg_actions,
            "sudo systemctl restart postgresql",
        ],
        # Always rerun this task.
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
    }


def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Generating Indexes."',
            action_selection.generate_sql,
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "Path to workload csv.",
                "default": "artifacts/workload/workload.csv",
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "Timeout for action generation",
                "default": "10m",
            },

        ],
        # Always rerun this task.
        "targets": ["actions.sql", "config.json"],
        "verbosity": VERBOSITY_DEFAULT,
        "uptodate": [False],
    }
