import action_selection

VERBOSITY_DEFAULT = 2

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"


def task_project1_setup():
    sql_list = [
        "CREATE EXTENSION IF NOT EXISTS hypopg ;"
        "SELECT * FROM pg_extension;"
    ]
    pg_actions = [
        f'PGPASSWORD={DEFAULT_PASS} psql --host=localhost --dbname={DEFAULT_DB} --username={DEFAULT_USER} --command="{sql}"'
        for sql in sql_list
    ]

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            f"sudo pip3 install -r ./requirements.txt",
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
            'cat pgtune.sql > action.sql',
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
