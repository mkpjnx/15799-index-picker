VERBOSITY_DEFAULT = 2

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"

def task_project1_setup():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            f"sudo pip3 install -r ./requirements.txt",
            "sudo apt-get install -y postgresql-common",
            "sudo sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y",
            "sudo apt-get update",
            "sudo apt-get install -y postgresql-14-hypopg"
        ],
        # "targets": ["actions.sql", "config.json"],
        # Always rerun this task.
        "verbosity" : VERBOSITY_DEFAULT,
        "uptodate": [False],
    }

def task_pg_setup():

    sql_list = [
        "CREATE EXTENSION IF NOT EXISTS hypopg ;"
        "SELECT * FROM pg_extension;"
    ]

    return {
        "actions": [
            *[
                f'PGPASSWORD={DEFAULT_PASS} psql --host=localhost --dbname={DEFAULT_DB} --username={DEFAULT_USER} --command="{sql}"'
                for sql in sql_list
            ],
            "sudo pg_ctlcluster 14 main restart",
        ],
        "verbosity": VERBOSITY_DEFAULT,
    }


def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo "SELECT 1;" > actions.sql',
            'echo "SELECT 2;" >> actions.sql',
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        # Always rerun this task.
        "targets": ["actions.sql", "config.json"],
        "uptodate": [False],
    }
