# doit automatically picks up tasks as long as their unqualified name is prefixed with task_.
# Read the guide: https://pydoit.org/tasks.html

def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo "SELECT 1;" > actions.sql',
            'echo "SELECT 2;" >> actions.sql',
            'echo \'{"VACUUM": true}\' > config.json',
        ],
        # Always rerun this task.
        "targets": ["actions.sql", "config.json"],
        "uptodate": [False],
    }
