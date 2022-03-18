from dynaconf import Dynaconf
import os


settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=["settings.yml", ".secrets.yml"],
    environments=True,
    env=os.environ.get("USER"),
)
