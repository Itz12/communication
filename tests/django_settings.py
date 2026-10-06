SECRET_KEY = "communication-tests-only"
INSTALLED_APPS = ["django.contrib.contenttypes", "communication"]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
