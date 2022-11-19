# docker-compose -f vinted-tracker/docker/docker-compose.yml up -d --no-recreate &&  celery multi restart w1 -A vinted-tracker.watcher -l INFO

from celery import Celery

app = Celery('vinted-tracker')
app.config_from_envvar('CELERY_CONFIG_MODULE')

if __name__ == '__main__':
    app.start()