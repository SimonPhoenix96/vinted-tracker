# docker-compose -f vinted-tracker/Docker/docker-compose.yml up -d --no-recreate &&  celery multi restart w1 -A vinted-tracker.watcher -l INFO

from celery import Celery

app = Celery('vinted-tracker',
             broker='amqp://localhost:5672',
             backend='rpc://',
             include=['vinted-tracker.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()