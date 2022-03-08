FROM ubuntu:20.04

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

COPY . ./

ARG DEBIAN_FRONTEND=noninteractive  
ENV TZ=Asia/Kolkata
ENV DISPLAY=:99
ENV PORT=8080
RUN chmod +x ./config/cloudrun.sh
RUN ./config/cloudrun.sh

CMD [ "python3", "./src/main.py", "BU", "0" ]
# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app