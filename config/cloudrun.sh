#!/bin/sh
apt update -y
apt-get update -y
apt -y upgrade
apt install -y unzip 
apt install -y xvfb 
apt install -y libxi6 
apt install -y libgconf-2-4 
apt install -y gnupg
apt-get install -y libnss3-dev
apt-get install -y libnss3
apt install -y default-jdk 
apt-get install -y xdg-utils
apt install -y python3-pip
apt-get install -y python3-testresources
apt --fix-broken install
apt-get install -y python3-tk python3-dev
apt install -y curl

curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add 
sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
apt -y update 
apt -y install google-chrome-stable 


pip install --upgrade requests
PATH=$PATH:/home/analytics_digitaledge/.local/bin
pip install -r ./requirements.txt
pip install "pymongo[srv]"