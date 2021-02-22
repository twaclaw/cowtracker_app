#!/bin/bash

#DOMAIN=
#sudo certbot --nginx -d ${DOMAIN}
# sudo apt-get install certbot supervisor letsencrypt nginx python3-certbot-nginx pipenv

mkdir code
cd code
git clone git@github.com:twaclaw/cowtracker-ui.git cowtracker-frontend
git clone git@github.com:twaclaw/cowtracker_app.git cowtracker-backend

mkdir ~/scripts
cp cowtracker-backend/scripts/launch* ~/scripts
chmod 700 ~/scripts/*

sudo cp cowtracker-backend/scripts/supervisord.conf /etc/supervisor/conf.d/cowtracker.conf
cd ~
mkdir logs
