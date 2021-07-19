#!/bin/bash

#DOMAIN=
sudo certbot --nginx -d ${DOMAIN}
sudo apt-get install certbot  letsencrypt nginx python3-certbot-nginx pipenv npm python3-certbot-nginx postgresql postgresql-client-common postgresql-client-12

# clone code
mkdir code
cd code
git clone git@github.com:twaclaw/cowtracker-ui.git cowtracker-frontend
git clone git@github.com:twaclaw/cowtracker_app.git cowtracker-backend

# prepare launching scripts
mkdir ~/scripts
cp code/cowtracker-backend/scripts/launch* ~/scripts
chmod 700 ~/scripts/*

# systemd configuration
mv ~/code/cowtracker-backend/config/systemd ~
systemctl --user link ~/systemd/cows.all.target
systemctl --user link ~/systemd/cows@.service
systemctl --user daemon-reload
systemctl --user enable cows.all.target
systemctl --user enable cows@1.service
systemctl --user start cows@1.service

cd ~
mkdir logs

# prepare backend code
cd ~/code/cowtracker-backend
pipenv install

# prepare frontend
cd ~/code/cowtracker-frontend
npm install
npm run build
npm run generate

# update nginx
sudo cp ~/code/cowtracker-backend/config/nginx.conf /etc/nginx/
nc -vlU cowtracker_1.sock
mv cowtracker_1.sock /tmp


# bootstrap database
sudo systemctl start postgresql.service