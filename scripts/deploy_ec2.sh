#!/bin/bash
# Despliega la parte A del parcial 3 en la EC2. Se corre en la EC2 (EC2 Instance Connect).
set -e
SRC=/tmp/p3-backend
APP=/home/ec2-user/app
cd $APP
# 1. El main.py de la EC2 debe ser el mismo del repo del parcial 2; si no, no se toca nada.
if [ ! -f main_p2_backup.py ] && ! diff -q $SRC/main_p2.py $APP/main.py >/dev/null; then
  echo "OJO: el main.py de la EC2 es distinto al del repo del parcial 2. No cambie nada."
  echo "Manda esta salida:"; diff $SRC/main_p2.py $APP/main.py | head -40; exit 1
fi
# 2. Copia de seguridad (solo la primera vez) y archivos nuevos
[ -f main_p2_backup.py ] || cp main.py main_p2_backup.py
cp $SRC/main.py $SRC/events.py $APP/
# 3. Variables del parcial 3 en el .env (si no estan)
grep -q '^KINESIS_STREAM=' .env || printf '\n# Parcial 3\nAWS_REGION=us-east-1\nKINESIS_STREAM=airline-events\nRAW_EVENTS_URI=s3://parcial2-aerolinea-838910/raw/events/\n' >> .env
# 4. Servicio con 1 worker (un solo consumidor del stream) y reinicio
sudo cp $SRC/airline.service /etc/systemd/system/airline.service
sudo systemctl daemon-reload
sudo systemctl restart airline
sleep 5
echo "--- workers:"; grep -o -- '--workers [0-9]*' /etc/systemd/system/airline.service
echo "--- health:"; curl -s localhost:8000/health; echo
echo "--- eventos:"; curl -s localhost:8000/admin/events-status; echo
