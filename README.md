# proyecto para BDNR

El projecto esta sencillito chavalos, primero les explico la arqui

tenemos dos contenedores data_loader y el de mongo

el data loader consta de:
- load_data.py (el archivo de python que saca data
de no se donde)
- Dockerfile un archivo sencillito para crear la imagen del contenedor que corre
load_data.py
- un requirements.txt que solo tiene requests y pymongo que son las librerias que
pip tiene que instalar

el container de mongo es la imagen oficial

ahora, se preguntaran, como sucede la magia? pues facil, docker como dijo Umami es poderoso
y existe algo que se llama docker compose que maneja varios container al mismo tiempo

a mi se me hizo bastante intuitivo, hechenle un vistazo a docker-compose.yml

finalmente hay un .gitignore generico

# Como correr el docker
Esta demasiado sencillo:
1. Se van a la rama donde van a correr el proyecto
2. Le dan a git clone este repo
3. ejecutan docker compose up --build
4. Disfruten

si quieren ver el contenedor de docker que esta corriendo de fondo solo le dan
docker -ps

para ver el nombre de su container de mongo (probablemente mongodb)

entonces se meten a interactuar con el con

docker exec -it mongodb bash

y ya adentro del container corren:

mongosh -u user -p password

literal esas son las credenciales, vienen definidas como variables de entorno en el
docker-compose.yml

y ya adentro de dicho mongosh le dan a lo que quieran, tipo

show dbs;
use financial_data;
db.crypto_prices.find();


