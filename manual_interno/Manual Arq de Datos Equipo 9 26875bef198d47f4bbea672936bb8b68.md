# Manual Arq de Datos Equipo 9

# Conectarse a máquinas remotas:

### Conectarse a bastion:

cambiar acá por su nombre de usuario y la dirección del ec2 que cambia diariamente

`ssh -i 'id_rsa' [santi_dpa_bastion@ec2-34-222-150-101.us-west-2.compute.amazonaws.com](mailto:santi_dpa_bastion@ec2-34-222-150-101.us-west-2.compute.amazonaws.com)`

### Conectarse a la máquina de procesamiento:

aquí todos usuamos el mismo usuario ubuntu:

`ssh -i 'id_rsa' ubuntu@[ec2-34-222-143-84.us-west-2.compute.amazonaws.com](http://ec2-34-222-143-84.us-west-2.compute.amazonaws.com/)`

### Una vez dentro de la máquina de procesamiento:

cambiarse a usuario root:

`sudo su`

entrar a la carpeta del repo:

`cd dpa_2021`

Activar el pyenv:

`pyenv activate dpa_2021`

Desde aquí se pueden correr todos los tasks de luigi. (abajo pongo los comandos para correr los que tenemos hasta ahora). 

### Conectarse a la postgres:

Desde la máquina de procesamiento:

`psql -U dpa_team -h [base-dpa-equipo9.cqzg9zyjyqov.us-west-2.rds.amazonaws.com](http://base-dpa-equipo9.cqzg9zyjyqov.us-west-2.rds.amazonaws.com/) -d db_dpa2021`

les va a pedir un password que no ponemos aquí por seguridad. 

# Tasks de Luigi:

Dentro de la máquina de procesamiento, correr los comandos indicados arriba de cambiarse a root, entrar al repo y activar pyenv 

luego, por única vez se corre este comando, para agregar nuestro repo al path:

`export PYTHONPATH=$PWD`

Y luego se corre un task, aquí algunos ejemplos:

Task 1:

`luigi --module src.pipeline.luigi.extract APIDataIngestion --ingest-type consecutive --local-scheduler`

Task 3:

`luigi --module src.pipeline.luigi.save_s3 S3Task --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler`

Task 5  TRANSFORMATION: 

`luigi --module src.pipeline.luigi.transform Transformation --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler`

Task 7 Feature_Engineering 

`luigi --module src.pipeline.luigi.feature_engineering FeatureEngineering --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler`

Task 8 

`luigi --module src.pipeline.luigi.feature_engineering_metadata FeatureEngineeringMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler`

## Visualizador de luigi:

Si deseas visualizar luigi en tu browser local debes crear un tunel a la máquina de procesamiento. 

Para ello, antes que nada: 

1. Debes habilitar una inbound rule con tu IP en la máquina de procesamiento
2. debes habilitar la llave que usas para conectarte a bastion entre las llaves autorizadas en la máquina de procesamiento 

Aquí deberás tener varias terminales abiertas a la misma vez: 

Terminal 1: en tu máquina local debes correr lo siguiente:

`ssh -i 'id_rsa' -NL localhost:4444:10.0.0.56:8082 [ubuntu@ec2-54-201-9-79.us-west-2.compute.amazonaws.com](mailto:ubuntu@ec2-54-201-9-79.us-west-2.compute.amazonaws.com)`

aquí debes reemplazar:

1. lo que está después de la @ por la dirección de la máquina de procesamiento de ese día
2. 10.0.0.56 por la dirección de la ip de la máquina de procesamiento. La misma la encontraras en el prompt de la máquina de procesamiento, pero escrita con guiones. tú debes escribirla con puntos, como en el ejemplo 

Terminal 2, en la máquina de procesamiento:

(luego de correr los comandos indicados en la sección "máquina de procesamiento", activar usuario root, entrar al repo, activar pyenv) corres el comando:

`luigid`

Terminal 3, en la máquina de procesamiento:

Corres un task de luigi, como en los ejemplos de arriba, pero le quitas la última parte

`--local-scheduler`

Abres tu browser local en: 

`localhost:4444`

Y podrás visualizar los tasks de luigi

## Indempotencia de luigi:

Cada task de luigi, antes de correr, se va a fijar si este ya fue corrido. Para eso en general busca un archivo o una entrada en postgres que le indica que ya fue corrido. 

Para poder volver a correr estos tasks, necesitas borrar aquello que le indica a luigi que no se vuelva a correr 

Para volver a correr los tasks de metadata, debes entrar a postgres y correr el siguiente comando: 

`DROP TABLE IF EXISTS public.table_updates;`

Este comando borra la tabla en la que luigi guarda la informacion de que ya corrió los tasks de metadata, o cualquier task que implique un CopyToTable, que es guardar cosas en Postgres. 

Para volver a correr los tasks de SaveS3, Transformation, Feature Engineering, debes entrar al S3 de Aws y borrar el pickle del día de la fecha, que está guardado dentro de su respectiva carpeta (ingestion para SaveS3, Transformation y Feature Engineering para los otros dos). 

Por último, para poder correr Extract, debes borrar un archivo que se guarda en la máquina de procesamiento, en la dirección 

`src/pipeline/luigi/ingestion_tmp/consecutive/`

Recuerda: nunca debes borrar la carpeta consecutive, de lo contrario tendrás un error.