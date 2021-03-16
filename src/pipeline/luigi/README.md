Correr Luigi: 

1. Posicionarse en la carpeta root del repo desde la terminal y correr la siguiente linea que incluye nuestro repo en el pythonpath: 
``export PYTHONPATH=$PWD``

2. Para correr el primer task ``APIDataIngestion``que guarda los datos de la api, correr el siguiente comando: 


``luigi --module src.pipeline.luigi.extract APIDataIngestion --ingest-type initial --local-scheduler``


-La ingest-type puede ser ``ìnitial`` o ``consecutive``
-La task no debería realizarse si ya existe un archivo .pkl en la carpeta ``luigi_tmp_files``

3. Para correr el segundo task ``S3Task`` correr el siguiente comando:


``luigi --module src.pipeline.luigi.save_s3 S3Task --ingest-type initial --bucket data-product-architecture-equipo-9 --root-path ingestion --year 2021 --month 03 --local-scheduler``

El ingest-type puede ser ``initial``o ``consecutive``
El año y mes pueden modificarse. 
Este task no debería correrse si ya está guardado el .pkl en el bucket y carpetas correspondientes del S3. 

PROBLEMAS / pendientes del checkpoint 3: 

-Necesitamos que el archivo temporal que se guarda, se guarde con la fecha del dia y que diga si fue una ingesta inicial o consecutiva en el nombre del archivo guardado. 
-Necesitamos que el segundo task, cuando se corre, vaya a fijarse si ya existe el archivo temporal dependiendo de si el parametro que se le paso es inicial o consecutive. 
-Necesitamos que task APIDataIngestion, cuando recibe la instruccion consecutive, vaya a mirar a la direccion creada por este mismo task, para saber cual fue la ultima descarga de archivos y descargue a partir de la fecha presente. 

-Cuando corremos el segundo task, y el primer task no se ha corrido, obtenemos el siguiente error inexplicable:

  File "/Users/sansansansan/repos_itam/dpa_2021/src/pipeline/luigi/extract.py", line 115, in run
    pickle.dump(ingesta, output_file)
UnboundLocalError: local variable 'ingesta' referenced before assignment

Si se obtiene el siguiente error: 

  File "/Users/sansansansan/repos_itam/dpa_2021/src/pipeline/luigi/save_s3.py", line 34, in run
    ingesta=pickle.load(open('src/pipeline/luigi/luigi_tmp_files/ingesta_tmp.pkl', 'rb'))
EOFError: Ran out of input


Quiere decir que el archivo tmp guardado se guardó con 0 bytes, error que a veces ocurre cuando se obtuvo el error anterior a este. 

