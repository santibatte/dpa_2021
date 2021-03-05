## MODULE WITH INFORMATION ABOUT THE FEATURES THAT ARE INCLUDED IN THE DATASET





"------------------------------------------------------------------------------"
##############################
## Data profiling functions ##
##############################


data_dict = {
    "folio": {
        "relevant": False,
        "data_type": "categoric",
        "model_relevant": False,
        "id_feature": True
    },
    "fecha_creacion": {
        "relevant": True,
        "data_type": "date_and_time",
        "model_relevant": False
    },
    "hora_creacion": {
        "relevant": True,
        "data_type": "date_and_time",
        "model_relevant": False
    },
    "dia_semana": {
        "relevant": True,
        "data_type": "categoric",
        "model_relevant": True
    },
    "codigo_cierre": { ## This feature will only be used to obtain labels and then its dropped.
        "relevant": True,
        "data_type": "categoric",
        "model_relevant": False
    },
    "fecha_cierre": {
        "relevant": False,
        "data_type": "date_and_time",
        "model_relevant": False
    },
    "año_cierre": {
        "relevant": False,
        "data_type": "date_and_time",
        "model_relevant": False
    },
    "mes_cierre": {
        "relevant": False,
        "data_type": "date_and_time",
        "model_relevant": False
    },
    "hora_cierre": {
        "relevant": False,
        "data_type": "date_and_time",
        "model_relevant": False
    },
    "delegacion_inicio": {
        "relevant": False,
        "data_type": "categoric",
        "model_relevant": False
    },
    "incidente_c4": {
        "relevant": True,
        "data_type": "categoric",
        "model_relevant": True
    },
    "latitud": {
        "relevant": False,
        "data_type": "coordenate",
        "model_relevant": False
    },
    "longitud": {
        "relevant": False,
        "data_type": "coordenate",
        "model_relevant": False
    },
    "clas_con_f_alarma": {
        "relevant": False,
        "data_type": "categoric",
        "model_relevant": False
    },
    "tipo_entrada": {
        "relevant": True,
        "data_type": "categoric",
        "model_relevant": True
    },
    "delegacion_cierre": {
        "relevant": False,
        "data_type": "categoric",
        "model_relevant": False
    },
    "geopoint": {
        "relevant": False,
        "data_type": "coordenate",
        "model_relevant": False
    },
    "mes": {
        "relevant": False,
        "data_type": "date_and_time",
        "model_relevant": False
    }
}



data_created_dict = {}





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
