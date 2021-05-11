# MODULE FOR AEQUITAS ANALYSIS SAVE METRICS AS PICKLE





"------------------------------------------------------------------------------"
#############
## Imports ##
#############
import pandas as pd
import numpy as np
import sys
import random
import pickle
from datetime import (date, datetime)

## Análisis Aequitas
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from aequitas.plotting import Plot


from src.utils.utils import (
    load_df
)

from src.utils.params_gen import (
    metadata_dir_loc,
    tests_dir_loc,
    aq_metadata,
    aq_metadata_index,
    aq_metadata_csv_name,
)

from src.utils.utils import write_csv_from_df





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


##
def load_selected_model_results(path):
    """
    ...
        args:
            path (string): ...
        returns:
            -
    """

    df = load_df(path)

    return df





"------------------------------------------------------------------------------"
##################################
## Aeaquitas analysis functions ##
##################################


def group(df):
    """
     args:
         df (dataframe):Recibe el data frame que tiene los features sobre los que queremos medir el sesgo entre los diferentes grupos.

     returns:
         -
     """
     # print("Métricas de ")
    #tables
    g = Group()
    xtab, attrbs = g.get_crosstabs(df)
    absolute_metrics = g.list_absolute_metrics(xtab)
    conteos_grupo=xtab[[col for col in xtab.columns if col not in absolute_metrics]]
    metricas_absolutas=xtab[['attribute_name', 'attribute_value']+[col for col in xtab.columns if col in absolute_metrics]].round(2)

    return xtab, conteos_grupo,metricas_absolutas



def bias(df_aeq, xtab):
    """
     args:
         df (dataframe): Recibe el data frame que tiene los features sobre los que queremos medir la disparidad
     returns:
         -
    """
    bias = Bias()
    bdf = bias.get_disparity_predefined_groups(xtab, original_df=df_aeq,
                                               ref_groups_dict={'zip_fake': 'High'},
                                               alpha=0.05, check_significance=True,
                                        mask_significance=True)

    ## Storing metadata
    aq_metadata["value_k"] = list(bdf["k"])[0]

    disparities = bdf[['attribute_name', 'attribute_value'] + bias.list_disparities(bdf)].round(2)



def fairness(bdf):
    """
     args:
         df (dataframe): Recibe el data frame que tiene los features sobre los que queremos medir la equidad.
     returns:
         -
    """
    fair = Fairness()
    fdf = fair.get_group_value_fairness(bdf)

    fdf[['attribute_name', 'attribute_value'] + absolute_metrics +
    bias.list_disparities(fdf) + parity_determinations].round(2)


    ## Storing metadata
    aq_metadata["v_group"] = str(fdf.iloc[0, "attribute_value"])
    aq_metadata["FOR_p"] = str(fdf.iloc[0, "FOR Parity"])
    aq_metadata["FNR_p"] = str(fdf.iloc[0, "FNR Parity"])

#def prep_data(dfx):
    #Load original dataframe with features
    #df_o = pd.read_csv(data_path)
    #df_o = pd.read_csv("../../" + "data/incidentes-viales-c5.csv")
    #df_o.drop(df_o[df_o.delegacion_inicio.isnull()].index, inplace = True)
    #df_aeq=pd.merge(dfx, df_o, on='folio', how='left')
    #df_aeq=df_aeq.loc[:, ['folio','label','score','delegacion_inicio']]

    #return df_aeq





"------------------------------------------------------------------------------"
#####################################
## Aequitas analisis main function ##
#####################################

def bias_fairness(df_aeq):
    """
     args:
         df (dataframe): dataframes that will be analyzed by Aequitas according to the selected model.
     returns:
         -
    """


    ## Storing time execution metadata
    aq_metadata[aq_metadata_index] = str(datetime.now())


    df_aeq = load_selected_model_results(aequitas_df_pickle_loc)
    df_aeq = prep_data(df_aeq)
    df_aeq = df_aeq.rename(columns = {'folio':'entity_id','label': 'label_value'}, inplace = False)



    xtab, conteos_grupo, metricas_absolutas = group(df_aeq)

    df = bias(df_aeq, xtab)

    fairness(df)


    ## Saving relevant module metadata

    # #### Model selected metadata
    # aq_metadata["selected_model"] = str(best_model)

    #### Converting metadata into dataframe and saving locally
    df_meta = pd.DataFrame.from_dict(aq_metadata, orient="index").T
    df_meta.set_index(aq_metadata_index, inplace=True)
    write_csv_from_df(df_meta, metadata_dir_loc, aq_metadata_csv_name)


    print("\n** Aequitas module successfully executed **\n")





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"

