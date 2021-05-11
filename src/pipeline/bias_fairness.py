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

## Análisis Aequitas
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from aequitas.plotting import Plot


from src.utils.utils import (
    load_df,
    save_df
)

#from src.utils.params_gen import (
    #y_test_pickle_loc,
    #test_predict_scores_pickle_loc,
    #metrics_report_pickle_loc,
    #aequitas_df_pickle_loc,
#)



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
    disparities=bdf[['attribute_name', 'attribute_value'] + bias.list_disparities(bdf)].round(2)



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




    xtab, conteos_grupo, metricas_absolutas = group(df_aeq)
    df = bias(df_aeq, xtab)

    # df=fairness(df)

    print("\n** Aequitas module successfully executed **\n")





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"

