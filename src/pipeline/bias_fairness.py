# MODULE FOR AEQUITAS ANALYSIS SAVE METRICS AS PICKLE





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler
)
import pandas as pd
import numpy as np
import seaborn as sns
import sys


## separando en train, test
from sklearn.model_selection import train_test_split
## Configuración del RF
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import time
from sklearn.preprocessing import StandardScaler, OneHotEncoder, KBinsDiscretizer
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
import matplotlib.pyplot as plt
import random
import pickle

## Análisis Aequitas
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from aequitas.plotting import Plot

from src.utils.utils import (
    load_df
)

from src.utils.params import (
    data_path,
    aequitas_df_pickle_loc
)





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
    bdf = bias.get_disparity_predefined_groups(xtab,
                                               original_df=df_aeq,
                                        ref_groups_dict={'delegacion_inicio':'IZTAPALAPA'},
                                        alpha=0.05, check_significance=True,
                                        mask_significance=True)
    bdf[['attribute_name', 'attribute_value'] + bias.list_disparities(bdf)].round(2)



def fairness(df):
    """
     args:
         df (dataframe): Recibe el data frame que tiene los features sobre los que queremos medir la equidad.
     returns:
         -
    """




def prep_data(dfx):
    #Load original dataframe with features
    df_o = pd.read_csv(data_path)
    #df_o = pd.read_csv("../../" + "data/incidentes-viales-c5.csv")
    df_o.drop(df_o[df_o.delegacion_inicio.isnull()].index, inplace = True)
    df_aeq=pd.merge(dfx, df_o, on='folio', how='left')
    df_aeq=df_aeq.loc[:, ['folio','label','score','delegacion_inicio']]

    return df_aeq





"------------------------------------------------------------------------------"
#####################################
## Aequitas analisis main function ##
#####################################

def bias_fairness(aequitas_df_pickle_loc):
    """
     args:
         df (dataframe): dataframes that will be analyzed by Aequitas according to the selected model.
     returns:
         -
    """

    df_aeq = load_selected_model_results(aequitas_df_pickle_loc)
    df_aeq = prep_data(df_aeq)
    df_aeq = df_aeq.rename(columns = {'folio':'entity_id','label': 'label_value'}, inplace = False)

    by_del = sns.countplot(x="delegacion_inicio", hue="label_value",
                            data=df_aeq[df_aeq.delegacion_inicio.isin(['COYOACAN', 'GUSTAVO A. MADERO', 'IZTAPALAPA', 'TLAHUAC',
           'MIGUEL HIDALGO', 'CUAUHTEMOC', 'CUAJIMALPA', 'ALVARO OBREGON',
           'BENITO JUAREZ', 'TLALPAN', 'VENUSTIANO CARRANZA', 'IZTACALCO',
           'XOCHIMILCO', 'MAGDALENA CONTRERAS', 'AZCAPOTZALCO', 'MILPA ALTA'])])
    # l=plt.setp(by_del.get_xticklabels(), rotation=90)

    by_del = sns.countplot(x="delegacion_inicio", hue="score",
                            data=df_aeq[df_aeq.delegacion_inicio.isin(['COYOACAN', 'GUSTAVO A. MADERO', 'IZTAPALAPA', 'TLAHUAC',
           'MIGUEL HIDALGO', 'CUAUHTEMOC', 'CUAJIMALPA', 'ALVARO OBREGON',
           'BENITO JUAREZ', 'TLALPAN', 'VENUSTIANO CARRANZA', 'IZTACALCO',
           'XOCHIMILCO', 'MAGDALENA CONTRERAS', 'AZCAPOTZALCO', 'MILPA ALTA'])])
    # l=plt.setp(by_del.get_xticklabels(), rotation=90)

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
