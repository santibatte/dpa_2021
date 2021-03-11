# (shebang incomplete) -> bin/???

## FILE TO STORE FUNCTIONS USED IN LAB_1





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports
import re

import unicodedata

import sys

import json

import pickle


## Third party imports
import pandas as pd
from pandas_profiling import ProfileReport
pd.set_option('display.max_columns', 100)

import plotly.express as px

import numpy as np

import seaborn as sns

import probscale

from scipy import stats

import plotly.graph_objects as go
import plotly.express as px
import plotly.figure_factory as ff

import matplotlib.pyplot as plt


## Local application imports

from src.utils.data_dict import (data_created_dict)





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


## Pretty print a dictionary and preserving special characters
def json_dump_dict(dictionary):
    """
    Pretty print a dictionary and preserving special characters
        args:
            dictionary (dictionary): dict that will be pretty printed
        returns:
            -
    """

    print(json.dumps(dictionary, indent=4, ensure_ascii=False).encode("utf8").decode())

    return



## Function to create entries that will populate the data_created_dict.
def update_data_created_dict(var, relevant=True, data_type="not_specified", model_relevant=False):
    """
    Function to create entries that will populate the data_created_dict.
        args:
            var (string): name of entry.
            relevant (boolean): option to specify whether the entry is relevant or not (maybe this is an usless tag).
            data_type (string): type of data related to the entry (e.g. categoric, numeric).
            model_relevant (boolean): option to specify whether the entry will be fed to the model.
        returns:
            # entry (dictionary): dictionary with information about the entry.
            -
    """

    entry = {
        var: {
            "relevant": relevant,
            "data_type": data_type,
            "model_relevant": model_relevant
        }
    }

    data_created_dict.update(entry)


    return



## Loading pickle.
def load_df(path):
    """
    Loading pickle.
        args:
            path (string): specific location of pickle that will be loaded.
        returns:
            pickle_load (*various*): loaded pickle object; most likely a dataframe.
    """

    pickle_load = pickle.load(open(path, "rb"))

    return pickle_load



## Dataframe that will be saved as picke object at specified path.
def save_df(df, path):
    """
    Dataframe that will be saved as picke object at specified path.
        args:
            df (dataframe): dataframe that will be converted and saved as pickle.
            path (string): location where the pickle will be placed.
        returns:
            -
    """

    pickle.dump(df, open(path, "wb"))





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


## Pretty print a dictionary and preserving special characters
def json_dump_dict(dictionary):
    """
    Pretty print a dictionary and preserving special characters
        args:
            dictionary (dictionary): dict that will be pretty printed
        returns:
            -
    """

    print(json.dumps(dictionary, indent=4, ensure_ascii=False).encode("utf8").decode())

    return



## Function to create entries that will populate the data_created_dict.
def update_data_created_dict(var, relevant=True, data_type="not_specified", model_relevant=False):
    """
    Function to create entries that will populate the data_created_dict.
        args:
            var (string): name of entry.
            relevant (boolean): option to specify whether the entry is relevant or not (maybe this is an usless tag).
            data_type (string): type of data related to the entry (e.g. categoric, numeric).
            model_relevant (boolean): option to specify whether the entry will be fed to the model.
        returns:
            # entry (dictionary): dictionary with information about the entry.
            -
    """

    entry = {
        var: {
            "relevant": relevant,
            "data_type": data_type,
            "model_relevant": model_relevant
        }
    }

    data_created_dict.update(entry)


    return



## Loading pickle.
def load_df(path):
    """
    Loading pickle.
        args:
            path (string): specific location of pickle that will be loaded.
        returns:
            pickle_load (*various*): loaded pickle object; most likely a dataframe.
    """

    pickle_load = pickle.load(open(path, "rb"))

    return pickle_load



## Dataframe that will be saved as picke object at specified path.
def save_df(df, path):
    """
    Dataframe that will be saved as picke object at specified path.
        args:
            df (dataframe): dataframe that will be converted and saved as pickle.
            path (string): location where the pickle will be placed.
        returns:
            -
    """

    pickle.dump(df, open(path, "wb"))





"------------------------------------------------------------------------------"
########################
## Plotting functions ##
########################


## Function to create bar plots for categorical data
def barplot_cat(data, col_name, tops=10):
    """
    Function to create bar plots for categorical data
        args:
            data (dataframe): data table where the column that will be plotted is.
            col_name (string): name of column that will be plotted.
            tops (int): number of bars that will be displayed before all others categories are grouped.
        returns:
            -
    """


    ## Function parameters
    work_df = data.copy()
    group_cats = True
    other_cats_tag = "Otras_categs"


    ## Obtaining dataframe ready for bar-plot

    #### Counting category values
    work_df.fillna("_faltante_", inplace=True)
    dfx = work_df[col_name].value_counts().to_frame()

    #### Saving relevant metrics before cuting the df
    tot_count = dfx[col_name].sum()
    tot_cats = dfx.shape[0]
    otras_cats = tot_cats - tops

    #### Evaluate if grouping will take place
    if tot_cats < tops:
        group_cats = False


    ## Grouping other cats
    if group_cats == True:

        #### Filtering top entries
        dfx = dfx[:tops]

        #### Grouping categories out of tops
        dfx.loc[other_cats_tag, :] = tot_count - dfx[col_name].sum()


    ## Crating bar graph
    fig = px.bar(
        dfx,
        x = dfx.index,
        y = col_name,
        title = col_name,
        text = col_name,
        labels = {
            "index": ""
        }
    )

    fig.show()


    #### Printing results of "Otros"
    if group_cats == True:
        print("Otras_categs contiene la siguiente información: ")
        print("    -> {} categorías ({:.2f}%)".format(otras_cats, otras_cats/tot_cats*100))
        print("    -> su conteo de valores representa el ({:.2f}%) del conteo total".format(dfx.loc[other_cats_tag, col_name]/tot_count*100))


    return



## Function to create Rugplot (Carpet) of numerical variable
def rugplot_num(data, col_name):
    """
    Function to create Rugplot (Carpet) of numerical variable
        args:
            data (dataframe): table with the column that will be plotted
            col_name (string): name of column that will be plotted
        returns:
            -
    """


    ## Creating figure
    fig = px.histogram(
        data,
        x=col_name, y=col_name,
        marginal="rug"
    )

    ## Formatting figure
    fig.update_layout(
        title = "Distribución de variable {}".format(col_name),
        xaxis_title = "Valor de la variable",
        yaxis_title = "",
    )

    fig.show()



## Function to create density estimate plot (Distplot)
def distplot_num(data, col_name, data_to_see):
    """
    Function to create density estimate plot (Distplot)
        args:
            data (dataframe): table with the column that will be plotted
            col_name (string): name of column that will be plotted
            data_to_see (int): percentage of data that will be displayed in the graph (takes values from 0 to 98)
        returns:
            -
    """


    ## Copying original data to get working data
    working_data = data.loc[:, [col_name]].copy()

    ## Adding quantiles to data to select how much data you want to see
    ntiles = 100
    working_data["quantile"] = pd.cut(working_data[col_name], ntiles, labels=list(np.linspace(1, ntiles, ntiles)))

    ## Defining the limits of the data that will be displayed
    if data_to_see <= 98:
        data_li = int((100 - data_to_see)/2)
        data_ls = int(100 - (100 - data_to_see)/2)
    else:
        data_li = 1
        data_ls = 100

    ## Filtering data according to limits
    mask_limits = (working_data["quantile"] > data_li) & (working_data["quantile"] < data_ls)
    data_filtered = working_data[mask_limits]


    ## Creating plot
    fig = sns.distplot(data_filtered[col_name], hist=False)
    fig.set_title("Distribución de la variable {}".format(col_name))
    fig.set_ylabel("")


    return



## Function to create histograms
def histograms_numeric_total(data,col_name):
    """
    Function to create histograms
        args:
            data (dataframe): data that will be analized
            col_name (string): name of the column that will be plotted.

        returns:
            -
    """
    fig=px.histogram(data, x=col_name)
    fig.show()

    return



## Function to create histograms by response variable.
def histograms_numeric(data, col_name,name_hue):
    """
       Function to create histograms
        args:
            data (dataframe): data that will be analized
            col_name (string): name of the column that will be plotted.
            name_hue (string): name of the column for hue variable.

        returns:
            -
     """
    data["col_name_new"]=np.log(data[col_name])
    fig=px.histogram(data, x="col_name_new", color=name_hue, labels={'col_name_new':col_name})
    fig.update_traces(opacity=.75)
    #fig.update_xaxes(range=[0,1.5*(data[col_name].quantile(.75)-data[col_name].quantile(.25))])
    fig.show()

    return



## Function to create histograms by response variable and categoric variable.
def histograms_numeric_rv_cat(data, col_name, response_var,cat_var_selec):
    """
       Function to create histograms
        args:
            data (dataframe): data that will be analized
            col_name (string): name of the column of the numeric variable that will be plotted.
            response_var(string): name of the column of the categoric response variable.
            cat_var_selec(string):name of the column of the categoric variable to be analized.
        returns:
            -
     """
    g=sns.FacetGrid(data, col=response_var, row=cat_var_selec,margin_titles=True)
    g.map_dataframe(sns.histplot, x=col_name)
    #IQR=1.5*(data[col_name].quantile(.75)-data[col_name].quantile(.25))
    g.set(xlim=(0,10000))
    g.set_axis_labels(col_name,"Count")
    g

    return



def box_plot_num(data,response_var, col_name):
    """
       Function to create boxplots for numeric variables.
        args:
            data (dataframe): data that will be analized
            col_name (string): name of the column that will be plotted.
            response_var(string): name of the column of the categoric response variable.

        returns:
            -
     """
    bp=px.box(data, x=response_var, y=col_name)
    return bp.show()



def scatterPlotFacet(df,columnX,columnY,hueName,facetName):
    """
    Create the scatter plot of numerical variables.
        args:
            df (Data Frame): data set into Dataframe.
            columnX (str):   name of column into de x axis
            columnY (str):   name of column into de y axis
            hueName (str):   name of column for de hue color of plot.
            facetName (str): name of column for facet spread plots
        returns:
           fig.show(): display the de scaterplot.
    """
    fig = px.scatter(df, x=columnX, y=columnY, color=hueName, facet_col=facetName)
    fig.show()

    return



def corr_plot(data, variables, title):
    """
    Function to create the correlation plot of the input variables
    """

    df_corr = data[variables].corr()

    f, axes = plt.subplots(figsize = (10, 6), gridspec_kw = {'hspace': 1, 'wspace': 0.5})
    plot = sns.heatmap(df_corr, annot = True, center = 0,
            xticklabels=df_corr.columns, cmap="YlGnBu",
            yticklabels=df_corr.columns).set_title(title)

    return plot



def equality_line(ax, label = None):
    """
    Function to add identity line in the qq-plot
    """

    limits = [
        np.min([ax.get_xlim(), ax.get_ylim()]),
        np.max([ax.get_xlim(), ax.get_ylim()]),
    ]
    ax.set_xlim(limits)
    ax.set_ylim(limits)
    ax.plot(limits, limits, 'k-', alpha = 0.75, zorder = 0, label = label)



def qq_plot(data, variable, ymin = -np.inf, ymax = np.inf):
    """
    Create qq-plot
    """

    trunc_data = data.loc[(data[variable] >= ymin) & (data[variable] <= ymax), :]
    val = trunc_data.shape[0]/data.shape[0]

    print("Porcentaje de datos conservado {}".format(val))

    norm = stats.norm(loc = 21, scale = 8)
    fig, ax = plt.subplots(figsize = (4, 4))
    ax.set_aspect('equal')

    common_opts = dict(
        plottype = 'qq',
        probax = 'x',
        problabel = 'Theoretical Quantiles',
        datalabel = 'Emperical Quantiles',
        scatter_kws = dict(label=variable)
    )

    fig = probscale.probplot(trunc_data[variable], ax = ax, dist = norm, **common_opts)

    equality_line(ax, label = 'Normal Distribution')
    ax.legend(loc = 'lower right')
    sns.despine()



def box_plot_num_location(data,col_name, alcaldia_selec):
    """
       Function to create boxplots for numeric variables.
        args:
            data (dataframe): data that will be analized
            col_name (string): name of the column that will be plotted.
            alcaldia_selec(string): name of the column of the categoric location variable.

        returns:
            -
    """
    dfx=data[data.alcaldia== alcaldia_selec]
    bp=px.box(dfx, x="indice_des", y=col_name)
    return bp.show()



## Create heatmap
def create_heatmap(data, col1, col2, count_col):
    """
    """


    ## Conteo de las observaciones por alcaldía clasificadas por indice de desarrollo
    dfx = data.copy()
    dfx2 = dfx.groupby([col1, col2])[count_col].count().unstack().fillna(0)


    ## Conversión de valores absoultos a proporcionales
    dfx2["total"] = dfx2.sum(axis=1)
    for col in [x for x in dfx2.columns if x != "total"]:
        dfx2[col + "_part"] = dfx2[col]/dfx2["total"]*100
        dfx2.drop(col, axis=1, inplace=True)
    dfx2.drop(["total"], axis=1, inplace=True)


    ## Heatmap con los resultados
    fig = px.imshow(dfx2)
    fig.update_layout(
        autosize=False,
        width=500,
        height=1000
    )
    fig.show()



## Evaluate consistency in colonia development tags
def colonia_devidx_consistency(data):
    """
    """


    ## Grouping and counting by "colonia" and "indice_des"
    dfx3 = data.groupby(["colonia", "indice_des"])["gid"].count().unstack()

    ## Determining proportion of entries per number of categories
    dfx3["categs"] = dfx3.notnull().sum(axis=1)
    dfx3 = dfx3["categs"].value_counts(normalize=True).to_frame()

    ## Adding text column to be more clear
    dfx3["categs_txt"] = dfx3.index.astype("str") + "_" + "categs"


    ## Creating plot
    fig = px.bar(dfx3, x="categs_txt", y="categs")
    fig.update_layout(
        title="Propoción del número de índices de desarrollo por colonia",
        xaxis_title="Número de categorías en la colonia",
        yaxis_title=""
    )
    fig.show()



## Creating scatter plot with coordinates
def scatter_map(data):
    plt.figure(figsize=(20,20))
    sns.scatterplot(data.longitud, data.latitud, hue=data.indice_des)
    plt.ioff()



## Creating graphs to analyze consuption distribution
def cons_dist_plots(data):
    """
    """


    dfs = []

    for key in keywords:

        col_sel = [col for col in data.columns if ("consumo" in col) & (key in col) & (col != keywords[key]["non"])]
        col_sel.append("indice_des")


        df_sel = data.loc[:, col_sel]
        df_sel = df_sel.groupby(["indice_des"]).sum()
        df_sel["total_sum"] = df_sel.sum(axis=1)


        for col in [col for col in df_sel.columns if "total_sum" not in col]:
            df_sel[col + "_prop"] = df_sel[col]/df_sel["total_sum"]
            df_sel.drop(col, axis=1, inplace=True)
        df_sel.drop("total_sum", axis=1, inplace=True)
        df_sel.rename(columns=keywords[key]["cols_names"], inplace=True)


        dfs.append(df_sel)

    fig_avg = go.Figure()

    for col in dfs[0].columns:
        fig_avg.add_trace(
            go.Bar(
                x = dfs[0].index,
                y = dfs[0][col],
                name = col,
                marker_color=plot_colors[col]
            )
        )

    fig_avg.update_layout(
        barmode="stack",
        title="Distribución del consumo promedio"
    )

    fig_avg.show()

    fig_tot = go.Figure()

    for col in dfs[1].columns:
        fig_tot.add_trace(
            go.Bar(
                x = dfs[1].index,
                y = dfs[1][col],
                name = col,
                marker_color=plot_colors[col]
            )
        )

    fig_tot.update_layout(
        barmode="stack",
        title="Distribución del consumo total"
    )

    fig_tot.show()



## Total consumption por alcaldía
def consumoPerAlcaldia(df, tipoConsumo):
    dffirst = df.groupby(["alcaldia", "indice_des"])["gid"].count().unstack().fillna(1).reset_index()
    dffirst.columns.name = None
    df_stat =df.groupby("alcaldia").agg(consumo_total=(tipoConsumo,"max")).reset_index()
    catego  = df["alcaldia"].value_counts()
    totalOb = len(df["alcaldia"])
    pro = proporcion(catego,totalOb)
    dfProp = pd.DataFrame(pro,columns=['alcaldia', 'frecuencia', 'proporcion'])
    merL1 = pd.merge(left=dfProp,right=df_stat, how='left', left_on='alcaldia', right_on='alcaldia')
    merL2 = pd.merge(left=merL1,right=dffirst,
                 how='left',
                 left_on='alcaldia',
                 right_on='alcaldia').sort_values(by=[tipoConsumo],ascending=False)
    merL2 = merL2.reindex(columns=['alcaldia','frecuencia','proporcion',tipoConsumo,'popular','bajo','medio','alto'])


    print(display(merL2))

    sns.set_style("darkgrid")
    plt.figure(figsize=(13,9))
    ax = sns.barplot(x="consumo_total", y="alcaldia", data=merL2)
    ax.set_yticklabels(ax.get_ymajorticklabels(), fontsize = 15)
    plt.title('Consumo total por Alcaldia')

    return



# Definimos una función para hacer los heatmaps:
def heatmaps(data,choose_year,axis,yes,ylabel):
    data1 = data.loc[data['year'] == choose_year]
    data1 = data1.pivot_table(index="day",columns="month",values='conteos')
    if yes == 'yes':
        heat = sns.heatmap(data1, cbar=True, ax=axis,cmap="RdBu")
    else:
        heat = sns.heatmap(data1, cbar=False, ax=axis,cmap="RdBu")
    heat.set_title(str(choose_year),fontsize=15)
    heat.set_xlabel('Month',fontsize=15)
    heat.set_ylabel(ylabel,fontsize=15)
    heat.set_yticklabels(heat.get_yticklabels(), rotation = 0)
    heat.set_xticklabels(heat.get_xticklabels())#, rotation = 90, fontsize = 8)
    return heat





"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
