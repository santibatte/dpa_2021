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





"------------------------------------------------------------------------------"
###############
## Functions ##
###############



## Counting number of variables in data (¿Cuántas variables tenemos?)
def count_vars(data):
    """
    Counting number of variables in data
        args:
            data (dataframe): data that is being analyzed
        returns:
             res (int): number of variables in the data
    """

    res = data.shape[1]
    print("Número de variables en los datos --> {}".format(res))

    return



## Counting number of observations in data (¿Cuántas observaciones tenemos?)
def count_obs(data):
    """
    Counting number of observations in data
        args:
            data (dataframe): data that is being analyzed
        returns:
            res (int): number of observations in the data

    """

    res = data.shape[0]

    print("Número de observaciones en los datos --> {}".format(res))

    return



## Counting number of unique observations for all variables
def count_unique_obs(data):
    """
    Counting number of unique observations for all variables
        args:
        data (dataframe): data that is being analyzed
        returns:
        (series): number of unique observations for all variables
    """
    return data.nunique()



def geo_transformation(data, variable_latlong, variable_drop):
    """
    Get the Latitude and Longitude columns from a specific column,
    then transform both columns to floats and finally remove the original column
        args:
            data (geodataframe): Original data with Geo Point column
            variable (string): Name of column with longitude and latitude data
            variable_drop (string): name of columns that will be dropped.
        returns:
            Geodataframe with columns longitude and latitude
    """

    data[['latitud','longitud']] = data.loc[:, variable_latlong].str.split(",", expand = True)
    data[['latitud','longitud']] = data[['latitud','longitud']].astype('float')
    data = data.drop(columns = [variable_latlong, variable_drop])

    return data



## Function to print the number of decimals that each of the geo columns have
def geo_vars_precision(data, geo_vars):
    """
    Function to print the number of decimals that each of the geo columns have
        args:
            data (dataframe): dataset that contains the geospatial columns (e.g. "latitud" & "longitud")
            geo_vars (list - strings): list with the names of the geospatial columns (e.g. ["latitud", "longitud"])
        returns:
            -
    """

    ## List where the resulting dataframes will be stored for join
    list_dfs_res = []

    ## Looop to create precision dataframes and append to list
    for col in geo_vars:
        df_geo_decs = data[col].astype("str").str.split(pat=".", expand=True).loc[:, 1].str.len().value_counts().to_frame()
        df_geo_decs.columns = ["No. of entries - " + col]
        list_dfs_res.append(df_geo_decs)

    ## Joining precision dataframes and printing result
    dfres = list_dfs_res[0].join(list_dfs_res[1])
    dfres.index.name = "No. of decimals"
    print(display(dfres))

    return



def count_type_vars(vars_sel, type_var):
    """
    Counting number of (numerical / categorical / text)  variables
        args:
            vars_sel (list): selection of columns that comply with the data type
            type_var (string): type of variable that is being counted
        returns:
            -
    """

    ## Creating dataframe to print selected variables
    vars_dict = {i:vars_sel[i-1] for i in range(1, len(vars_sel) + 1)}
    df_print_vars = pd.DataFrame.from_dict(vars_dict, orient="index")
    df_print_vars.columns = ["Variable(s)"]

    print("Número de variables de tipo {} --> {}".format(type_var, len(vars_sel)))
    # print("- Las variables de tipo {} son: \n".format(type_var, vars_sel))
    print(display(df_print_vars))


    return



## Transform columns' names to standard format
def clean_col_names(dataframe):
    """
    Transform columns' names to standard format (lowercase, no spaces, no points)
        args:
            dataframe (dataframe): df whose columns will be formatted.
        returns:
            dataframe (dataframe): df with columns cleaned.
    """

    ## Definition of cleaning funcitons that will be applied to the columns' names
    fun1 = lambda x: x.lower() ## convert to lowercase
    fun2 = lambda x: re.sub("( |¡|!|¿|\?|\.|,|;|:)", "_", x) ## eliminate spaces and punctuation signs for underscore
    fun3 = lambda x: unicodedata.normalize("NFD", x).encode("ascii", "ignore").decode("utf-8") ## substitute accents for normal letters
    funcs = [fun1, fun2, fun3]

    ## Applying the defined functions to the columns' names
    for fun in funcs:
        dataframe.columns = [fun(col) for col in dataframe.columns]

    return dataframe



## Data profiling for numeric variables
def data_profiling_numeric(data, num_vars):
    """
    Data profiling for numeric variables
        Args:
            data(dataframe): dataframe that will be analyzed.
        num_vars (list): list of variables' names in the dataframe that will be analyzed.
        Retruns:
            Dataframe with the data profiling (type, number of observations, mean, sd, quartiles, max, min, unique observations, top 5 repeated observations, number of null variables)
            of the choosen numeric variables.
    """

    ## Copy of initial dataframe to select only numerical columns
    dfx = data.loc[:, num_vars]


    ## Pipeline to create dataframe with general data description
    print("*********************************")
    print("** General description of data **")
    print("*********************************")

    #### List where the resulting dataframes will be stored for further concatenation
    res_dfs = []

    #### Type of numeric variables
    dfx_dtype = dfx.dtypes.to_frame().T
    dfx_dtype.index = ["dtype"]
    res_dfs.append(dfx_dtype)

    #### Counting unique variables
    dfx_uniqvars = dfx.nunique().to_frame().T
    dfx_uniqvars.index = ["count_unique"]
    res_dfs.append(dfx_uniqvars)

    #### Counting missing values
    dfx_missing = dfx.isnull().sum().to_frame().T
    dfx_missing.index = ["missing_v"]
    res_dfs.append(dfx_missing)

    #### General description of the data and addition of min values
    dfx_desc = dfx.describe()
    dfx_desc.loc["min", :] = dfx.min(axis=0)
    res_dfs.append(dfx_desc)

    #### Concatenating resulting dataframes into one final result
    print(display(pd.concat(res_dfs, axis=0)))
    print("-"*75)
    print("-"*75)
    print("\n\n".format())


    ## Pipeline to obtain top repeated variables per column
    print("****************************")
    print("** Top repeated variables **")
    print("****************************")

    #### Initial variables
    tops = 5 #### Number of tops that will be selected
    i = 0 #### Counter to start joining dataframes

    #### Loop through all variables that will be processed
    for col_sel in dfx:

        #### Creating dataframe with top entries and count
        dfxx = dfx[col_sel].value_counts().iloc[:tops].to_frame()
        dfxx.reset_index(drop=False, inplace=True)
        dfxx["part"] = round(dfxx[col_sel]/dfx[col_sel].count()*100, 2)
        dfxx.columns = pd.MultiIndex.from_tuples([(col_sel, tag) for tag in ["value", "count", "part_notnull"]])

        #### Joining all the variables in one final dataframe
        if i == 0:
            df_tops = dfxx
            i += 1
        else:
            df_tops = df_tops.join(dfxx)

    ## Fill empty spaces of resulting dataframe and renaming index entries
    df_tops.fillna("-", inplace=True)
    df_tops.index = ["top_" + str(i) for i in range(1, df_tops.shape[0] + 1)]
    print(display(df_tops))
    print("-"*75)
    print("-"*75)
    print()
    return



def convert_lower(data, vars_lower):
    """
     Converting observatios for selected columns into lowercase.
        args:
            data (dataframe): data that is being analyzed.
            vars_lower (list): list of the columns' names in the dataframe that will be changed to lowercase.
        returns:
            data(dataframe): dataframe that is being analyzed with the observations (of the selected columns) in lowercase.
    """
    for x in vars_lower:
        data[x]=data[x].str.lower()
    return data



## Function to correct selected entries in dataframe
def correct_selected_entries(data, correction_dict):
    """
    Function to correct selected entries in dataframe
        args:
            data (dataframe): dataset that contains entries that need to be corrected
        returns:
            correction_dict (dictionary): reference to correct bad entries
                dictionary format:
                    > keys --> name of the column that has bad entries
                    > values --> dictionary that contains bad word and required word (e.g. {catt: cat})
    """

    ## Loop to apply corrections
    for corr_col in correction_dict:
        for bad_ent in correction_dict[corr_col]:
            data[corr_col] = data[corr_col].replace(bad_ent, correction_dict[corr_col][bad_ent])

    return data



def proporcion(listaVar,n):
    """
    Calculate the data proportion of categorical variables.
        args:
            listaVar (Serie): Serie with unique values of categorical variables
                               to get use value_counts() into a Serie
            n (int): value of total observation of data set.
        returns:
           newList(list): List with name, count and proportion of each category.
    """
    newList = []
    for lis in listaVar.iteritems():
        newList.append([lis[0],lis[1],"{}%".format(round(100*(lis[1]/n),1))])
    return newList



def data_profiling_categ(data, cat_vars):
    """
    Create the data profiling of categorical variables.
        args:
            data (Data Frame): data set into Dataframe.
            cat_vars (list): list with categorical variables names.
        returns:
           display(): display the Dataframes with info.
    """

    for val in cat_vars:
        print("*********************************")
        print("Variable Categorica {}".format(val))
        print("*********************************")

        catego  = data[val].value_counts()
        totalOb = len(data[val])
        can_Cat = len(catego)
        moda    = data[val].mode().values[0]
        valFal  = data[val].isnull().sum()
        top1    = [catego[0:1].index[0],catego[0:1].values[0]] if can_Cat >= 1 else 0
        top2    = [catego[1:2].index[0],catego[1:2].values[0]] if can_Cat >= 2 else 0
        top3    = [catego[2:3].index[0],catego[2:3].values[0]] if can_Cat >= 3 else 0

        elemVarCat = {
            "Info":val,
            "Num_Registros":[totalOb],
            "Num_de_categorias":[can_Cat],
            "Moda":[moda],
            "Valores_faltantes":[valFal],
            "Top1":[top1],
            "Top2":[top2],
            "Top3":[top3]
            }

        #primerdataframe
        df_catVar = pd.DataFrame(elemVarCat).set_index("Info").T

        #mostrar primer data frame
        print(display(df_catVar))

        print("Valores de las categorias y sus proporciones")
        #segundodataframe donde se muestra los valores de las categorias su cantidad y su proporción.
        pro = proporcion(catego,totalOb)
        dfProp = pd.DataFrame(pro,columns=['Categoría', 'Observaciones', 'proporción']).set_index("Categoría")
        #mostrar primer data frame
        print(display(dfProp))
        print("\n\n".format())
    return





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
