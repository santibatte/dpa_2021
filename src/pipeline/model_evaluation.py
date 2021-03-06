## MODEL EVALUATION





"------------------------------------------------------------------------------"
#############
## Imports ##
#############


##

import pickle
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
import numpy as np
import sys
from sklearn.metrics import (
    roc_curve, roc_auc_score,
    confusion_matrix,
    precision_recall_curve,
    accuracy_score,
    precision_score,
    recall_score
)


##

from src.utils.utils import (
    load_df,
    save_df
)

from src.utils.params import (
    y_test_pickle_loc,
    test_predict_scores_pickle_loc,
    metrics_report_pickle_loc,
    aequitas_df_pickle_loc
)





"------------------------------------------------------------------------------"
#################################
## Generic ancillary functions ##
#################################


##
def load_model(path):
    """
    ...
        args:
            path (string): ...
        returns:
            -
    """

    df = load_df(path)

    return df



## ...
def save_metrics(metrics_report, path):
    """
    ...
        args:
            ...
        returns:
            -
    """

    save_df(metrics_report, path)





"------------------------------------------------------------------------------"
###################################
## Modeling evaluation functions ##
###################################


##
def f_predicted_labels(y_test,predicted_scores,umbral):
	dfx = pd.DataFrame(y_test)
	dfx["1_prob"] = predicted_scores[:, 1]
	dfx["prob_label"] = dfx["1_prob"].apply(lambda x: 1 if x >= umbral else 0)
	dfx["correct"] = dfx.apply(lambda x: True if x["label"] == x["prob_label"] else False, axis=1)
	predicted_labels = dfx['prob_label']

	return predicted_labels



##
def curva_roc(y_test,predicted_labels,fpr,tpr):
	plt.clf()
	plt.plot([0,1],[0,1], 'k--', c="red")
	plt.plot(fpr, tpr)
	plt.title("ROC best RF, AUC: {}".format(roc_auc_score(y_test, predicted_labels)))
	plt.xlabel("fpr")
	plt.ylabel("tpr")

	return plt.show()



##
def get_metrics_report(fpr, tpr, thresholds, precision, recall, thresholds_2):
    df_1 = pd.DataFrame({'threshold': thresholds_2,'precision': precision,
                    'recall': recall})
    df_1['f1_score'] = 2 * (df_1.precision * df_1.recall) / (df_1.precision + df_1.recall)

    df_2 = pd.DataFrame({'tpr': tpr, 'fpr': fpr, 'threshold': thresholds})
    df_2['tnr'] = 1 - df_2['fpr']
    df_2['fnr'] = 1 - df_2['tpr']

    df = df_1.merge(df_2, on="threshold")

    return df



##
def tabla_referencia():
	tabla = tabulate(np.array([['True Positive (tp)', 'False Negative (fn)'],
		['False Positive (fp)', 'True Negative (tn)']]),
	headers=['Dato\Predicción','Etiqueta +','Etiqueta -'],
	showindex=['Etiqueta +','Etiqueta -'],
	tablefmt='pretty')

	return tabla



##
def tabla_confusion(data):
	tabla = tabulate(data,
	headers=['Dato\Predicción','Etiqueta +','Etiqueta -'],
	showindex=['Etiqueta +','Etiqueta -'],
	tablefmt='pretty')

	return tabla



##
def precision_at_k(y_true, y_scores, k):
	threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
	y_pred = np.asarray([1 if i >= threshold else 0 for i in y_scores])

	return precision_score(y_true, y_pred)



##
def recall_at_k(y_true, y_scores, k):
	threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
	y_pred = np.asarray([1 if i >= threshold else 0 for i in y_scores])

	return recall_score(y_true, y_pred)



##
def curva_pre_re(y_test,y_scores,k_espe):
	k_values = list(np.arange(0.01, 0.99, 0.01))
	d = pd.DataFrame(index=range(len(k_values)),columns=['k','precision','recall'])
	for k in range(len(k_values)):
		d['k'][k] = k_values[k]
		d['precision'][k]=precision_at_k(y_test,y_scores,k_values[k])
		d['recall'][k]=recall_at_k(y_test,y_scores,k_values[k])

	fig, ax1 = plt.subplots()
	ax1.plot(d['k'], d['precision'], label='precision@k')
	ax1.plot(d['k'], d['recall'], label='recall@,')
	plt.axvline(k_espe,color='r')
	plt.title("Precision and Recall at k%")
	plt.xlabel("k%")
	plt.ylabel("best_value")
	plt.legend()





"------------------------------------------------------------------------------"
#######################################
## Modeling evaluation main function ##
#######################################


##
def model_evaluation(y_test_pickle_loc, test_predict_scores_pickle_loc):
    """
    """

    ## Importing raw entries
    y_test = load_df(y_test_pickle_loc)
    predicted_scores = load_df(test_predict_scores_pickle_loc)

    ##
    predicted_labels = f_predicted_labels(y_test, predicted_scores, 0.18)

    fpr, tpr, thresholds = roc_curve(y_test, predicted_scores[:,1], pos_label=1)

    print("\n++ Tabla de referencia: \n{}\n".format(tabla_referencia()))
    print("++ Tabla de confusión 1: \n{}\n".format(tabla_confusion(confusion_matrix(y_test, predicted_labels, normalize='all'))))
    print("++ Tabla de confusión 2: \n{}\n".format(tabla_confusion(confusion_matrix(y_test, predicted_labels))))
    print("\n++ Score accuracy: {}\n".format(accuracy_score(y_test, predicted_labels)))

    precision, recall, thresholds_2 = precision_recall_curve(y_test, predicted_scores[:,1], pos_label=1)

    thresholds_2 = np.append(thresholds_2, 1)

    metrics_report = get_metrics_report(fpr, tpr, thresholds, precision, recall, thresholds_2)

    save_metrics(metrics_report, metrics_report_pickle_loc)

    negocio = metrics_report[metrics_report.fpr <= 0.06]

    punto_corte = negocio.head(1).threshold.values[0]

    new_labels = [0 if score < punto_corte else 1 for score in predicted_scores[:,1]]

    print("++ Tabla de confusión reajustada 1: \n{}\n".format(tabla_confusion(confusion_matrix(y_test, new_labels, normalize='all'))))

    k=20/551

    print("\n++ Precision @k: {}".format(precision_at_k(y_test, predicted_scores[:,1], k)))
    print("\n++ Recall @k: {}\n".format(recall_at_k(y_test,predicted_scores[:,1],k)))

    df_aeq = pd.DataFrame(y_test)
    df_aeq["score"] = new_labels

    save_metrics(df_aeq, aequitas_df_pickle_loc)

    print("\n** Model evaluation module successfully executed **\n")





"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
"------------------------------------------------------------------------------"
