


high_income_zip_codes = [60606, 60601, 60611, 60614, 60603, 60655, 60646, 60605, 60657, 60631, 60661, 60652, 60643, 60610,60634, 60613]

medium_income_zip_codes = [60630, 60656, 60638, 60659, 60641, 60645, 60618, 60607, 60633, 60629, 60639, 60625, 60622, 60628, 60632, 60620, 60617, 60647]

low_income_zip_codes = [60660, 60619, 60651, 60640, 60615, 60626, 60604, 60616, 60623, 60608, 60636, 60649, 60644, 60609, 60612, 60602, 60637, 60624, 60621, 60653, 60654]


def create_reference_group(df):
    """
    df a df with zip code
    returns a df with new column zip_income_classification which can be used as a reference group in aequitas analysis
    """

    zip_food_list=list(df['Zip'])

    classification_food = []

    for val in zip_food_list:
        if val in high_income_zip_codes:
            classification_food.append('High')
        elif val in medium_income_zip_codes:
            classification_food.append('Medium')
        elif val in low_income_zip_codes:
            classification_food.append('Low')
        else:
            classification_food.append('Other')

    df['Zip_income_classification'] = classification_food

    return df
