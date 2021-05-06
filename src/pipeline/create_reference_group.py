

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
