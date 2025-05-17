from .extract import get_frequency_itemsets
from datetime import datetime
from typing import Dict, List, Tuple

def age_range(age: int) -> str:
    """
    Returns the age range of a given age.
    """
    if age < 18:
        return "0-17"   
    elif age <= 24:
        return "18-24"
    elif age <= 34:
        return "25-34"
    elif age <= 44:
        return "35-44"
    elif age <= 54:
        return "45-54"
    elif age <= 64:
        return "55-64"
    else:
        return "65+"


def product_predominant_profile(
        data: List[tuple]
        ) -> Dict[str, List[Dict[str, str]]]:
    '''
    Returns the genres that most consumed a given product and their age range.
    '''

    products = {}
    for product_id, product_name, gender, age in data:
        if product_name not in products:
            products[product_name] = []
        
        products[product_name].append((gender, age))

    output = []

    for product_name, profiles in products.items():
        gender_count = {'M': 0, 'F': 0}

        age_ranges_count = {}
    
        for gender, age in profiles:
            gender_count[gender] += 1
            range_key = age_range(age)
            age_ranges_count[range_key] = age_ranges_count.get(range_key, 0) + 1

        predominant_gender = 'M' if gender_count['M'] >= gender_count['F'] else 'F'
        predominant_age_range = max(age_ranges_count.items(), key=lambda x: x[1])[0]

        #TODO Improve the logic to return gender or age if have the same count
        
        output.append({
            'Product': product_name,
            'Predominant_Gender': predominant_gender,
            'Predominant_Age_Range': predominant_age_range,
        })

    result = {
        'processing_date': datetime.now().strftime("%d/%m/%Y"),
        'data': output
    }

    return result

def most_common_products(
        data: List[Tuple[str, str, int]]
        ) -> Dict[str, List[Dict[str, str]]]:
    """
    Returns the most common products bought together.

    Args:
        data: Lista de tuplas contendo (produto1_nome, produto2_nome, frequencia)
    """
    output = []

    for produto1_nome, produto2_nome, frequencia in data:
        output.append({
            'Product_1': produto1_nome,
            'Product_2': produto2_nome,
            'Count': frequencia
        })

    result = {
        'processing_date': datetime.now().strftime("%d/%m/%Y"),
        'data': output
    }

    return result
