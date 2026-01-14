import re

def normalize_column_name(col_name):
    """
    Transforma 'Customer Name (English)' em 'customer_name_english'.
    Remove caracteres especiais e coloca em snake_case.
    """
    # Remove espaços nas pontas e coloca em minúsculo
    clean_name = col_name.strip().lower()
    
    # Substitui espaços e pontos por underline
    clean_name = clean_name.replace(" ", "_").replace(".", "_")
    
    # Remove qualquer caractere que não seja letra, número ou underline
    clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
    
    # Evita underlines duplicados (ex: customer__name)
    clean_name = re.sub(r'_+', '_', clean_name)
    
    return clean_name