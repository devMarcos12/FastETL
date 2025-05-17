from typing import Dict, List, Tuple
import logging
from ..db.postgree_connection import postgres_cursor, dict_cursor

logger = logging.getLogger(__name__)

def fetch_transaction_products() -> list:
    """
    For each sell made, return the product id, product
    """
    try:
        with postgres_cursor() as cursor:
            cursor.execute('''
                SELECT
                    p.id as product_id,
                    p.nome as product_name,
                    c.gender,
                    c.age
                FROM produtos p
                JOIN itens_venda iv ON p.id = iv.id_produto
                JOIN vendas v ON iv.id_venda = v.id
                JOIN clientes c ON v.id_cliente = c.id
            ''')
            data = cursor.fetchall()
            logger.info(f"Fetched {len(data)} transaction products")
            return data
    except Exception as e:
        logger.error(f"Error fetching transaction products: {e}")
        raise

def get_frequency_itemsets() -> List[Tuple[str, str, int]]:
    """
    Returns the frequency of itemsets (product pairs)
    that were sold together more than 3 times.
    """
    try:
        with postgres_cursor() as cursor:
            cursor.execute('''
                WITH produtos_por_venda AS (
                    SELECT
                        v.id as venda_id,
                        p1.id as produto1_id,
                        p1.nome as produto1_nome,
                        p2.id as produto2_id,
                        p2.nome as produto2_nome
                    FROM vendas v
                    JOIN itens_venda iv1 ON v.id = iv1.id_venda
                    JOIN produtos p1 ON iv1.id_produto = p1.id
                    JOIN itens_venda iv2 ON v.id = iv2.id_venda
                    JOIN produtos p2 ON iv2.id_produto = p2.id
                    WHERE p1.id < p2.id
                )
                SELECT
                    produto1_nome,
                    produto2_nome,
                    COUNT(*) as frequencia
                FROM produtos_por_venda
                GROUP BY produto1_nome, produto2_nome
                HAVING COUNT(*) > 3
                ORDER BY frequencia DESC
            ''')
            data = cursor.fetchall()
            logger.info(f"Fetched {len(data)} frequency itemsets")
            return data
    except Exception as e:
        logger.error(f"Error fetching frequency itemsets: {e}")
        raise