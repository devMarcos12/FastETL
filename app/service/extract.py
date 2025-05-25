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

def extract_orders_with_customers_and_items() -> list:
    """
    Fetch all complete orders (with customer and items).
    """
    try:
        with dict_cursor() as cursor:
            cursor.execute('''
                SELECT
                    v.id,
                    v.data_venda,
                    v.valor_total,
                    c.id as cliente_id,
                    c.nome,
                    c.email,
                    c.gender,
                    c.age
                FROM vendas v
                JOIN clientes c ON v.id_cliente = c.id
                ORDER BY v.data_venda DESC  -- Ordenação para análise temporal
            ''')
            orders = cursor.fetchall()

            order_ids = [order['id'] for order in orders]
            if not order_ids:
                return []

            placeholders = ','.join(['%s'] * len(order_ids))
            cursor.execute(f'''
                SELECT
                    iv.id_venda,
                    p.id,
                    p.nome,
                    iv.quantidade,
                    iv.preco_unitario,
                    cat.nome as categoria
                FROM itens_venda iv
                JOIN produtos p ON iv.id_produto = p.id
                LEFT JOIN categorias cat ON p.id_categoria = cat.id
                WHERE iv.id_venda IN ({placeholders})
            ''', order_ids)

            items_by_order = {}
            for item in cursor.fetchall():
                order_id = item.pop('id_venda')
                if order_id not in items_by_order:
                    items_by_order[order_id] = []
                items_by_order[order_id].append(item)

            for order in orders:
                order['itens'] = items_by_order.get(order['id'], [])

            logger.info(f"Fetched {len(orders)} orders with {sum(len(o['itens']) for o in orders)} items")
            return orders
    except Exception as e:
        logger.error(f"Error fetching all orders: {e}")
        raise