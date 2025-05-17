from postgree_connection import get_new_postgres_connection
from faker import Faker
import random
from datetime import date

fake = Faker()

def calculate_age(birthdate: date) -> int:
    today = date.today()
    return today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))

def populate_men_clients(cur, num_clients=100):
    for _ in range(num_clients):
        birthdate = fake.date_of_birth(minimum_age=18, maximum_age=80)
        age = calculate_age(birthdate)
        cur.execute(
            "INSERT INTO clientes (nome, email, gender, age, data_nascimento, data_cadastro) " \
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                fake.name(),
                fake.email(),
                'M',
                age,
                birthdate,
                fake.date_between(start_date='-2y', end_date='today')
            )
        )

def populate_women_clients(cur, num_clients=100):
    for _ in range(num_clients):
        birthdate = fake.date_of_birth(minimum_age=18, maximum_age=80)
        age = calculate_age(birthdate)
        cur.execute(
            "INSERT INTO clientes (nome, email, gender, age, data_nascimento, data_cadastro) " \
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                fake.name(),
                fake.email(),
                'F',
                age,
                birthdate,
                fake.date_between(start_date='-2y', end_date='today')
            )
        )

def populate_categorias(cur):
    categorias = [
        'Eletrônicos', 'Livros', 'Roupas', 'Alimentos', 'Móveis', 'Brinquedos', 'Esportes',
        'Beleza', 'Saúde', 'Automotivo', 'Jardinagem', 'Ferramentas', 'Pet Shop', 'Informática',
        'Celulares'
    ]
    for categoria in categorias:
        cur.execute(
            "INSERT INTO categorias (nome) VALUES (%s) ON CONFLICT DO NOTHING",
            (categoria,)
        )

def populate_produtos(cur):
    produtos_por_categoria = {
        'Eletrônicos': ['Notebook', 'Smart TV', 'Fone de Ouvido', 'Tablet'],
        'Livros': ['Romance', 'Biografia', 'HQ', 'Didático'],
        'Roupas': ['Camiseta', 'Calça', 'Vestido', 'Jaqueta'],
        'Alimentos': ['Arroz', 'Feijão', 'Chocolate', 'Café'],
        'Móveis': ['Sofá', 'Mesa', 'Cadeira', 'Estante'],
        'Brinquedos': ['Quebra-cabeça', 'Boneca', 'Carrinho', 'Jogo de Tabuleiro'],
        'Esportes': ['Bola', 'Tênis', 'Bicicleta', 'Raquete'],
        'Beleza': ['Perfume', 'Batom', 'Shampoo', 'Creme'],
        'Saúde': ['Vitamínico', 'Termômetro', 'Máscara', 'Curativo'],
        'Automotivo': ['Pneu', 'Óleo', 'Lâmpada', 'Bateria'],
        'Jardinagem': ['Vaso', 'Pá', 'Semente', 'Adubo'],
        'Ferramentas': ['Martelo', 'Chave de Fenda', 'Alicate', 'Serrote'],
        'Pet Shop': ['Ração', 'Coleira', 'Brinquedo Pet', 'Caminha'],
        'Informática': ['Mouse', 'Teclado', 'Monitor', 'HD Externo'],
        'Celulares': ['Smartphone', 'Carregador', 'Capa', 'Fone Bluetooth']
    }

    cur.execute("SELECT id, nome FROM categorias")
    categorias = {nome: id_categoria for id_categoria, nome in cur.fetchall()}

    for categoria, produtos in produtos_por_categoria.items():
        id_categoria = categorias[categoria]
        for nome_produto in produtos:
            preco = round(random.uniform(10, 2000), 2)
            cur.execute(
                "INSERT INTO produtos (nome, preco, id_categoria) VALUES (%s, %s, %s)",
                (nome_produto, preco, id_categoria)
            )

def populate_vendas_e_itens(cur, num_vendas=200, max_itens=8):
    cur.execute("SELECT id FROM clientes")
    clientes_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT id, preco FROM produtos")
    produtos = cur.fetchall()

    for _ in range(num_vendas):
        id_cliente = random.choice(clientes_ids)
        data_venda = fake.date_between(start_date='-1y', end_date='today')
        num_itens = random.randint(1, max_itens)
        itens = random.sample(produtos, k=num_itens)
        valor_total = 0
        cur.execute(
            "INSERT INTO vendas (id_cliente, data_venda, valor_total) VALUES (%s, %s, %s) RETURNING id",
            (id_cliente, data_venda, 0)
        )
        id_venda = cur.fetchone()[0]

        for prod in itens:
            id_produto, preco = prod
            quantidade = random.randint(1, 3)
            preco_unitario = preco
            valor_total += preco_unitario * quantidade
            cur.execute(
                "INSERT INTO itens_venda (id_venda, id_produto, quantidade, preco_unitario) " \
                "VALUES (%s, %s, %s, %s)",
                (id_venda, id_produto, quantidade, preco_unitario)
            )

        cur.execute(
            "UPDATE vendas SET valor_total = %s WHERE id = %s",
            (valor_total, id_venda)
        )

    print("Vendas e itens_venda populados com sucesso!")

if __name__ == "__main__":
    conn = get_new_postgres_connection()
    cur = conn.cursor()
    populate_men_clients(cur, num_clients=50)
    populate_women_clients(cur, num_clients=50)
    populate_categorias(cur)
    populate_produtos(cur)
    populate_vendas_e_itens(cur, num_vendas=100, max_itens=8)
    cur.close()
    conn.close()
    print("Successfully populated the database!")