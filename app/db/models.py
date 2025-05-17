from postgree_connection import get_new_postgres_connection

def create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            email VARCHAR(100),
            gender VARCHAR(1),
            age INTEGER,
            data_nascimento DATE,
            data_cadastro DATE
        );
        CREATE TABLE IF NOT EXISTS categorias (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(50) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS produtos (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100) NOT NULL,
            preco NUMERIC(10,2),
            id_categoria INTEGER REFERENCES categorias(id)
        );
        CREATE TABLE IF NOT EXISTS vendas (
            id SERIAL PRIMARY KEY,
            id_cliente INTEGER REFERENCES clientes(id),
            data_venda DATE,
            valor_total NUMERIC(10,2)
        );
        CREATE TABLE IF NOT EXISTS itens_venda (
            id SERIAL PRIMARY KEY,
            id_venda INTEGER REFERENCES vendas(id),
            id_produto INTEGER REFERENCES produtos(id),
            quantidade INTEGER,
            preco_unitario NUMERIC(10,2)
        );
    """)

if __name__ == "__main__":
    conn = get_new_postgres_connection()
    cur = conn.cursor()
    create_tables(cur)
    conn.commit()
    cur.close()
    conn.close()
    print("Successfully created tables!")