import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import uuid

# Configuração do banco de dados
DATABASE_URL = os.getenv('DATABASE_URL')

def get_engine():
    """Cria conexão com o banco de dados"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL não configurada")
    
    # Verificar se a URL está no formato correto
    if not DATABASE_URL.startswith(('postgresql://', 'postgres://')):
        raise ValueError("DATABASE_URL deve começar com postgresql:// ou postgres://")
    
    try:
        return create_engine(DATABASE_URL)
    except Exception as e:
        raise ValueError(f"Erro ao conectar com o banco: {e}")

def init_database():
    """Inicializa as tabelas do banco de dados"""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Criar tabela de filiais
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS filiais (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100) NOT NULL UNIQUE
            )
        """))
        
        # Inserir filiais padrão se não existirem
        conn.execute(text("""
            INSERT INTO filiais (id, nome) VALUES 
            (1, 'Lucas do Rio Verde'),
            (2, 'Brasnorte'),
            (3, 'Juara')
            ON CONFLICT (nome) DO NOTHING
        """))
        
        # Criar tabela de produtos
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS produtos (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                codigo VARCHAR(50) NOT NULL,
                nome VARCHAR(200) NOT NULL,
                valor DECIMAL(10,2) NOT NULL,
                filial_id INTEGER REFERENCES filiais(id),
                data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(codigo, filial_id)
            )
        """))
        
        # Criar tabela de movimentações
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS movimentacoes (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                produto_id UUID REFERENCES produtos(id) ON DELETE CASCADE,
                tipo VARCHAR(10) NOT NULL CHECK (tipo IN ('Entrada', 'Saída')),
                quantidade INTEGER NOT NULL CHECK (quantidade > 0),
                setor VARCHAR(100) NOT NULL,
                observacao TEXT,
                filial_id INTEGER REFERENCES filiais(id),
                data_movimentacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.commit()

def get_filiais():
    """Retorna lista de filiais"""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, nome FROM filiais ORDER BY id"))
        return pd.DataFrame(result.fetchall(), columns=['id', 'nome'])

def get_produtos(filial_id=None):
    """Retorna produtos, opcionalmente filtrados por filial"""
    engine = get_engine()
    with engine.connect() as conn:
        if filial_id:
            result = conn.execute(text("""
                SELECT id, codigo, nome, valor, filial_id, data_cadastro 
                FROM produtos 
                WHERE filial_id = :filial_id 
                ORDER BY data_cadastro DESC
            """), {"filial_id": filial_id})
        else:
            result = conn.execute(text("""
                SELECT id, codigo, nome, valor, filial_id, data_cadastro 
                FROM produtos 
                ORDER BY data_cadastro DESC
            """))
        
        df = pd.DataFrame(result.fetchall(), columns=['id', 'codigo', 'nome', 'valor', 'filial_id', 'data_cadastro'])
        if not df.empty:
            df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
        return df

def get_movimentacoes(filial_id=None):
    """Retorna movimentações, opcionalmente filtradas por filial"""
    engine = get_engine()
    with engine.connect() as conn:
        if filial_id:
            result = conn.execute(text("""
                SELECT m.id, m.produto_id, m.tipo, m.quantidade, m.setor, 
                       m.observacao, m.filial_id, m.data_movimentacao,
                       p.codigo, p.nome as produto_nome
                FROM movimentacoes m
                JOIN produtos p ON m.produto_id = p.id
                WHERE m.filial_id = :filial_id
                ORDER BY m.data_movimentacao DESC
            """), {"filial_id": filial_id})
        else:
            result = conn.execute(text("""
                SELECT m.id, m.produto_id, m.tipo, m.quantidade, m.setor, 
                       m.observacao, m.filial_id, m.data_movimentacao,
                       p.codigo, p.nome as produto_nome
                FROM movimentacoes m
                JOIN produtos p ON m.produto_id = p.id
                ORDER BY m.data_movimentacao DESC
            """))
        
        df = pd.DataFrame(result.fetchall(), columns=[
            'id', 'produto_id', 'tipo', 'quantidade', 'setor', 
            'observacao', 'filial_id', 'data_movimentacao', 'codigo', 'produto_nome'
        ])
        if not df.empty:
            df['data_movimentacao'] = pd.to_datetime(df['data_movimentacao'])
        return df

def get_estoque_atual(filial_id=None):
    """Retorna estoque atual, opcionalmente filtrado por filial"""
    engine = get_engine()
    with engine.connect() as conn:
        if filial_id:
            result = conn.execute(text("""
                SELECT 
                    p.id as produto_id,
                    p.codigo,
                    p.nome,
                    p.valor,
                    p.filial_id,
                    COALESCE(
                        (SELECT SUM(CASE WHEN tipo = 'Entrada' THEN quantidade ELSE -quantidade END)
                         FROM movimentacoes 
                         WHERE produto_id = p.id), 0
                    ) as quantidade_atual
                FROM produtos p
                WHERE p.filial_id = :filial_id
                ORDER BY p.nome
            """), {"filial_id": filial_id})
        else:
            result = conn.execute(text("""
                SELECT 
                    p.id as produto_id,
                    p.codigo,
                    p.nome,
                    p.valor,
                    p.filial_id,
                    COALESCE(
                        (SELECT SUM(CASE WHEN tipo = 'Entrada' THEN quantidade ELSE -quantidade END)
                         FROM movimentacoes 
                         WHERE produto_id = p.id), 0
                    ) as quantidade_atual
                FROM produtos p
                ORDER BY p.nome
            """))
        
        return pd.DataFrame(result.fetchall(), columns=[
            'produto_id', 'codigo', 'nome', 'valor', 'filial_id', 'quantidade_atual'
        ])

def adicionar_produto(codigo, nome, valor, filial_id):
    """Adiciona um novo produto"""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO produtos (codigo, nome, valor, filial_id)
            VALUES (:codigo, :nome, :valor, :filial_id)
        """), {
            "codigo": codigo,
            "nome": nome,
            "valor": valor,
            "filial_id": filial_id
        })
        conn.commit()

def registrar_movimentacao(produto_id, tipo, quantidade, setor, observacao, filial_id, data_movimentacao=None):
    """Registra uma movimentação"""
    engine = get_engine()
    with engine.connect() as conn:
        if data_movimentacao:
            conn.execute(text("""
                INSERT INTO movimentacoes (produto_id, tipo, quantidade, setor, observacao, filial_id, data_movimentacao)
                VALUES (:produto_id, :tipo, :quantidade, :setor, :observacao, :filial_id, :data_movimentacao)
            """), {
                "produto_id": produto_id,
                "tipo": tipo,
                "quantidade": quantidade,
                "setor": setor,
                "observacao": observacao,
                "filial_id": filial_id,
                "data_movimentacao": data_movimentacao
            })
        else:
            conn.execute(text("""
                INSERT INTO movimentacoes (produto_id, tipo, quantidade, setor, observacao, filial_id)
                VALUES (:produto_id, :tipo, :quantidade, :setor, :observacao, :filial_id)
            """), {
                "produto_id": produto_id,
                "tipo": tipo,
                "quantidade": quantidade,
                "setor": setor,
                "observacao": observacao,
                "filial_id": filial_id
            })
        conn.commit()

def produto_existe(codigo, filial_id):
    """Verifica se um produto já existe"""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM produtos 
            WHERE codigo = :codigo AND filial_id = :filial_id
        """), {"codigo": codigo, "filial_id": filial_id})
        return result.scalar() > 0

def get_produto_por_codigo(codigo, filial_id):
    """Retorna produto pelo código"""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, codigo, nome, valor, filial_id, data_cadastro 
            FROM produtos 
            WHERE codigo = :codigo AND filial_id = :filial_id
        """), {"codigo": codigo, "filial_id": filial_id})
        
        row = result.fetchone()
        if row:
            return {
                'id': str(row[0]),
                'codigo': row[1],
                'nome': row[2],
                'valor': float(row[3]),
                'filial_id': row[4],
                'data_cadastro': row[5]
            }
        return None

def remover_produtos(produto_ids):
    """Remove produtos e suas movimentações"""
    engine = get_engine()
    with engine.connect() as conn:
        # As movimentações serão removidas automaticamente por CASCADE
        placeholders = ','.join([f':id_{i}' for i in range(len(produto_ids))])
        params = {f'id_{i}': produto_id for i, produto_id in enumerate(produto_ids)}
        
        conn.execute(text(f"""
            DELETE FROM produtos 
            WHERE id IN ({placeholders})
        """), params)
        conn.commit()

def remover_movimentacoes(movimentacao_ids):
    """Remove movimentações específicas"""
    engine = get_engine()
    with engine.connect() as conn:
        placeholders = ','.join([f':id_{i}' for i in range(len(movimentacao_ids))])
        params = {f'id_{i}': mov_id for i, mov_id in enumerate(movimentacao_ids)}
        
        conn.execute(text(f"""
            DELETE FROM movimentacoes 
            WHERE id IN ({placeholders})
        """), params)
        conn.commit()