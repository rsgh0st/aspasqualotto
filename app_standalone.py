import streamlit as st
import pandas as pd
from datetime import datetime
import uuid
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração da página
st.set_page_config(
    page_title="Pasqualotto Controle de Estoque",
    page_icon="📦",
    layout="wide"
)

# Tentar importar e inicializar banco PostgreSQL
try:
    from database_standalone import *
    init_database()
    usando_banco = True
    st.success("Conectado ao banco PostgreSQL - Dados sincronizados entre filiais!")
except Exception as e:
    st.warning(f"Usando modo local temporário. Configure DATABASE_URL no arquivo .env")
    usando_banco = False

# Inicializar dados locais se necessário
if not usando_banco:
    if 'filiais' not in st.session_state:
        st.session_state.filiais = pd.DataFrame({
            'id': [1, 2, 3],
            'nome': ['Lucas do Rio Verde', 'Brasnorte', 'Juara']
        })
    
    if 'produtos' not in st.session_state:
        st.session_state.produtos = pd.DataFrame(columns=['id', 'codigo', 'nome', 'valor', 'filial_id', 'data_cadastro'])
    
    if 'movimentacoes' not in st.session_state:
        st.session_state.movimentacoes = pd.DataFrame(columns=['id', 'produto_id', 'tipo', 'quantidade', 'setor', 'observacao', 'filial_id', 'data_movimentacao'])

# Funções adaptadoras que usam banco ou dados locais
def get_filiais_adaptado():
    if usando_banco:
        return get_filiais()
    else:
        return st.session_state.filiais

def adicionar_produto_adaptado(codigo, nome, valor, filial_id):
    if usando_banco:
        adicionar_produto(codigo, nome, valor, filial_id)
    else:
        novo_produto = pd.DataFrame({
            'id': [str(uuid.uuid4())],
            'codigo': [codigo],
            'nome': [nome],
            'valor': [valor],
            'filial_id': [filial_id],
            'data_cadastro': [datetime.now()]
        })
        
        if st.session_state.produtos.empty:
            st.session_state.produtos = novo_produto
        else:
            st.session_state.produtos = pd.concat([st.session_state.produtos, novo_produto], ignore_index=True)

def get_produtos_adaptado(filial_id):
    if usando_banco:
        return get_produtos(filial_id)
    else:
        produtos_df = st.session_state.produtos
        if produtos_df.empty:
            return pd.DataFrame()
        return produtos_df[produtos_df['filial_id'] == filial_id]

def produto_existe_adaptado(codigo, filial_id):
    if usando_banco:
        return produto_existe(codigo, filial_id)
    else:
        produtos = st.session_state.produtos
        if produtos.empty:
            return False
        return not produtos[(produtos['codigo'] == codigo) & (produtos['filial_id'] == filial_id)].empty

def get_produto_por_codigo_adaptado(codigo, filial_id):
    if usando_banco:
        return get_produto_por_codigo(codigo, filial_id)
    else:
        produtos = st.session_state.produtos
        if produtos.empty:
            return None
        produto = produtos[(produtos['codigo'] == codigo) & (produtos['filial_id'] == filial_id)]
        return produto.iloc[0].to_dict() if not produto.empty else None

def registrar_movimentacao_adaptado(produto_id, tipo, quantidade, setor, observacao, filial_id, data_movimentacao=None):
    if usando_banco:
        registrar_movimentacao(produto_id, tipo, quantidade, setor, observacao, filial_id, data_movimentacao)
    else:
        if data_movimentacao is None:
            data_movimentacao = datetime.now()
        
        nova_movimentacao = pd.DataFrame({
            'id': [str(uuid.uuid4())],
            'produto_id': [produto_id],
            'tipo': [tipo],
            'quantidade': [quantidade],
            'setor': [setor],
            'observacao': [observacao],
            'filial_id': [filial_id],
            'data_movimentacao': [data_movimentacao]
        })
        
        if st.session_state.movimentacoes.empty:
            st.session_state.movimentacoes = nova_movimentacao
        else:
            st.session_state.movimentacoes = pd.concat([st.session_state.movimentacoes, nova_movimentacao], ignore_index=True)

def get_estoque_atual_adaptado(filial_id=None):
    if usando_banco:
        return get_estoque_atual(filial_id)
    else:
        produtos = st.session_state.produtos
        movimentacoes = st.session_state.movimentacoes
        
        if produtos.empty:
            return pd.DataFrame()
        
        if filial_id:
            produtos = produtos[produtos['filial_id'] == filial_id]
        
        estoque_list = []
        for _, produto in produtos.iterrows():
            mov_produto = movimentacoes[movimentacoes['produto_id'] == produto['id']] if not movimentacoes.empty else pd.DataFrame()
            
            quantidade_atual = 0
            if not mov_produto.empty:
                entradas = mov_produto[mov_produto['tipo'] == 'Entrada']['quantidade'].sum()
                saidas = mov_produto[mov_produto['tipo'] == 'Saída']['quantidade'].sum()
                quantidade_atual = entradas - saidas
            
            estoque_list.append({
                'produto_id': produto['id'],
                'codigo': produto['codigo'],
                'nome': produto['nome'],
                'valor': produto['valor'],
                'quantidade_atual': quantidade_atual,
                'filial_id': produto['filial_id']
            })
        
        return pd.DataFrame(estoque_list)

def get_movimentacoes_adaptado(filial_id):
    if usando_banco:
        return get_movimentacoes(filial_id)
    else:
        movimentacoes_df = st.session_state.movimentacoes
        if movimentacoes_df.empty:
            return pd.DataFrame()
        
        mov_filial = movimentacoes_df[movimentacoes_df['filial_id'] == filial_id]
        if mov_filial.empty:
            return pd.DataFrame()
        
        # Juntar com produtos para obter nome
        produtos = st.session_state.produtos
        resultado = mov_filial.merge(produtos[['id', 'codigo', 'nome']], left_on='produto_id', right_on='id', suffixes=('', '_produto'))
        resultado = resultado.rename(columns={'nome': 'produto_nome'})
        return resultado

def remover_produtos_adaptado(produto_ids):
    if usando_banco:
        remover_produtos(produto_ids)
    else:
        # Remover movimentações relacionadas
        st.session_state.movimentacoes = st.session_state.movimentacoes[
            ~st.session_state.movimentacoes['produto_id'].isin(produto_ids)
        ]
        # Remover produtos
        st.session_state.produtos = st.session_state.produtos[
            ~st.session_state.produtos['id'].isin(produto_ids)
        ]

def remover_movimentacoes_adaptado(movimentacao_ids):
    if usando_banco:
        remover_movimentacoes(movimentacao_ids)
    else:
        st.session_state.movimentacoes = st.session_state.movimentacoes[
            ~st.session_state.movimentacoes['id'].isin(movimentacao_ids)
        ]

# Interface principal
st.title("📦 Pasqualotto Controle de Estoque Multi-Filial")
st.markdown("---")

# Seleção de filial
filiais_df = get_filiais_adaptado()
if filiais_df.empty:
    st.error("Nenhuma filial encontrada!")
    st.stop()

col_filial, col_info = st.columns([2, 3])

with col_filial:
    filial_selecionada = st.selectbox(
        "Selecione a Filial:", 
        options=filiais_df['id'].tolist(),
        format_func=lambda x: filiais_df[filiais_df['id'] == x]['nome'].iloc[0],
        key="filial_atual"
    )

with col_info:
    if filial_selecionada:
        nome_filial = filiais_df[filiais_df['id'] == filial_selecionada]['nome'].iloc[0]
        status_conexao = "🌐 ONLINE - PostgreSQL" if usando_banco else "💾 LOCAL"
        st.info(f"🏢 Filial: **{nome_filial}** | {status_conexao}")

# Navegação por abas
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "➕ Adicionar Produtos", 
    "📋 Histórico de Produtos", 
    "🔄 Entrada/Saída", 
    "📊 Estoque Atual",
    "🌐 Visão Geral"
])

# Aba 1: Adicionar Produtos
with tab1:
    st.header("Adicionar Novo Produto")
    
    col1, col2 = st.columns(2)
    
    with col1:
        codigo_produto = st.text_input("Código do Produto*", placeholder="Ex: PROD001")
        nome_produto = st.text_input("Nome do Produto*", placeholder="Ex: Produto Exemplo")
    
    with col2:
        valor_produto = st.number_input("Valor (R$)*", min_value=0.01, step=0.01, format="%.2f")
    
    if st.button("💾 Adicionar Produto", type="primary"):
        if not codigo_produto.strip():
            st.error("❌ Código do produto é obrigatório!")
        elif not nome_produto.strip():
            st.error("❌ Nome do produto é obrigatório!")
        elif produto_existe_adaptado(codigo_produto, filial_selecionada):
            st.error("❌ Já existe um produto com este código nesta filial!")
        else:
            try:
                adicionar_produto_adaptado(codigo_produto, nome_produto, valor_produto, filial_selecionada)
                st.success(f"✅ Produto '{nome_produto}' adicionado com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Erro ao adicionar produto: {e}")

# Aba 2: Histórico de Produtos
with tab2:
    st.header("Histórico de Produtos Cadastrados")
    
    try:
        produtos_df = get_produtos_adaptado(filial_selecionada)
        
        if not produtos_df.empty:
            # Filtros
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                filtro_codigo = st.text_input("Filtrar por código:", placeholder="Digite o código para filtrar")
            with col2:
                filtro_nome = st.text_input("Filtrar por nome:", placeholder="Digite o nome para filtrar")
            with col3:
                modo_selecao_produtos = st.checkbox("Modo seleção", help="Ative para selecionar e remover produtos")
            
            # Aplicar filtros
            df_filtrado = produtos_df.copy()
            
            if filtro_codigo:
                df_filtrado = df_filtrado[df_filtrado['codigo'].str.contains(filtro_codigo, case=False, na=False)]
            
            if filtro_nome:
                df_filtrado = df_filtrado[df_filtrado['nome'].str.contains(filtro_nome, case=False, na=False)]
            
            if not df_filtrado.empty:
                if modo_selecao_produtos:
                    st.warning("⚠️ Modo seleção ativo. Marque os produtos que deseja remover.")
                    
                    produtos_selecionados = []
                    
                    for idx, row in df_filtrado.iterrows():
                        col_check, col_info = st.columns([0.1, 0.9])
                        
                        with col_check:
                            selecionado = st.checkbox("Selecionar", key=f"prod_{idx}", label_visibility="collapsed")
                            if selecionado:
                                produtos_selecionados.append(str(row['id']))
                        
                        with col_info:
                            data_formatada = row['data_cadastro'].strftime("%d/%m/%Y %H:%M")
                            st.write(f"**{row['codigo']}** - {row['nome']} | R$ {row['valor']:.2f} | Cadastrado em: {data_formatada}")
                    
                    if produtos_selecionados:
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button("🗑️ Remover Produtos Selecionados", type="primary"):
                                try:
                                    remover_produtos_adaptado(produtos_selecionados)
                                    st.success(f"✅ {len(produtos_selecionados)} produto(s) removido(s)!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Erro ao remover produtos: {e}")
                        
                        with col_btn2:
                            st.write(f"**{len(produtos_selecionados)}** produto(s) selecionado(s)")
                else:
                    # Exibição normal
                    df_exibir = df_filtrado[['codigo', 'nome', 'valor', 'data_cadastro']].copy()
                    df_exibir['valor'] = df_exibir['valor'].apply(lambda x: f"R$ {x:.2f}")
                    df_exibir['data_cadastro'] = df_exibir['data_cadastro'].dt.strftime("%d/%m/%Y %H:%M")
                    df_exibir.columns = ['Código', 'Nome', 'Valor', 'Data de Cadastro']
                    
                    st.dataframe(df_exibir, use_container_width=True)
                    st.info(f"📊 Total de produtos: {len(df_filtrado)}")
            else:
                st.warning("⚠️ Nenhum produto encontrado com os filtros aplicados.")
        else:
            st.info("📝 Nenhum produto cadastrado ainda nesta filial.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar produtos: {e}")

# Aba 3: Entrada/Saída
with tab3:
    st.header("Movimentação de Estoque")
    
    try:
        produtos_df = get_produtos_adaptado(filial_selecionada)
        
        if not produtos_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                opcoes_produtos = [f"{row['codigo']} - {row['nome']}" for _, row in produtos_df.iterrows()]
                produto_selecionado = st.selectbox("Selecione o Produto*", opcoes_produtos)
                
                tipo_movimentacao = st.selectbox("Tipo de Movimentação*", ["Entrada", "Saída"])
                quantidade = st.number_input("Quantidade*", min_value=1, step=1)
                
            with col2:
                usar_data_atual = st.checkbox("Usar data e hora atual", value=True)
                
                data_selecionada = datetime.now().date()
                hora_selecionada = datetime.now().time()
                
                if not usar_data_atual:
                    col_data, col_hora = st.columns(2)
                    with col_data:
                        data_selecionada = st.date_input("Data da Movimentação", value=datetime.now().date())
                    with col_hora:
                        hora_selecionada = st.time_input("Hora da Movimentação", value=datetime.now().time())
                
                setor = st.text_input("Setor de Destino*", placeholder="Ex: Almoxarifado, Produção, Vendas")
                observacao = st.text_area("Observação (opcional)", placeholder="Adicione uma observação")
            
            if st.button("📝 Registrar Movimentação", type="primary"):
                if produto_selecionado and quantidade > 0 and setor.strip():
                    try:
                        codigo_selecionado = produto_selecionado.split(" - ")[0]
                        produto = get_produto_por_codigo_adaptado(codigo_selecionado, filial_selecionada)
                        
                        if produto:
                            if usar_data_atual:
                                data_movimentacao = datetime.now()
                            else:
                                data_movimentacao = datetime.combine(data_selecionada, hora_selecionada)
                            
                            # Verificar estoque para saída
                            if tipo_movimentacao == "Saída":
                                estoque_df = get_estoque_atual_adaptado(filial_selecionada)
                                produto_estoque = estoque_df[estoque_df['produto_id'] == produto['id']]
                                
                                if not produto_estoque.empty:
                                    estoque_atual = produto_estoque['quantidade_atual'].iloc[0]
                                    if quantidade > estoque_atual:
                                        st.error(f"❌ Estoque insuficiente! Disponível: {estoque_atual} unidades")
                                        st.stop()
                            
                            registrar_movimentacao_adaptado(
                                produto['id'], tipo_movimentacao, quantidade, 
                                setor, observacao, filial_selecionada, data_movimentacao
                            )
                            st.success(f"✅ {tipo_movimentacao} de {quantidade} unidades registrada para {setor}!")
                            st.rerun()
                        else:
                            st.error("❌ Produto não encontrado!")
                    except Exception as e:
                        st.error(f"❌ Erro ao registrar movimentação: {e}")
                else:
                    st.error("❌ Preencha todos os campos obrigatórios!")
            
            # Histórico de movimentações
            st.markdown("---")
            st.subheader("📈 Histórico de Movimentações")
            
            try:
                movimentacoes_df = get_movimentacoes_adaptado(filial_selecionada)
                
                if not movimentacoes_df.empty:
                    # Filtros
                    col_filtro, col_acoes = st.columns([3, 1])
                    with col_filtro:
                        filtro_historico = st.selectbox("Filtrar por tipo:", ["Todas", "Entrada", "Saída"])
                    with col_acoes:
                        modo_selecao = st.checkbox("Modo seleção", help="Para remover movimentações")
                    
                    # Aplicar filtro
                    if filtro_historico != "Todas":
                        df_mov_filtrado = movimentacoes_df[movimentacoes_df['tipo'] == filtro_historico]
                    else:
                        df_mov_filtrado = movimentacoes_df
                    
                    if not df_mov_filtrado.empty:
                        if modo_selecao:
                            st.warning("⚠️ Modo seleção ativo. Marque movimentações para remover.")
                            
                            itens_selecionados = []
                            
                            for idx, row in df_mov_filtrado.iterrows():
                                col_check, col_info = st.columns([0.1, 0.9])
                                
                                with col_check:
                                    selecionado = st.checkbox("Selecionar", key=f"mov_{idx}", label_visibility="collapsed")
                                    if selecionado:
                                        itens_selecionados.append(str(row['id']))
                                
                                with col_info:
                                    data_formatada = row['data_movimentacao'].strftime("%d/%m/%Y %H:%M")
                                    st.write(f"**{row['codigo']}** - {row['produto_nome']} | {row['tipo']}: {row['quantidade']} un. | Setor: {row['setor']} | {data_formatada}")
                                    if row['observacao']:
                                        st.caption(f"Obs: {row['observacao']}")
                            
                            if itens_selecionados:
                                col_btn1, col_btn2 = st.columns(2)
                                with col_btn1:
                                    if st.button("🗑️ Remover Selecionados", type="primary"):
                                        try:
                                            remover_movimentacoes_adaptado(itens_selecionados)
                                            st.success(f"✅ {len(itens_selecionados)} movimentação(ões) removida(s)!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"❌ Erro ao remover movimentações: {e}")
                                
                                with col_btn2:
                                    st.write(f"**{len(itens_selecionados)}** item(ns) selecionado(s)")
                        else:
                            # Exibição normal
                            df_mov_exibir = df_mov_filtrado[['codigo', 'produto_nome', 'tipo', 'quantidade', 'setor', 'data_movimentacao', 'observacao']]
                            df_mov_exibir = df_mov_exibir.copy()
                            df_mov_exibir['data_movimentacao'] = df_mov_exibir['data_movimentacao'].dt.strftime("%d/%m/%Y %H:%M")
                            df_mov_exibir.columns = ['Código', 'Produto', 'Tipo', 'Quantidade', 'Setor', 'Data', 'Observação']
                            
                            st.dataframe(df_mov_exibir, use_container_width=True)
                            st.info(f"📊 Total de movimentações: {len(df_mov_filtrado)}")
                    else:
                        st.info("📝 Nenhuma movimentação encontrada com o filtro aplicado.")
                else:
                    st.info("📝 Nenhuma movimentação registrada nesta filial.")
            except Exception as e:
                st.error(f"❌ Erro ao carregar movimentações: {e}")
        else:
            st.info("📝 Cadastre produtos primeiro para registrar movimentações.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {e}")

# Aba 4: Estoque Atual
with tab4:
    st.header("Estoque Atual")
    
    try:
        estoque_df = get_estoque_atual_adaptado(filial_selecionada)
        
        if not estoque_df.empty:
            # Estatísticas
            col1, col2, col3 = st.columns(3)
            
            total_produtos = len(estoque_df)
            produtos_com_estoque = len(estoque_df[estoque_df['quantidade_atual'] > 0])
            produtos_sem_estoque = total_produtos - produtos_com_estoque
            
            with col1:
                st.metric("📦 Total de Produtos", total_produtos)
            with col2:
                st.metric("✅ Com Estoque", produtos_com_estoque)
            with col3:
                st.metric("⚠️ Sem Estoque", produtos_sem_estoque)
            
            st.markdown("---")
            
            # Filtros
            col1, col2 = st.columns(2)
            with col1:
                filtro_estoque = st.selectbox("Filtrar por:", ["Todos", "Com estoque", "Sem estoque"])
            with col2:
                filtro_busca = st.text_input("Buscar produto:", placeholder="Digite código ou nome")
            
            # Aplicar filtros
            df_estoque_filtrado = estoque_df.copy()
            
            if filtro_estoque == "Com estoque":
                df_estoque_filtrado = df_estoque_filtrado[df_estoque_filtrado['quantidade_atual'] > 0]
            elif filtro_estoque == "Sem estoque":
                df_estoque_filtrado = df_estoque_filtrado[df_estoque_filtrado['quantidade_atual'] == 0]
            
            if filtro_busca:
                df_estoque_filtrado = df_estoque_filtrado[
                    (df_estoque_filtrado['codigo'].str.contains(filtro_busca, case=False, na=False)) |
                    (df_estoque_filtrado['nome'].str.contains(filtro_busca, case=False, na=False))
                ]
            
            if not df_estoque_filtrado.empty:
                # Calcular valor total
                df_estoque_filtrado['valor_total'] = df_estoque_filtrado['quantidade_atual'] * df_estoque_filtrado['valor']
                
                # Preparar para exibição
                df_exibir = df_estoque_filtrado[['codigo', 'nome', 'quantidade_atual', 'valor', 'valor_total']].copy()
                df_exibir['valor'] = df_exibir['valor'].apply(lambda x: f"R$ {x:.2f}")
                df_exibir['valor_total'] = df_exibir['valor_total'].apply(lambda x: f"R$ {x:.2f}")
                df_exibir.columns = ['Código', 'Produto', 'Quantidade', 'Valor Unitário', 'Valor Total']
                
                st.dataframe(df_exibir, use_container_width=True)
                
                valor_total_geral = df_estoque_filtrado['valor_total'].sum()
                st.success(f"💰 Valor total do estoque: R$ {valor_total_geral:.2f}")
            else:
                st.warning("⚠️ Nenhum produto encontrado com os filtros aplicados.")
        else:
            st.info("📝 Nenhum produto no estoque. Cadastre produtos primeiro.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar estoque: {e}")

# Aba 5: Visão Geral
with tab5:
    st.header("Visão Geral - Todas as Filiais")
    
    try:
        estoque_geral = get_estoque_atual_adaptado()
        
        if not estoque_geral.empty:
            # Adicionar nome da filial
            filiais_df = get_filiais_adaptado()
            estoque_com_filial = estoque_geral.merge(filiais_df, left_on='filial_id', right_on='id', suffixes=('', '_filial'))
            estoque_com_filial = estoque_com_filial.rename(columns={'nome': 'filial_nome'})
            
            st.subheader("📊 Resumo por Filial")
            
            # Estatísticas por filial
            resumo_filiais = estoque_com_filial.groupby('filial_nome').agg({
                'produto_id': 'count',
                'quantidade_atual': 'sum',
                'valor': 'mean'
            }).round(2)
            
            resumo_filiais['valor_total_estoque'] = estoque_com_filial.groupby('filial_nome').apply(
                lambda x: (x['quantidade_atual'] * x['valor']).sum()
            ).round(2)
            
            resumo_filiais.columns = ['Produtos', 'Qtd Total', 'Valor Médio', 'Valor Total Estoque']
            resumo_filiais['Valor Médio'] = resumo_filiais['Valor Médio'].apply(lambda x: f"R$ {x:.2f}")
            resumo_filiais['Valor Total Estoque'] = resumo_filiais['Valor Total Estoque'].apply(lambda x: f"R$ {x:.2f}")
            
            st.dataframe(resumo_filiais, use_container_width=True)
            
            st.markdown("---")
            st.subheader("📋 Estoque Detalhado - Todas as Filiais")
            
            # Filtro por filial
            filial_filtro = st.selectbox(
                "Filtrar por filial:", 
                ["Todas"] + filiais_df['nome'].tolist()
            )
            
            if filial_filtro != "Todas":
                estoque_exibir = estoque_com_filial[estoque_com_filial['filial_nome'] == filial_filtro]
            else:
                estoque_exibir = estoque_com_filial
            
            if not estoque_exibir.empty:
                df_geral = estoque_exibir[['filial_nome', 'codigo', 'nome', 'quantidade_atual', 'valor']].copy()
                df_geral['valor_total'] = df_geral['quantidade_atual'] * df_geral['valor']
                df_geral['valor'] = df_geral['valor'].apply(lambda x: f"R$ {x:.2f}")
                df_geral['valor_total'] = df_geral['valor_total'].apply(lambda x: f"R$ {x:.2f}")
                df_geral.columns = ['Filial', 'Código', 'Produto', 'Quantidade', 'Valor Unit.', 'Valor Total']
                
                st.dataframe(df_geral, use_container_width=True)
                
                valor_total_geral = estoque_exibir['quantidade_atual'] * estoque_exibir['valor']
                st.success(f"💰 Valor total do estoque (filtrado): R$ {valor_total_geral.sum():.2f}")
            else:
                st.info("📝 Nenhum produto encontrado.")
        else:
            st.info("📝 Nenhum produto cadastrado ainda.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar visão geral: {e}")

# Rodapé
st.markdown("---")
st.markdown("**Pasqualotto Controle de Estoque Multi-Filial** - Sistema integrado de gestão")

# Instruções para uso independente
with st.expander("🚀 Como usar este sistema independentemente"):
    st.markdown("""
    **Para executar este sistema em seu próprio servidor:**
    
    1. **Baixe os arquivos:**
       - app_standalone.py
       - database_standalone.py
       - .env.example (renomear para .env)
       - requirements_standalone.txt
    
    2. **Instale as dependências:**
       ```bash
       pip install -r requirements_standalone.txt
       ```
    
    3. **Configure o banco de dados (opcional):**
       - Edite o arquivo .env com sua URL do PostgreSQL
       - Ou use o modo local (dados salvos no navegador)
    
    4. **Execute o sistema:**
       ```bash
       streamlit run app_standalone.py --server.port 8501
       ```
    
    5. **Acesse:** http://localhost:8501
    
    **Para usar no celular:** Adicione à tela inicial como PWA
    """)

# Status da conexão
with st.expander("🔗 Status da Conexão"):
    if usando_banco:
        st.success("✅ Conectado ao PostgreSQL")
        st.info("Dados sincronizados em tempo real entre todas as filiais")
        st.info("Base de dados compartilhada na nuvem")
    else:
        st.warning("⚠️ Modo local ativo")
        st.info("Dados armazenados localmente no navegador")
        st.info("Configure DATABASE_URL no arquivo .env para sincronização entre filiais")