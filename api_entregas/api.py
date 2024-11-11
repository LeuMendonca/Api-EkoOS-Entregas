from ninja import Router
from django.db import connection
import json
import traceback

api_entregas = Router()

# ------------------------------------------- LOGIN -------------------------------------------
@api_entregas.get("login/")
def get_login( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*)
        FROM (
            SELECT 
                ek_tenant.seq_tenant , login , type_user , seq_entregador , nome_empresa
            FROM ek_tenant_user INNER JOIN ek_tenant 
                ON ek_tenant_user.seq_tenant = ek_tenant.seq_tenant
            WHERE login = %s AND password = %s
        ) as x
    """,[
        request.GET["usuario"] , request.GET["senha"]
    ])
    objetoUsuario = cursor.fetchall()

    if objetoUsuario:
        return {
            "Status": 200,
            "Usuario": objetoUsuario[0][0]
        }

# ---------------------------------------- ENTREGADORES ----------------------------------------

@api_entregas.get("entregadores/options")
def get_options_entregadores( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*)               
        FROM (
            (SELECT '' as value , 'Selecione...' as label)
                UNION ALL
            (
                SELECT
                    seq_entregador::varchar as value,
                    nome_entregador as label
                FROM 
                    ek_entregador     
                ORDER BY nome_entregador
            )
        ) as x
    """)
    objetoEntregador = cursor.fetchall()

    if objetoEntregador:
        return {
            "Status": 200,
            "Entregadores": objetoEntregador[0][0]
        }
    else:
        return []

@api_entregas.get("entregadores/")
def get_entregadores( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*)
        FROM
            (
            SELECT  
                ek_entregador.seq_entregador as sequencial,
                ek_entregador.nome_entregador    
            FROM ek_entregador 
            ORDER BY seq_entregador              
        ) as x
    """)
    objetoEntregadores = cursor.fetchall()

    if objetoEntregadores:
        return objetoEntregadores[0][0]
    else:
        return []

@api_entregas.post("entregadores/")
def post_entregadores( request ):
    cursor = connection.cursor()

    entregadores_unicode = request.body.decode('utf-8')
    entregadores = json.loads(entregadores_unicode)['body']

    try:
        cursor.execute("""
            INSERT INTO ek_entregador(
                nome_entregador, usuario, senha , email_entregador , fone_entregador , dt_cadastro
            )VALUES( %s , %s , %s, %s , %s , now()) 
            RETURNING seq_entregador
        """,[
            entregadores["dbedNome"],
            entregadores["dbedUsuario"],
            entregadores["dbedSenha"],
            entregadores["dbedEmail"],
            entregadores["dbedContato"]
        ])
        codigo_entregador = cursor.fetchall()

        return {
            "Status": 200,
            "Mensagem": f"Entregador { codigo_entregador[0][0] } cadastrado com sucesso!"
        }
    except:
        traceback.print_exc()
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao cadastrar o entregador!"
            }
        }

@api_entregas.get("entregadores/edit")
def get_entregador( request ):
    cursor = connection.cursor()

    print( request.GET["codigo_entregador"] )

    cursor.execute("""
        SELECT json_agg(x.*)
        FROM (
            SELECT
                nome_entregador as nome, usuario, senha , email_entregador as email, fone_entregador as fone
            FROM
                ek_entregador
            WHERE
                seq_entregador = %s           
        ) as x
    """,[
        request.GET["codigo_entregador"]
    ])
    objetoEntregador = cursor.fetchall()

    print(objetoEntregador)

    if objetoEntregador:
        return {
            "Status": 200,
            "Entregador": objetoEntregador[0][0]
        }
    else:
        return []
    
@api_entregas.put("entregadores/")
def put_entregadores( request ):
    cursor = connection.cursor()
    
    entregadores_unicode = request.body.decode('utf-8')
    entregadores = json.loads(entregadores_unicode)['body']

    parametros_unicode = request._body.decode('utf-8')
    parametros = json.loads(parametros_unicode)['params']
    
    try:
        cursor.execute("""
            UPDATE ek_entregador
            SET nome_entregador = %s,
                usuario = %s,
                senha = %s,
                email_entregador = %s,
                fone_entregador = %s
            WHERE seq_entregador = %s
            RETURNING seq_entregador
        """,[
            entregadores["dbedNome"],
            entregadores["dbedUsuario"],
            entregadores["dbedSenha"],
            entregadores["dbedEmail"],
            entregadores["dbedContato"],
            parametros["codigo_entregador"]
        ])
        codigo_entregador = cursor.fetchall()

        if codigo_entregador:
            return {
                "Status": 200,
                "Mensagem": f"Entregador { codigo_entregador[0][0] } atualizado com sucesso!"
            }
    except:
        traceback.print_exc()
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao atualizar o entregador!"
            }
        }
    

# ---------------------------------------- VEICULOS ----------------------------------------

@api_entregas.get("veiculos/")
def get_veiculos( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*)
        FROM
            (
            SELECT
                seq_veiculo as sequencial,
                placa_veiculo as placa_veiculo,
                nome_veiculo
            FROM ek_veiculo 
            ORDER BY seq_veiculo
        ) as x
    """)
    objetoVeiculo = cursor.fetchall()

    if objetoVeiculo:
        return objetoVeiculo[0][0]
    else:
        return []

@api_entregas.post("veiculos/")
def post_veiculo( request ):
    cursor = connection.cursor()

    entregadores_unicode = request.body.decode('utf-8')
    entregadores = json.loads(entregadores_unicode)['body']

    try:
        cursor.execute("""
            INSERT INTO ek_veiculo(
                nome_veiculo, placa_veiculo , dt_cadastro
            )VALUES( %s , %s , now() )
            RETURNING seq_veiculo
        """,[
            entregadores["dbedNome"],
            entregadores["dbedPlaca"]
        ])
        codigo_veiculo = cursor.fetchall()

        if codigo_veiculo:
            return {
                "Status": 200,
                "Mensagem": f"Veículo { codigo_veiculo[0][0] } cadastrado com sucesso!"
            }
    except:
        traceback.print_exc()

        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao cadastrar o veículo!"
            }
        }

@api_entregas.get("veiculos/options")
def get_options_veiculos( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*)               
        FROM (
            (SELECT '' as value , 'Selecione...' as label)
            UNION ALL
            (SELECT
                seq_veiculo::varchar as value,
                ( nome_veiculo || ' - ' || placa_veiculo ) as label
            FROM 
                ek_veiculo
            ORDER BY nome_veiculo)
        ) as x
    """)
    objetoVeiculo = cursor.fetchall()

    if objetoVeiculo:
        return {
            "Status": 200,
            "Veiculos": objetoVeiculo[0][0]
        }
    else:
        return []

@api_entregas.get("veiculos/edit")
def get_veiculo( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*)
        FROM (
            SELECT
                nome_veiculo as nome, placa_veiculo as placa
            FROM
                ek_veiculo
            WHERE
                seq_veiculo = %s           
        ) as x
    """,[
        request.GET["codigo_veiculo"]
    ])
    objetoVeiculo = cursor.fetchall()

    if objetoVeiculo:
        return {
            "Status": 200,
            "Veiculo": objetoVeiculo[0][0]
        }
    else:
        return []

@api_entregas.put("veiculos/")
def put_veiculo( request ):
    cursor = connection.cursor()
    
    veiculo_unicode = request.body.decode('utf-8')
    veiculo = json.loads(veiculo_unicode)['body']

    parametros_unicode = request._body.decode('utf-8')
    parametros = json.loads(parametros_unicode)['params']
    
    try:
        cursor.execute("""
            UPDATE ek_veiculo
            SET nome_veiculo = %s,
                placa_veiculo = %s
            WHERE seq_veiculo = %s
            RETURNING seq_veiculo
        """,[
            veiculo["dbedNome"],
            veiculo["dbedPlaca"],
            parametros["codigo_veiculo"]
        ])
        codigo_veiculo = cursor.fetchall()

        if codigo_veiculo:
            return {
                "Status": 200,
                "Mensagem": f"Veículo { codigo_veiculo[0][0] } atualizado com sucesso!"
            }
    except:
        traceback.print_exc()
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao atualizar o veículo!"
            }
        }
    

# ---------------------------------------- Entregas Consulta ----------------------------------------

@api_entregas.get("entregas/")
def get_entregas( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT sum(x.contador)
        FROM (
            SELECT
                count( distinct ek_entrega.seq_entrega) as contador
            FROM
                ek_entrega INNER JOIN ek_item_entrega
                    ON ek_entrega.seq_entrega = ek_item_entrega.seq_entrega
            WHERE
                (
                    (%s = '' OR ek_entrega.seq_pedido_cli::varchar = %s) AND
                    (%s = '' OR ek_entrega.status_entrega::varchar = %s) AND
                    (%s = '' OR ek_entrega.tipo_operacao::varchar = %s) AND
                    (%s = '' OR ek_entrega.nome_pessoa::varchar LIKE %s) 
                )
        ) as x
    """,[
        request.GET["query_pedido"], request.GET["query_pedido"], 
        request.GET["query_status"], request.GET["query_status"], 
        request.GET["query_tipoEntrega"], request.GET["query_tipoEntrega"], 
        '%' + request.GET["query_cliente"] + '%', '%' + request.GET["query_cliente"] + '%', 
    ])
    paginacao = cursor.fetchall()

    cursor.execute("""
        SELECT
            json_agg(x.*)
        FROM (
            SELECT
                ek_entrega.seq_entrega as sequencial,
                ek_entrega.seq_pedido_cli as pedido,
                ek_entrega.nome_pessoa as cliente,
                ( COALESCE(ek_entrega.endereco,'') || ', ' || COALESCE(ek_entrega.numero_endereco,'') || ' | ' || COALESCE(ek_entrega.bairro_endereco,'') || ' | ' || COALESCE(ek_entrega.cidade_endereco,'')  ) as endereco,
                (   CASE WHEN coalesce(ek_entrega.tipo_operacao,'E') = 'E' THEN 'ENTREGA'
                        WHEN coalesce(ek_entrega.tipo_operacao,'E') = 'T' THEN 'TROCA'
                        WHEN coalesce(ek_entrega.tipo_operacao,'E') = 'R' THEN 'RECOLHIMENTO'
                    ELSE '' END) AS tipo_entrega,
                ek_entrega.observacao,
                ek_entrega.status_entrega as status,
                TO_CHAR(ek_entrega.dt_cadastro::date,'DD/MM/YYYY') as data_venda

            FROM
                ek_entrega INNER JOIN ek_item_entrega
                    ON ek_entrega.seq_entrega = ek_item_entrega.seq_entrega
            WHERE
                (
                    (%s = '' OR ek_entrega.seq_pedido_cli::varchar = %s) AND
                    (%s = '' OR ek_entrega.status_entrega::varchar = %s) AND
                    (%s = '' OR ek_entrega.tipo_operacao::varchar = %s) AND
                    (%s = '' OR ek_entrega.nome_pessoa::varchar LIKE %s) 
                )
            GROUP BY 
                ek_entrega.seq_entrega,
                ek_entrega.nome_pessoa,
                ek_entrega.endereco,
                ek_entrega.numero_endereco,
                ek_entrega.bairro_endereco,
                ek_entrega.cidade_endereco,
                ek_entrega.tipo_operacao,
                ek_entrega.observacao,
                status_entrega
            ORDER BY ek_entrega.seq_entrega DESC
            offset %s limit 10
        )as x            
    """,[
        request.GET["query_pedido"], request.GET["query_pedido"], 
        request.GET["query_status"], request.GET["query_status"], 
        request.GET["query_tipoEntrega"], request.GET["query_tipoEntrega"], 
        '%' + request.GET["query_cliente"] + '%', '%' + request.GET["query_cliente"] + '%', 
        request.GET["offset"]
    ])
    objetoVendas = cursor.fetchall()

    if objetoVendas:
        return {
            "Status": 200,
            "Vendas": objetoVendas[0][0],
            "TotalVendas": paginacao[0][0]
        }

@api_entregas.post("entregas/gera-troca")
def gera_troca( request ):
    cursor = connection.cursor()

    entrega_unicode = request.body.decode('utf-8')
    entrega = json.loads(entrega_unicode)['body']


    try:
        cursor.execute("""
            INSERT INTO ek_entrega(
                seq_tenant, seq_pedido_cli, cod_pessoa, nome_pessoa, cnpj_cpf, endereco, numero_endereco, 
                bairro_endereco, cep_endereco, cidade_endereco, contato_pessoa, tipo_operacao, observacao, status_entrega, 
                dt_cadastro
            ) (
                SELECT 
                    seq_tenant, seq_pedido_cli, cod_pessoa, nome_pessoa, cnpj_cpf, endereco, numero_endereco, 
                    bairro_endereco, cep_endereco, cidade_endereco, contato_pessoa, 'T' , observacao, 'EM ABERTO', 
                    now()
                FROM
                    ek_entrega
                WHERE ek_entrega.seq_entrega = %s
            ) RETURNING seq_entrega;
        """,[
            entrega.get("sequencial_entrega")
        ])
        sequencial_entrega = cursor.fetchall()

        if sequencial_entrega:
            
            try:
                cursor.execute("""
                    INSERT INTO ek_item_entrega(
                        seq_tenant , seq_entrega, seq_item_pedido_cli, cod_produto, descricao_produto, 
                        quantidade_produto, seq_veiculo, data_entrega, foto_item, pontos_item, status_entrega_item, observacao_item, 
                        dt_cadastro
                    ) (
                        SELECT
                            seq_tenant, %s ,  seq_item_pedido_cli, cod_produto, descricao_produto, 
                            quantidade_produto, null, null, foto_item, null, 'P', observacao_item, 
                            now()
                        FROM ek_item_entrega
                        WHERE ek_item_entrega.seq_entrega = %s
                    );
                """,[
                    sequencial_entrega[0][0],
                    entrega.get("sequencial_entrega")
                ])
            except:
                traceback.print_exc()
                return {
                    "Status": 400,
                    "Erro": {
                        "causa": "Houve um erro ao inserir o item da troca!"
                    }
                }
            
            return {
                "Status": 200,
                "Mensagem": "Troca gerada com sucesso!",
                "SequencialTroca": sequencial_entrega[0][0]
            }
    except:
        traceback.print_exc();
        return{
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao inserir a troca!"
            }
        }

@api_entregas.post("entregas/gera-recolhimento")
def gera_troca( request ):
    cursor = connection.cursor()

    entrega_unicode = request.body.decode('utf-8')
    entrega = json.loads(entrega_unicode)['body']


    try:
        cursor.execute("""
            INSERT INTO ek_entrega(
                seq_tenant, seq_pedido_cli, cod_pessoa, nome_pessoa, cnpj_cpf, endereco, numero_endereco, 
                bairro_endereco, cep_endereco, cidade_endereco, contato_pessoa, tipo_operacao, observacao, status_entrega, 
                dt_cadastro
            ) (
                SELECT 
                    seq_tenant, seq_pedido_cli, cod_pessoa, nome_pessoa, cnpj_cpf, endereco, numero_endereco, 
                    bairro_endereco, cep_endereco, cidade_endereco, contato_pessoa, 'R' , observacao, 'EM ABERTO', 
                    now()
                FROM
                    ek_entrega
                WHERE ek_entrega.seq_entrega = %s
            ) RETURNING seq_entrega;
        """,[
            entrega.get("sequencial_entrega")
        ])
        sequencial_entrega = cursor.fetchall()

        if sequencial_entrega:
            
            try:
                cursor.execute("""
                    INSERT INTO ek_item_entrega(
                        seq_tenant , seq_entrega, seq_item_pedido_cli, cod_produto, descricao_produto, 
                        quantidade_produto, seq_veiculo, data_entrega, foto_item, pontos_item, status_entrega_item, observacao_item, 
                        dt_cadastro
                    ) (
                        SELECT
                            seq_tenant, %s ,  seq_item_pedido_cli, cod_produto, descricao_produto, 
                            quantidade_produto, null, null, foto_item, null, 'P', observacao_item, 
                            now()
                        FROM ek_item_entrega
                        WHERE ek_item_entrega.seq_entrega = %s
                    );
                """,[
                    sequencial_entrega[0][0],
                    entrega.get("sequencial_entrega")
                ])
            except:
                traceback.print_exc()
                return {
                    "Status": 400,
                    "Erro": {
                        "causa": "Houve um erro ao inserir o item do recolhimento!"
                    }
                }
            
            return {
                "Status": 200,
                "Mensagem": "Recolhimento gerado com sucesso!",
                "SequencialRecolhimento": sequencial_entrega[0][0]
            }
    except:
        traceback.print_exc();
        return{
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao inserir o recolhimento!"
            }
        }

@api_entregas.delete("entregas/")
def delete_entrega( request ):
    cursor = connection.cursor()

    try:
        cursor.execute("""
            DELETE FROM ek_item_entrega WHERE seq_entrega = %s
        """,[
            request.GET["sequencial_entrega"]
        ])
    except:
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao excluir o item da entrega"
            }
        }

    try:
        cursor.execute("""
            DELETE FROM ek_entrega WHERE seq_entrega = %s
        """,[
            request.GET["sequencial_entrega"]
        ])
    except:
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao excluir a entrega"
            }
        }
    
    return {
        "Status": 200,
        "Mensagem": "Registro excluido com sucesso!"
    }


# ----------------------------------- Entregas Modal Pendentes -----------------------------------

@api_entregas.get("entregas/modal")
def get_entregas_modal( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            json_agg(x.*)
        FROM (
            SELECT
                ek_entrega.seq_entrega AS sequencial,
                ek_entrega.seq_pedido_cli AS pedido,
                ek_entrega.nome_pessoa AS cliente,
                (CASE 
                    WHEN COALESCE(ek_entrega.tipo_operacao, 'E') = 'E' THEN 'ENTREGA'
                    WHEN COALESCE(ek_entrega.tipo_operacao, 'E') = 'T' THEN 'TROCA'
                    WHEN COALESCE(ek_entrega.tipo_operacao, 'E') = 'R' THEN 'RECOLHIMENTO'
                    ELSE '' 
                END) AS tipo_entrega,
                TO_CHAR(ek_entrega.dt_cadastro::date, 'DD/MM/YYYY') AS data_venda,
                
                (SELECT json_agg(item_agendar.*)
                    FROM (
                        SELECT 
                            ek_item_entrega.seq_entrega AS sequencial_entrega,
                            ek_item_entrega.seq_item_entrega AS sequencial_item,
                            ek_item_entrega.cod_produto AS codigo,
                            ek_item_entrega.descricao_produto AS produto,
                            ek_item_entrega.quantidade_produto AS quantidade
                        FROM ek_item_entrega 
                        WHERE ek_item_entrega.seq_entrega = ek_entrega.seq_entrega
                        AND ek_item_entrega.status_entrega_item != 'C'
                        AND ek_item_entrega.seq_item_entrega NOT IN
                            (SELECT DISTINCT seq_item_entrega 
                            FROM ek_entregador_item_entrega 
                            WHERE ek_entregador_item_entrega.seq_item_entrega = ek_item_entrega.seq_item_entrega 
                                AND COALESCE(seq_entregador, 0) >= 0)
                        ORDER BY ek_item_entrega.seq_item_entrega
                    ) AS item_agendar 
                ) AS itens_agendar,

                (SELECT json_agg(item_agendado.*)
                FROM (
                        SELECT 
                            ek_entregador_item_entrega.seq_entregador_item_entrega AS sequencial,
                            ek_entregador_item_entrega.seq_item_entrega AS sequencial_item,
                            ek_item_entrega.cod_produto,
                            ek_item_entrega.descricao_produto AS desc_produto,
                            ek_item_entrega.quantidade_produto AS quantidade,
                            ek_entregador_item_entrega.seq_entregador::varchar AS cod_entregador,
                            ek_entregador.nome_entregador AS entregador,
                            ek_veiculo.seq_veiculo::varchar AS cod_veiculo,
                            ek_veiculo.nome_veiculo AS veiculo,
                            COALESCE(ek_entregador_pontos.pontuacao::varchar, '0') AS pontos,
                            ek_entregador_pontos.seq_entregador_pontos AS sequencial_pontos
                        FROM ek_entregador_item_entrega
                        INNER JOIN ek_item_entrega
                            ON ek_entregador_item_entrega.seq_item_entrega = ek_item_entrega.seq_item_entrega
                        LEFT JOIN ek_entregador
                            ON ek_entregador_item_entrega.seq_entregador = ek_entregador.seq_entregador
                        LEFT JOIN ek_veiculo
                            ON ek_item_entrega.seq_veiculo = ek_veiculo.seq_veiculo
                        LEFT JOIN ek_entregador_pontos
                            ON ek_entregador_pontos.seq_entregador_item_entrega = ek_entregador_item_entrega.seq_entregador_item_entrega
                        WHERE 
                            ek_item_entrega.seq_entrega = ek_entrega.seq_entrega
                        GROUP BY
                            ek_entregador_item_entrega.seq_entregador_item_entrega,
                            ek_entregador_item_entrega.seq_item_entrega,
                            ek_item_entrega.cod_produto,
                            ek_item_entrega.descricao_produto,
                            ek_item_entrega.quantidade_produto,
                            ek_entregador_item_entrega.seq_entregador,
                            ek_entregador.nome_entregador,
                            ek_veiculo.seq_veiculo,
                            ek_veiculo.nome_veiculo,
                            ek_entregador_pontos.pontuacao,
                            ek_entregador_pontos.seq_entregador_pontos
                        ORDER BY ek_entregador_item_entrega.seq_item_entrega
                    ) AS item_agendado 
                ) AS itens_agendados

            FROM
                ek_entrega 
            INNER JOIN ek_item_entrega
                ON ek_entrega.seq_entrega = ek_item_entrega.seq_entrega
            WHERE 
                ek_entrega.seq_entrega = %s
            GROUP BY
                ek_entrega.seq_entrega,
                ek_entrega.seq_pedido_cli,
                ek_entrega.nome_pessoa,
                ek_entrega.tipo_operacao,
                ek_entrega.dt_cadastro
        ) AS x
    """,[
        request.GET["sequencial_entrega"]
    ])
    objetoVendaModal = cursor.fetchall()

    if objetoVendaModal:
        return {
            "Status": 200,
            "Venda": objetoVendaModal[0][0]
        }
    else:
        return []

@api_entregas.post("entregas/modal")
def post_entregas_modal( request ):
    cursor = connection.cursor()

    def extraiEntregador( objeto ):
        listaEntregadores = []
        for item in objeto:
            if 'Entregador' in item:
                listaEntregadores.append(item)
        return listaEntregadores

    

    formulario_unicode = request.body.decode('utf-8')
    formulario = json.loads(formulario_unicode)['body']

    parametros_unicode = request._body.decode('utf-8')
    parametros = json.loads(parametros_unicode)['params']

    camposEntregadores = extraiEntregador( formulario )
    

    if camposEntregadores:

        cursor.execute("SELECT %s < now()::date",[ formulario.get("dbedDataEntrega") ])
        dataValida = cursor.fetchall()[0][0]

        if dataValida:
            return {
                "Status": 400,
                "Erro": {
                    "causa": "Data de entrega deve ser maior que a data atual!"
                }
            }


        for campo in camposEntregadores:

            campoCompleto = campo
            numeroRowEntregador = str(campo).split("EntregadorItem")[1]
            
            for entregador in formulario.get(campoCompleto):

                sequencialEntregadorItemEntrega = ''

                try:
                    cursor.execute("""
                        INSERT INTO ek_entregador_item_entrega(
                            seq_tenant,
                            seq_entregador,
                            seq_item_entrega,
                            dt_cadastro
                        ) VALUES (
                            %s, %s, %s , now()
                        ) RETURNING seq_entregador_item_entrega
                    """,[
                        '1',
                        entregador,
                        numeroRowEntregador
                    ])
                    sequencial = cursor.fetchall()[0][0]
                    sequencialEntregadorItemEntrega = sequencial

                except:
                    traceback.print_exc()
                    return {
                        "Status": 400,
                        "Erro": {
                            "causa": "Houve um erro ao vinculador o entregador!"
                        }
                    }

                try:
                    cursor.execute("""
                        UPDATE ek_item_entrega
                        SET 
                            seq_veiculo = %s,
                            status_entrega_item = 'A'
                        WHERE seq_item_entrega = %s
                    """,[
                        formulario.get('dbedVeiculo'),
                        numeroRowEntregador
                    ])
                except:
                    traceback.print_exc()
                    return {
                        "Status": 400,
                        "Erro": {
                            "causa": "Houve um erro ao atualizar o item da entrega!"
                        }
                    }

                try:
                    cursor.execute("""
                        INSERT INTO ek_entregador_pontos(
                            seq_entregador,
                            seq_item_entrega,
                            pontuacao,
                            dt_cadastro,
                            seq_entregador_item_entrega
                        )VALUES(
                            %s , %s , %s , now(), %s
                        )
                    """,[
                        entregador,
                        numeroRowEntregador,
                        None if formulario.get(f"dbedPontosItem{numeroRowEntregador}") == '' else formulario.get(f"dbedPontosItem{numeroRowEntregador}"),
                        sequencialEntregadorItemEntrega
                    ])
                except:
                    traceback.print_exc()
                    return {
                        "Status": 400,
                        "Erro": {
                            "causa": "Houve um erro ao inserir os pontos do entregador!"
                        }
                    }

                # Manipulação do status da entrega -> 

                cursor.execute("""
                    SELECT seq_entrega FROM ek_item_entrega WHERE ek_item_entrega.seq_item_entrega = %s
                """,[
                    numeroRowEntregador
                ])
                sequencial_entrega = cursor.fetchall()

                cursor.execute("""
                    SELECT 
                        (CASE WHEN
                            (SELECT (SELECT count(*) FROM ek_item_entrega WHERE ek_item_entrega.seq_entrega = %s AND ek_item_entrega.status_entrega_item != 'C' )
                                =
                            (SELECT count(*) FROM ek_entregador_item_entrega WHERE seq_item_entrega  IN 
                                ( SELECT seq_item_entrega FROM ek_item_entrega WHERE ek_item_entrega.seq_entrega = %s AND ek_item_entrega.status_entrega_item != 'C' )))
                            THEN 'FINALIZADO' else 'EM ABERTO' 
                        END ) as status
                """,[
                    sequencial_entrega[0][0] , sequencial_entrega[0][0]
                ])
                status_entrega = cursor.fetchall()

                try:
                    cursor.execute("""
                        UPDATE ek_entrega
                        SET status_entrega = %s
                        WHERE ek_entrega.seq_entrega = %s
                    """,[
                        status_entrega[0][0],
                        sequencial_entrega[0][0]
                    ])
                except:
                    traceback.print_exc()
                    return {
                        "Status": 400,
                        "Erro": {
                            "causa": "Houve um erro ao atualizar o status da entrega!"
                        }
                    }

        return {
            "Status": 200,
            "TotalInserido": len(camposEntregadores),
            "Mensagem": "Entrega agendada com sucesso!"
        }
    else:
        return {
            "Status": 400,
            "Erro": {
                "causa": "Insira ao menos um entregador!"
            }
        }

@api_entregas.delete("entregas/modal")
def delete_item_entregas( request ):
    cursor = connection.cursor()

    try:
        cursor.execute("""
            UPDATE ek_item_entrega
            SET status_entrega_item = 'C' 
            WHERE ek_item_entrega.seq_item_entrega = %s
        """,[
            request.GET["sequencial"]
        ])

        cursor.execute("""
            SELECT seq_entrega FROM ek_item_entrega WHERE ek_item_entrega.seq_item_entrega = %s
        """,[
            request.GET["sequencial"]
        ])
        sequencial_entrega = cursor.fetchall()

        cursor.execute("""
            SELECT 
                CASE 
                    WHEN (SELECT COUNT(*) FROM ek_item_entrega WHERE seq_entrega = %s) = 
                        (SELECT COUNT(*) FROM ek_item_entrega WHERE seq_entrega = %s AND status_entrega_item = 'C')
                    THEN 'CANCELADO'
                    WHEN (SELECT COUNT(*) FROM ek_item_entrega WHERE seq_entrega = %s AND status_entrega_item != 'C') = 
                        (SELECT COUNT(*) FROM ek_entregador_item_entrega WHERE seq_item_entrega IN 
                            (SELECT seq_item_entrega FROM ek_item_entrega WHERE seq_entrega = %s AND status_entrega_item != 'C'))
                    THEN 'FINALIZADO'
                    ELSE 'EM ABERTO'
                END AS status
        """,[
            sequencial_entrega[0][0] , sequencial_entrega[0][0],
            sequencial_entrega[0][0] , sequencial_entrega[0][0],
        ])
        status_entrega = cursor.fetchall()
        

        if status_entrega:
            try:
                cursor.execute("""
                    UPDATE ek_entrega
                    SET status_entrega = %s
                    WHERE ek_entrega.seq_entrega = %s
                """,[
                    status_entrega[0][0],
                    sequencial_entrega[0][0]
                ])
            except:
                traceback.print_exc()
                return {
                    "Status": 400,
                    "Erro": {
                        "causa": "Houve um erro ao atualizar o status da entrega!"
                    }
                }

        return {
            "Status": 200,
            "Mensagem": "Item excluído com sucesso!"
        }
    except:
        traceback.print_exc()
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro na exclusão do item!"
            }
        }

# ----------------------------------- Entregas Modal Agendados -----------------------------------


@api_entregas.put("entregas/modal/agendados")
def put_modal_agendados( request ):
    cursor = connection.cursor()

    formulario_unicode = request.body.decode('utf-8')
    formulario = json.loads(formulario_unicode)['body']

    parametros_unicode = request._body.decode('utf-8')
    parametros = json.loads(parametros_unicode)['params']

    try:
        cursor.execute("""
            UPDATE 
                ek_entregador_item_entrega
            SET
                seq_entregador = %s                
            WHERE
                ek_entregador_item_entrega.seq_entregador_item_entrega = %s
        """,[
            formulario.get("dbedEntregador"),
            parametros.get("sequencial_entrega")
        ])

        cursor.execute("""
            UPDATE
                ek_entregador_pontos
            SET
                pontuacao = %s
            WHERE
                ek_entregador_pontos.seq_entregador_item_entrega = %s
        """,[
            formulario.get("dbedPontos"),
            parametros.get("sequencial_entrega")
        ])

        cursor.execute("""
            UPDATE ek_item_entrega
            SET seq_veiculo = %s
            WHERE seq_item_entrega IN (
                SELECT seq_item_entrega FROM ek_entregador_item_entrega WHERE seq_entregador_item_entrega = %s
            )
        """,[
            formulario.get("dbedVeiculo"),
            parametros.get("sequencial_entrega")
        ])

        return {
            "Status": 200,
            "Mensagem": "Item atualizado com sucesso!"
        }

    except:
        traceback.print_exc()
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao atualizar o item da entrega!"
            }
        }

@api_entregas.delete("entregas/modal/agendados")
def delete_modal_agendados( request ):
    cursor = connection.cursor()

    try:
        cursor.execute("""
            DELETE FROM ek_entregador_item_entrega
            WHERE seq_entregador_item_entrega = %s
            RETURNING seq_item_entrega
        """,[
            request.GET["sequencial"]
        ])
        sequencial_item = cursor.fetchall()

        cursor.execute("""
            UPDATE ek_item_entrega SET status_entrega_item = 'P' WHERE ek_item_entrega.seq_item_entrega = %s
        """,[
            sequencial_item[0][0]
        ])

        cursor.execute("""
            SELECT seq_entrega FROM ek_item_entrega WHERE seq_item_entrega = %s
        """,[
            sequencial_item[0][0]
        ])
        sequencial_entrega = cursor.fetchall()

        cursor.execute("""
            SELECT 
                (CASE WHEN
                    (SELECT (SELECT count(*) FROM ek_item_entrega WHERE ek_item_entrega.seq_entrega = %s AND ek_item_entrega.status_entrega_item != 'C' )
                        =
                    (SELECT count(*) FROM ek_entregador_item_entrega WHERE seq_item_entrega  IN 
                        ( SELECT seq_item_entrega FROM ek_item_entrega WHERE ek_item_entrega.seq_entrega = %s AND ek_item_entrega.status_entrega_item != 'C' )))
                    THEN 'FINALIZADO' else 'EM ABERTO' 
                END ) as status
        """,[
            sequencial_entrega[0][0] , sequencial_entrega[0][0]
        ])
        status_entrega = cursor.fetchall()

        try:
            cursor.execute("""
                UPDATE ek_entrega
                SET status_entrega = %s
                WHERE ek_entrega.seq_entrega = %s
            """,[
                status_entrega[0][0],
                sequencial_entrega[0][0]
            ])
        except:
            traceback.print_exc()
            return {
                "Status": 400,
                "Erro": {
                    "causa": "Houve um erro ao atualizar o status da entrega!"
                }
            }

        if sequencial_item:
            cursor.execute("""
                DELETE FROM ek_entregador_pontos
                WHERE seq_item_entrega = %s
            """,[
                request.GET["sequencial"]
            ])

        return {
            "Status": 200,
            "Mensagem": "Item desvinculado com sucesso!"
        }
    except:
        traceback.print_exc()
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao desvincular o item!"
            }
        }


# ----------------------------------- Page Consulta -----------------------------------
@api_entregas.get("entregas/consultar-agendamentos")
def getConsultaAgendamentos( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT sum(x.contador)
        FROM (
            SELECT
                count( distinct ek_item_entrega.seq_item_entrega) as contador
            FROM
                ek_entrega INNER JOIN ek_item_entrega
                    ON ek_entrega.seq_entrega = ek_item_entrega.seq_entrega
                INNER JOIN ek_entregador_item_entrega 
                    ON ek_entregador_item_entrega.seq_item_entrega = ek_item_entrega.seq_item_entrega
                INNER JOIN ek_entregador
                    ON ek_entregador.seq_entregador = ek_entregador_item_entrega.seq_entregador
                INNER JOIN ek_veiculo
                    ON ek_veiculo.seq_veiculo = ek_item_entrega.seq_veiculo
            WHERE (
                    (%s = '' OR ek_item_entrega.seq_veiculo::varchar = %s) AND
                    (%s = '' OR ek_entregador_item_entrega.seq_entregador::varchar = %s) AND
                    (%s = '' OR ek_item_entrega.data_entrega::date::varchar = %s) AND
                    (%s = '' OR ek_entrega.seq_pedido_cli::varchar = %s) AND
                    (%s = '' OR ek_item_entrega.status_entrega_item::varchar = %s)
                )
        ) as x
    """,[
        request.GET["query_veiculo"], request.GET["query_veiculo"], 
        request.GET["query_entregador"], request.GET["query_entregador"], 
        request.GET["query_data_entrega"], request.GET["query_data_entrega"], 
        request.GET["query_venda"], request.GET["query_venda"], 
        request.GET["query_status"], request.GET["query_status"]
    ])
    paginacao = cursor.fetchall()

    cursor.execute("""
        SELECT json_agg(x.*)
        FROM (
            SELECT
                ek_item_entrega.seq_item_entrega as sequencial_item,
                ek_entrega.seq_entrega as sequencial_entrega,
                ek_entrega.seq_pedido_cli as pedido,
                ek_entrega.nome_pessoa as cliente,
                ( COALESCE(ek_entrega.endereco,'') || ', ' || COALESCE(ek_entrega.numero_endereco,'') || ' | ' || COALESCE(ek_entrega.bairro_endereco,'') || ' | ' || COALESCE(ek_entrega.cidade_endereco,'')) as endereco,
                ( ek_item_entrega.cod_produto::varchar || ' - ' ||  ek_item_entrega.descricao_produto ) as produto,
                TO_CHAR( ek_item_entrega.data_entrega , 'DD/MM/YYYY' )  as data_entrega,
                COALESCE(ek_entregador.nome_entregador,'N/A') as nome_entregador,
                COALESCE(ek_veiculo.nome_veiculo,'N/A') as nome_veiculo,
                ek_entrega.observacao,
                (
                    CASE    WHEN ek_item_entrega.status_entrega_item = 'A' THEN 'AGENDADO'
                            WHEN ek_item_entrega.status_entrega_item = 'F' THEN 'FINALIZADO'
                            WHEN ek_item_entrega.status_entrega_item = 'R' THEN 'EM ROTA'
                            WHEN ek_item_entrega.status_entrega_item = 'C' THEN 'CANCELADO'
                            WHEN ek_item_entrega.status_entrega_item = 'AD' THEN 'ADIADO'
                            WHEN ek_item_entrega.status_entrega_item = 'P' THEN 'PENDENTE'
                            ELSE 'OUTROS' 
                    END 
                ) as status,
                (   CASE WHEN coalesce(ek_entrega.tipo_operacao,'E') = 'E' THEN 'ENTREGA'
                        WHEN coalesce(ek_entrega.tipo_operacao,'E') = 'T' THEN 'TROCA'
                        WHEN coalesce(ek_entrega.tipo_operacao,'E') = 'R' THEN 'RECOLHIMENTO'
                    ELSE '' END) AS tipo_entrega
            FROM 
                ek_entrega INNER JOIN ek_item_entrega
                    ON ek_entrega.seq_entrega = ek_item_entrega.seq_entrega
                INNER JOIN ek_entregador_item_entrega 
                    ON ek_entregador_item_entrega.seq_item_entrega = ek_item_entrega.seq_item_entrega
                INNER JOIN ek_entregador
                    ON ek_entregador.seq_entregador = ek_entregador_item_entrega.seq_entregador
                INNER JOIN ek_veiculo
                    ON ek_veiculo.seq_veiculo = ek_item_entrega.seq_veiculo
            WHERE (
                    (%s = '' OR ek_item_entrega.seq_veiculo::varchar = %s) AND
                    (%s = '' OR ek_entregador_item_entrega.seq_entregador::varchar = %s) AND
                    (%s = '' OR ek_item_entrega.data_entrega::date::varchar = %s) AND
                    (%s = '' OR ek_entrega.seq_pedido_cli::varchar = %s) AND
                    (%s = '' OR ek_item_entrega.status_entrega_item::varchar = %s)
                )
            ORDER BY ek_entrega.seq_pedido_cli DESC
            offset %s limit 10
        ) as x
    """,
        [
            request.GET["query_veiculo"], request.GET["query_veiculo"], 
            request.GET["query_entregador"], request.GET["query_entregador"], 
            request.GET["query_data_entrega"], request.GET["query_data_entrega"], 
            request.GET["query_venda"], request.GET["query_venda"], 
            request.GET["query_status"], request.GET["query_status"],
            request.GET["offset"]
        ])
    objetoAgendamentos = cursor.fetchall()


    if objetoAgendamentos:
        return {
            "Status": 200,
            "Vendas": objetoAgendamentos[0][0],
            "TotalVendas": paginacao[0][0]
        }
    else:
        return []

@api_entregas.get("entregador/consultar-agendamentos-modal")
def getEntregaModal( request ):
    cursor = connection.cursor()

    cursor.execute("""
        SELECT json_agg(x.*) FROM (
            SELECT 
                ek_entrega.seq_pedido_cli AS pedido,
                (ek_entrega.cod_pessoa::varchar || ' - ' || ek_entrega.nome_pessoa) AS cliente,
                (COALESCE(ek_entrega.endereco, '') || ', ' || COALESCE(ek_entrega.numero_endereco, '') || ' | ' || COALESCE(ek_entrega.bairro_endereco, '') || ' | ' || COALESCE(ek_entrega.cidade_endereco, '')) AS endereco,
                
                CASE 
                    WHEN COALESCE(ek_entrega.tipo_operacao, 'E') = 'E' THEN 'ENTREGA'
                    WHEN COALESCE(ek_entrega.tipo_operacao, 'E') = 'T' THEN 'TROCA'
                    WHEN COALESCE(ek_entrega.tipo_operacao, 'E') = 'R' THEN 'RECOLHIMENTO'
                    ELSE '' 
                END AS tipo_entrega,
                
                CASE 
                    WHEN ek_item_entrega.status_entrega_item = 'A' THEN 'AGENDADO'
                    WHEN ek_item_entrega.status_entrega_item = 'F' THEN 'FINALIZADO'
                    WHEN ek_item_entrega.status_entrega_item = 'R' THEN 'EM ROTA'
                    WHEN ek_item_entrega.status_entrega_item = 'C' THEN 'CANCELADO'
                    WHEN ek_item_entrega.status_entrega_item = 'AD' THEN 'ADIADO'
                    WHEN ek_item_entrega.status_entrega_item = 'P' THEN 'PENDENTE'
                    ELSE 'OUTROS'
                END AS status,
                
                (
                    SELECT json_agg(
                        json_build_object(
                            'nome_entregador', i.nome_entregador,
                            'nome_veiculo', i.nome_veiculo,
                            'produto', i.produto,
                            'pontos', i.pontuacao
                        )
                    )
                    FROM (
                        SELECT
                            ek_entregador.nome_entregador,
                            ek_veiculo.nome_veiculo,
                            (ek_item_entrega.cod_produto::varchar || ' - ' || ek_item_entrega.descricao_produto) AS produto,
                            ek_entregador_pontos.pontuacao
                        FROM ek_entregador
                        INNER JOIN ek_entregador_item_entrega
                            ON ek_entregador.seq_entregador = ek_entregador_item_entrega.seq_entregador
                        INNER JOIN ek_item_entrega
                            ON ek_item_entrega.seq_item_entrega = ek_entregador_item_entrega.seq_item_entrega
                        INNER JOIN ek_veiculo
                            ON ek_veiculo.seq_veiculo = ek_item_entrega.seq_veiculo
                        INNER JOIN ek_entregador_pontos
                            ON ek_entregador_pontos.seq_entregador_item_entrega = ek_entregador_item_entrega.seq_entregador_item_entrega
                        WHERE ek_item_entrega.seq_entrega = ek_entrega.seq_entrega
                        ORDER BY ek_item_entrega.seq_item_entrega
                    ) AS i
                ) AS itens

            FROM ek_entrega
            INNER JOIN ek_item_entrega
                ON ek_item_entrega.seq_entrega = ek_entrega.seq_entrega
            WHERE ek_entrega.seq_entrega = %s
            GROUP BY
                ek_entrega.seq_entrega,
                ek_entrega.seq_pedido_cli,
                ek_entrega.cod_pessoa,
                ek_entrega.nome_pessoa,
                ek_entrega.endereco,
                ek_entrega.numero_endereco,
                ek_entrega.bairro_endereco,
                ek_entrega.cidade_endereco,
                ek_entrega.tipo_operacao,
                ek_item_entrega.status_entrega_item
        ) as x
    """,[
        request.GET["sequencial_entrega"]
    ])
    objetoEntrega = cursor.fetchall()

    if objetoEntrega:
        return {
            "Status": 200,
            "Entrega": objetoEntrega[0][0]
        }
    else:
        return {
            "Status": 400,
            "Erro": {
                "causa": "Houve um erro ao buscar os dados!"
            }
        }

